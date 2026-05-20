package queue

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"
)

// PostgresStorage implements Storage interface using PostgreSQL.
type PostgresStorage struct {
	db *sql.DB
}

// NewPostgresStorage instantiates a new PostgreSQL storage engine.
// Usage example:
//	storage, err := queue.NewPostgresStorage(db)
func NewPostgresStorage(db *sql.DB) (*PostgresStorage, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := createJobsTable(ctx, db); err != nil {
		return nil, err
	}
	return &PostgresStorage{db: db}, nil
}

func (p *PostgresStorage) Enqueue(ctx context.Context, env *JobEnvelope) error {
	runAt := time.Now()
	if env.RunAt != nil {
		runAt = *env.RunAt
	}
	maxAttempts := env.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 3
	}

	if env.UniqueKey != "" {
		lockKey := env.Queue + ":" + env.UniqueKey
		ttl := env.UniqueTTL
		if ttl <= 0 {
			ttl = 24 * time.Hour
		}
		expiresAt := time.Now().Add(ttl)

		_, err := p.db.ExecContext(ctx, `
			INSERT INTO runiq_unique_locks (lock_key, job_id, expires_at)
			VALUES ($1, $2, $3)
			ON CONFLICT (lock_key) DO NOTHING`,
			lockKey, env.JobID, expiresAt,
		)
		if err != nil {
			return err
		}

		var existingJobID string
		var existingExpiresAt time.Time
		err = p.db.QueryRowContext(ctx, `
			SELECT job_id, expires_at FROM runiq_unique_locks WHERE lock_key = $1`,
			lockKey,
		).Scan(&existingJobID, &existingExpiresAt)
		if err != nil {
			return err
		}

		if existingJobID != env.JobID {
			var exists bool
			_ = p.db.QueryRowContext(ctx, `
				SELECT EXISTS(SELECT 1 FROM runiq_jobs WHERE job_id = $1)`,
				existingJobID,
			).Scan(&exists)

			if time.Now().Before(existingExpiresAt) && exists {
				return ErrDuplicateJob
			}

			_, err = p.db.ExecContext(ctx, `
				INSERT INTO runiq_unique_locks (lock_key, job_id, expires_at)
				VALUES ($1, $2, $3)
				ON CONFLICT (lock_key) DO UPDATE SET job_id = $2, expires_at = $3`,
				lockKey, env.JobID, expiresAt,
			)
			if err != nil {
				return err
			}
		}
	}

	query := `
		INSERT INTO runiq_jobs (job_id, queue, name, args, trace_id, span_id, status, attempts, max_attempts, run_at, unique_key)
		VALUES ($1, $2, $3, $4, $5, $6, 'pending', 0, $7, $8, $9)
		ON CONFLICT (job_id) DO UPDATE SET
			status = 'pending', attempts = 0, max_attempts = $7, run_at = $8, unique_key = $9, updated_at = CURRENT_TIMESTAMP`
	_, err := p.db.ExecContext(ctx, query,
		env.JobID, env.Queue, env.Name, env.Args,
		env.TraceContext.TraceID, env.TraceContext.SpanID,
		maxAttempts, runAt, env.UniqueKey,
	)
	return err
}

// Dequeue fetches the next pending job from PostgreSQL using FOR UPDATE SKIP LOCKED.
func (p *PostgresStorage) Dequeue(ctx context.Context, queueName string) (*JobEnvelope, error) {
	query := `
		UPDATE runiq_jobs
		SET status = 'running', locked_at = CURRENT_TIMESTAMP
		WHERE job_id = (
			SELECT job_id FROM runiq_jobs
			WHERE queue = $1 AND status = 'pending' AND (run_at IS NULL OR run_at <= CURRENT_TIMESTAMP)
			ORDER BY created_at ASC
			FOR UPDATE SKIP LOCKED LIMIT 1
		)
		RETURNING job_id, queue, name, args, trace_id, span_id, attempts, max_attempts, unique_key, batch_id`
	row := p.db.QueryRowContext(ctx, query, queueName)
	var env JobEnvelope
	err := row.Scan(&env.JobID, &env.Queue, &env.Name, &env.Args, &env.TraceContext.TraceID, &env.TraceContext.SpanID, &env.Attempts, &env.MaxAttempts, &env.UniqueKey, &env.BatchID)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &env, err
}

// Ack marks the job as processed on success.
func (p *PostgresStorage) Ack(ctx context.Context, jobID string) error {
	var uniqueKey, queueName, batchID string
	err := p.db.QueryRowContext(ctx, "SELECT unique_key, queue, batch_id FROM runiq_jobs WHERE job_id = $1", jobID).Scan(&uniqueKey, &queueName, &batchID)
	if err != nil && err != sql.ErrNoRows {
		return err
	}

	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(ctx, "UPDATE runiq_jobs SET status = 'processed', locked_at = NULL, updated_at = CURRENT_TIMESTAMP WHERE job_id = $1", jobID)
	if err != nil {
		return err
	}

	if uniqueKey != "" {
		lockKey := queueName + ":" + uniqueKey
		_, err = tx.ExecContext(ctx, "DELETE FROM runiq_unique_locks WHERE lock_key = $1", lockKey)
		if err != nil {
			return err
		}
	}

	if batchID != "" {
		var pendingJobs int
		var status, callbackQueue, callbackName string
		var callbackArgs []byte
		err = tx.QueryRowContext(ctx, `
			UPDATE runiq_batches
			SET pending_jobs = pending_jobs - 1
			WHERE batch_id = $1
			RETURNING pending_jobs, status, callback_queue, callback_name, callback_args`,
			batchID,
		).Scan(&pendingJobs, &status, &callbackQueue, &callbackName, &callbackArgs)
		if err == nil {
			if status == "sealed" && pendingJobs == 0 {
				_, err = tx.ExecContext(ctx, `
					UPDATE runiq_batches
					SET status = 'completed'
					WHERE batch_id = $1`, batchID)
				if err != nil {
					return err
				}

				callbackJobID := generateJobID()
				query := `
					INSERT INTO runiq_jobs (job_id, queue, name, args, status, attempts, max_attempts, run_at)
					VALUES ($1, $2, $3, $4, 'pending', 0, 3, CURRENT_TIMESTAMP)`
				_, err = tx.ExecContext(ctx, query, callbackJobID, callbackQueue, callbackName, callbackArgs)
				if err != nil {
					return err
				}
			}
		} else if err != sql.ErrNoRows {
			return err
		}
	}

	return tx.Commit()
}

// Fail updates the status to failed and sets error details. If attempts < max_attempts, schedules a retry.
func (p *PostgresStorage) Fail(ctx context.Context, jobID string, runErr error) error {
	var attempts, maxAttempts int
	var uniqueKey, queueName, batchID string
	err := p.db.QueryRowContext(ctx, "SELECT attempts, max_attempts, unique_key, queue, batch_id FROM runiq_jobs WHERE job_id = $1", jobID).Scan(&attempts, &maxAttempts, &uniqueKey, &queueName, &batchID)
	if err != nil {
		return err
	}

	if maxAttempts <= 0 {
		maxAttempts = 3
	}

	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if attempts+1 < maxAttempts {
		delaySec := (1 << uint(attempts)) * 10
		if delaySec > 3600 {
			delaySec = 3600
		}
		// Deterministic jitter using execution nanoseconds to keep imports minimal
		jitterSec := time.Now().Nanosecond() % 3
		nextRun := time.Now().Add(time.Duration(delaySec+jitterSec) * time.Second)

		query := `
			UPDATE runiq_jobs
			SET status = 'pending', attempts = attempts + 1, run_at = $2, error_message = $3, locked_at = NULL, updated_at = CURRENT_TIMESTAMP
			WHERE job_id = $1`
		_, err = tx.ExecContext(ctx, query, jobID, nextRun, runErr.Error())
		if err != nil {
			return err
		}
	} else {
		query := `
			UPDATE runiq_jobs
			SET status = 'dead', error_message = $2, attempts = attempts + 1, locked_at = NULL, updated_at = CURRENT_TIMESTAMP
			WHERE job_id = $1`
		_, err = tx.ExecContext(ctx, query, jobID, runErr.Error())
		if err != nil {
			return err
		}

		if uniqueKey != "" {
			lockKey := queueName + ":" + uniqueKey
			_, err = tx.ExecContext(ctx, "DELETE FROM runiq_unique_locks WHERE lock_key = $1", lockKey)
			if err != nil {
				return err
			}
		}

		if batchID != "" {
			_, err = tx.ExecContext(ctx, "UPDATE runiq_batches SET status = 'failed' WHERE batch_id = $1", batchID)
			if err != nil {
				return err
			}
		}
	}
	return tx.Commit()
}

// PollScheduled is a no-op for PostgreSQL since Dequeue filters by run_at natively.
func (p *PostgresStorage) PollScheduled(ctx context.Context, queue string) error {
	return nil
}

func createJobsTable(ctx context.Context, db *sql.DB) error {
	schema := `
	CREATE TABLE IF NOT EXISTS runiq_jobs (
		job_id VARCHAR(255) PRIMARY KEY,
		queue VARCHAR(255) NOT NULL,
		name VARCHAR(255) NOT NULL,
		args BYTEA NOT NULL,
		trace_id VARCHAR(255) DEFAULT '',
		span_id VARCHAR(255) DEFAULT '',
		status VARCHAR(50) NOT NULL DEFAULT 'pending',
		attempts INT DEFAULT 0,
		max_attempts INT DEFAULT 3,
		error_message TEXT DEFAULT '',
		created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
		locked_at TIMESTAMP WITH TIME ZONE,
		run_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
		unique_key VARCHAR(255) DEFAULT ''
	);
	ALTER TABLE runiq_jobs ADD COLUMN IF NOT EXISTS max_attempts INT DEFAULT 3;
	ALTER TABLE runiq_jobs ADD COLUMN IF NOT EXISTS run_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP;
	ALTER TABLE runiq_jobs ADD COLUMN IF NOT EXISTS unique_key VARCHAR(255) DEFAULT '';
	ALTER TABLE runiq_jobs ADD COLUMN IF NOT EXISTS batch_id VARCHAR(255) DEFAULT '';

	CREATE TABLE IF NOT EXISTS runiq_unique_locks (
		lock_key VARCHAR(255) PRIMARY KEY,
		job_id VARCHAR(255) NOT NULL,
		expires_at TIMESTAMP WITH TIME ZONE NOT NULL
	);

	CREATE TABLE IF NOT EXISTS runiq_processes (
		process_id VARCHAR(255) PRIMARY KEY,
		concurrency INT NOT NULL,
		queues TEXT NOT NULL,
		heartbeat_at TIMESTAMP WITH TIME ZONE NOT NULL
	);

	CREATE TABLE IF NOT EXISTS runiq_cron_locks (
		cron_name VARCHAR(255) NOT NULL,
		execution_minute TIMESTAMP WITH TIME ZONE NOT NULL,
		acquired_at TIMESTAMP WITH TIME ZONE NOT NULL,
		PRIMARY KEY (cron_name, execution_minute)
	);

	CREATE TABLE IF NOT EXISTS runiq_rate_limits (
		job_name VARCHAR(255) NOT NULL,
		request_timestamp BIGINT NOT NULL
	);
	CREATE INDEX IF NOT EXISTS idx_runiq_rate_limits_ts ON runiq_rate_limits (job_name, request_timestamp);

	CREATE TABLE IF NOT EXISTS runiq_batches (
		batch_id VARCHAR(255) PRIMARY KEY,
		callback_queue VARCHAR(255) NOT NULL,
		callback_name VARCHAR(255) NOT NULL,
		callback_args BYTEA NOT NULL,
		total_jobs INT NOT NULL DEFAULT 0,
		pending_jobs INT NOT NULL DEFAULT 0,
		status VARCHAR(50) NOT NULL DEFAULT 'open',
		created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
	);
	`
	_, err := db.ExecContext(ctx, schema)
	return err
}

// GetStats queries the PostgreSQL database for job status counts and recent job details.
func (p *PostgresStorage) GetStats(ctx context.Context) (*Stats, error) {
	query := `
		SELECT queue, status, COUNT(*)
		FROM runiq_jobs
		GROUP BY queue, status`
	rows, err := p.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var stats Stats
	queueMap := make(map[string]*QueueStats)
	for rows.Next() {
		var qName, status string
		var count int64
		if err := rows.Scan(&qName, &status, &count); err != nil {
			return nil, err
		}
		p.accumulateStats(&stats, queueMap, qName, status, count)
	}
	for _, qs := range queueMap {
		stats.Queues = append(stats.Queues, *qs)
	}

	// Fetch recent job details
	jobsQuery := `
		SELECT job_id, queue, name, status, trace_id, error_message, created_at
		FROM runiq_jobs
		ORDER BY created_at DESC
		LIMIT 100`
	jRows, err := p.db.QueryContext(ctx, jobsQuery)
	if err != nil {
		return nil, err
	}
	defer jRows.Close()

	for jRows.Next() {
		var jd JobDetail
		var createdAt time.Time
		var errMsg sql.NullString
		if err := jRows.Scan(&jd.JobID, &jd.Queue, &jd.Name, &jd.Status, &jd.TraceID, &errMsg, &createdAt); err != nil {
			return nil, err
		}
		jd.ErrorMessage = errMsg.String
		jd.CreatedAt = createdAt.Format(time.RFC3339)
		stats.Jobs = append(stats.Jobs, jd)
	}

	activeProcesses, err := p.GetActiveProcesses(ctx)
	if err == nil {
		stats.Processes = activeProcesses
	}

	return &stats, nil
}

func (p *PostgresStorage) accumulateStats(stats *Stats, queueMap map[string]*QueueStats, qName, status string, count int64) {
	qs, ok := queueMap[qName]
	if !ok {
		qs = &QueueStats{Name: qName}
		queueMap[qName] = qs
	}
	switch status {
	case "pending":
		stats.Pending += count
		qs.Pending += count
	case "running":
		stats.Running += count
		qs.Running += count
	case "failed", "dead":
		stats.Failed += count
		qs.Failed += count
	case "processed":
		stats.Processed += count
		qs.Processed += count
	}
}

// Retry resets a failed job back to pending state for re-execution.
func (p *PostgresStorage) Retry(ctx context.Context, jobID string) error {
	query := `
		UPDATE runiq_jobs
		SET status = 'pending', attempts = 0, run_at = CURRENT_TIMESTAMP, error_message = ''
		WHERE job_id = $1`
	_, err := p.db.ExecContext(ctx, query, jobID)
	return err
}

// Cancel deletes a pending, scheduled, or failed job from storage.
func (p *PostgresStorage) Cancel(ctx context.Context, jobID string) error {
	var uniqueKey, queueName string
	err := p.db.QueryRowContext(ctx, "SELECT unique_key, queue FROM runiq_jobs WHERE job_id = $1", jobID).Scan(&uniqueKey, &queueName)
	if err != nil && err != sql.ErrNoRows {
		return err
	}

	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(ctx, "DELETE FROM runiq_jobs WHERE job_id = $1", jobID)
	if err != nil {
		return err
	}

	if uniqueKey != "" {
		lockKey := queueName + ":" + uniqueKey
		_, err = tx.ExecContext(ctx, "DELETE FROM runiq_unique_locks WHERE lock_key = $1", lockKey)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

// ClearQueue removes all jobs belonging to the specified queue.
func (p *PostgresStorage) ClearQueue(ctx context.Context, queue string) error {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(ctx, "DELETE FROM runiq_jobs WHERE queue = $1", queue)
	if err != nil {
		return err
	}

	_, err = tx.ExecContext(ctx, "DELETE FROM runiq_unique_locks WHERE lock_key LIKE $1", queue+":%")
	if err != nil {
		return err
	}

	return tx.Commit()
}

// RegisterProcess stores a worker process info in PostgreSQL.
func (p *PostgresStorage) RegisterProcess(ctx context.Context, info *ProcessInfo) error {
	queuesJSON, err := json.Marshal(info.Queues)
	if err != nil {
		return err
	}
	query := `
		INSERT INTO runiq_processes (process_id, concurrency, queues, heartbeat_at)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (process_id) DO UPDATE SET
			concurrency = $2, queues = $3, heartbeat_at = $4`
	_, err = p.db.ExecContext(ctx, query, info.ProcessID, info.Concurrency, string(queuesJSON), info.HeartbeatAt)
	return err
}

// HeartbeatProcess updates the process heartbeat timestamp in PostgreSQL.
func (p *PostgresStorage) HeartbeatProcess(ctx context.Context, processID string) error {
	query := `UPDATE runiq_processes SET heartbeat_at = $2 WHERE process_id = $1`
	_, err := p.db.ExecContext(ctx, query, processID, time.Now())
	return err
}

// GetActiveProcesses prunes dead processes and returns active ones from PostgreSQL.
func (p *PostgresStorage) GetActiveProcesses(ctx context.Context) ([]ProcessInfo, error) {
	deadTimeLimit := time.Now().Add(-15 * time.Second)
	_, _ = p.db.ExecContext(ctx, "DELETE FROM runiq_processes WHERE heartbeat_at < $1", deadTimeLimit)

	rows, err := p.db.QueryContext(ctx, "SELECT process_id, concurrency, queues, heartbeat_at FROM runiq_processes ORDER BY heartbeat_at DESC")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var processes []ProcessInfo
	for rows.Next() {
		var info ProcessInfo
		var queuesJSON string
		if err := rows.Scan(&info.ProcessID, &info.Concurrency, &queuesJSON, &info.HeartbeatAt); err != nil {
			return nil, err
		}
		_ = json.Unmarshal([]byte(queuesJSON), &info.Queues)
		processes = append(processes, info)
	}
	return processes, nil
}

// LockCronExecution attempts to acquire a unique execution lock for a cron job at a specific minute.
func (p *PostgresStorage) LockCronExecution(ctx context.Context, cronName string, executionMinute time.Time) (bool, error) {
	_, _ = p.db.ExecContext(ctx, "DELETE FROM runiq_cron_locks WHERE execution_minute < $1", time.Now().Add(-1*time.Hour))
	res, err := p.db.ExecContext(ctx, `
		INSERT INTO runiq_cron_locks (cron_name, execution_minute, acquired_at)
		VALUES ($1, $2, $3)
		ON CONFLICT (cron_name, execution_minute) DO NOTHING`,
		cronName, executionMinute.Truncate(time.Minute), time.Now(),
	)
	if err != nil {
		return false, err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return rows > 0, nil
}

// GetRunningJobsCount returns the number of currently running jobs with the specified name.
func (p *PostgresStorage) GetRunningJobsCount(ctx context.Context, jobName string) (int, error) {
	var count int
	err := p.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM runiq_jobs WHERE name = $1 AND status = 'running'", jobName).Scan(&count)
	return count, err
}

// CheckRateLimit checks and increments/updates the rate limit window for a job name.
func (p *PostgresStorage) CheckRateLimit(ctx context.Context, jobName string, limit int, period time.Duration) (bool, error) {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()

	now := time.Now().UnixNano()
	clearBefore := now - period.Nanoseconds()

	_, err = tx.ExecContext(ctx, "DELETE FROM runiq_rate_limits WHERE job_name = $1 AND request_timestamp < $2", jobName, clearBefore)
	if err != nil {
		return false, err
	}

	var count int
	err = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM runiq_rate_limits WHERE job_name = $1", jobName).Scan(&count)
	if err != nil {
		return false, err
	}

	if count >= limit {
		return false, nil
	}

	_, err = tx.ExecContext(ctx, "INSERT INTO runiq_rate_limits (job_name, request_timestamp) VALUES ($1, $2)", jobName, now)
	if err != nil {
		return false, err
	}

	err = tx.Commit()
	return err == nil, err
}

// PostponeJob postpones a job to be executed in the future without failing it.
func (p *PostgresStorage) PostponeJob(ctx context.Context, jobID string, queueName string, delay time.Duration) error {
	runAt := time.Now().Add(delay)
	_, err := p.db.ExecContext(ctx, "UPDATE runiq_jobs SET status = 'pending', run_at = $2, locked_at = NULL WHERE job_id = $1", jobID, runAt)
	return err
}

// CreateBatch registers a new batch record with open status and callback details.
func (p *PostgresStorage) CreateBatch(ctx context.Context, batchID string, callback *JobEnvelope) error {
	query := `
		INSERT INTO runiq_batches (batch_id, callback_queue, callback_name, callback_args, total_jobs, pending_jobs, status)
		VALUES ($1, $2, $3, $4, 0, 0, 'open')`
	_, err := p.db.ExecContext(ctx, query, batchID, callback.Queue, callback.Name, callback.Args)
	return err
}

// EnqueueInBatch associates a job envelope with a batch and enqueues it, incrementing batch job counts.
func (p *PostgresStorage) EnqueueInBatch(ctx context.Context, batchID string, env *JobEnvelope) error {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Increment total and pending counts
	_, err = tx.ExecContext(ctx, `
		UPDATE runiq_batches
		SET total_jobs = total_jobs + 1, pending_jobs = pending_jobs + 1
		WHERE batch_id = $1`, batchID)
	if err != nil {
		return err
	}

	// Handle Unique Lock check
	if env.UniqueKey != "" {
		lockKey := env.Queue + ":" + env.UniqueKey
		ttl := env.UniqueTTL
		if ttl <= 0 {
			ttl = 24 * time.Hour
		}
		expiresAt := time.Now().Add(ttl)

		_, err = tx.ExecContext(ctx, `
			INSERT INTO runiq_unique_locks (lock_key, job_id, expires_at)
			VALUES ($1, $2, $3)
			ON CONFLICT (lock_key) DO NOTHING`,
			lockKey, env.JobID, expiresAt,
		)
		if err != nil {
			return err
		}

		var existingJobID string
		var existingExpiresAt time.Time
		err = tx.QueryRowContext(ctx, `
			SELECT job_id, expires_at FROM runiq_unique_locks WHERE lock_key = $1`,
			lockKey,
		).Scan(&existingJobID, &existingExpiresAt)
		if err != nil {
			return err
		}

		if existingJobID != env.JobID {
			var exists bool
			_ = tx.QueryRowContext(ctx, `
				SELECT EXISTS(SELECT 1 FROM runiq_jobs WHERE job_id = $1)`,
				existingJobID,
			).Scan(&exists)

			if time.Now().Before(existingExpiresAt) && exists {
				return ErrDuplicateJob
			}

			_, err = tx.ExecContext(ctx, `
				INSERT INTO runiq_unique_locks (lock_key, job_id, expires_at)
				VALUES ($1, $2, $3)
				ON CONFLICT (lock_key) DO UPDATE SET job_id = $2, expires_at = $3`,
				lockKey, env.JobID, expiresAt,
			)
			if err != nil {
				return err
			}
		}
	}

	// Insert the job into runiq_jobs
	runAt := time.Now()
	if env.RunAt != nil {
		runAt = *env.RunAt
	}
	maxAttempts := env.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 3
	}

	query := `
		INSERT INTO runiq_jobs (job_id, queue, name, args, trace_id, span_id, status, attempts, max_attempts, run_at, unique_key, batch_id)
		VALUES ($1, $2, $3, $4, $5, $6, 'pending', 0, $7, $8, $9, $10)`
	_, err = tx.ExecContext(ctx, query,
		env.JobID, env.Queue, env.Name, env.Args,
		env.TraceContext.TraceID, env.TraceContext.SpanID,
		maxAttempts, runAt, env.UniqueKey, batchID,
	)
	if err != nil {
		return err
	}

	return tx.Commit()
}

// SubmitBatch seals the batch enqueuing phase and triggers callback if all jobs have already completed.
func (p *PostgresStorage) SubmitBatch(ctx context.Context, batchID string) error {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Update status to sealed
	var pendingJobs int
	var callbackQueue, callbackName string
	var callbackArgs []byte
	err = tx.QueryRowContext(ctx, `
		UPDATE runiq_batches
		SET status = 'sealed'
		WHERE batch_id = $1
		RETURNING pending_jobs, callback_queue, callback_name, callback_args`,
		batchID,
	).Scan(&pendingJobs, &callbackQueue, &callbackName, &callbackArgs)
	if err != nil {
		return err
	}

	if pendingJobs == 0 {
		// All jobs completed! Enqueue callback
		_, err = tx.ExecContext(ctx, `
			UPDATE runiq_batches
			SET status = 'completed'
			WHERE batch_id = $1`, batchID)
		if err != nil {
			return err
		}

		callbackJobID := generateJobID()
		query := `
			INSERT INTO runiq_jobs (job_id, queue, name, args, status, attempts, max_attempts, run_at)
			VALUES ($1, $2, $3, $4, 'pending', 0, 3, CURRENT_TIMESTAMP)`
		_, err = tx.ExecContext(ctx, query, callbackJobID, callbackQueue, callbackName, callbackArgs)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}
