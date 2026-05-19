package queue

import (
	"context"
	"database/sql"
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

// Enqueue persists a job envelope into PostgreSQL.
func (p *PostgresStorage) Enqueue(ctx context.Context, env *JobEnvelope) error {
	runAt := time.Now()
	if env.RunAt != nil {
		runAt = *env.RunAt
	}
	maxAttempts := env.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 3
	}

	query := `
		INSERT INTO runiq_jobs (job_id, queue, name, args, trace_id, span_id, status, attempts, max_attempts, run_at)
		VALUES ($1, $2, $3, $4, $5, $6, 'pending', 0, $7, $8)
		ON CONFLICT (job_id) DO UPDATE SET
			status = 'pending', attempts = 0, max_attempts = $7, run_at = $8, updated_at = CURRENT_TIMESTAMP`
	_, err := p.db.ExecContext(ctx, query,
		env.JobID, env.Queue, env.Name, env.Args,
		env.TraceContext.TraceID, env.TraceContext.SpanID,
		maxAttempts, runAt,
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
		RETURNING job_id, queue, name, args, trace_id, span_id, attempts, max_attempts`
	row := p.db.QueryRowContext(ctx, query, queueName)
	var env JobEnvelope
	err := row.Scan(&env.JobID, &env.Queue, &env.Name, &env.Args, &env.TraceContext.TraceID, &env.TraceContext.SpanID, &env.Attempts, &env.MaxAttempts)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &env, err
}

// Ack marks the job as processed on success.
func (p *PostgresStorage) Ack(ctx context.Context, jobID string) error {
	_, err := p.db.ExecContext(ctx, "UPDATE runiq_jobs SET status = 'processed', locked_at = NULL, updated_at = CURRENT_TIMESTAMP WHERE job_id = $1", jobID)
	return err
}

// Fail updates the status to failed and sets error details. If attempts < max_attempts, schedules a retry.
func (p *PostgresStorage) Fail(ctx context.Context, jobID string, runErr error) error {
	var attempts, maxAttempts int
	err := p.db.QueryRowContext(ctx, "SELECT attempts, max_attempts FROM runiq_jobs WHERE job_id = $1", jobID).Scan(&attempts, &maxAttempts)
	if err != nil {
		return err
	}

	if maxAttempts <= 0 {
		maxAttempts = 3
	}

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
		_, err = p.db.ExecContext(ctx, query, jobID, nextRun, runErr.Error())
	} else {
		query := `
			UPDATE runiq_jobs
			SET status = 'failed', error_message = $2, attempts = attempts + 1, locked_at = NULL, updated_at = CURRENT_TIMESTAMP
			WHERE job_id = $1`
		_, err = p.db.ExecContext(ctx, query, jobID, runErr.Error())
	}
	return err
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
		run_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
	);
	ALTER TABLE runiq_jobs ADD COLUMN IF NOT EXISTS max_attempts INT DEFAULT 3;
	ALTER TABLE runiq_jobs ADD COLUMN IF NOT EXISTS run_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP;
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
	case "failed":
		stats.Failed += count
		qs.Failed += count
	case "processed":
		stats.Processed += count
		qs.Processed += count
	}
}
