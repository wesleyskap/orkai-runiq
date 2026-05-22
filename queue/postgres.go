package queue

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// sqlExecutor captures the shared query interface of *sql.DB and *sql.Tx.
type sqlExecutor interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
}

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

func (p *PostgresStorage) acquireUniqueLock(ctx context.Context, q sqlExecutor, env *JobEnvelope) error {
	if env.UniqueKey == "" {
		return nil
	}
	lockKey := env.Queue + ":" + env.UniqueKey
	ttl := env.UniqueTTL
	if ttl <= 0 {
		ttl = 24 * time.Hour
	}
	expiresAt := time.Now().Add(ttl)

	_, err := q.ExecContext(ctx, `
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
	err = q.QueryRowContext(ctx, `
		SELECT job_id, expires_at FROM runiq_unique_locks WHERE lock_key = $1`,
		lockKey,
	).Scan(&existingJobID, &existingExpiresAt)
	if err != nil {
		return err
	}

	if existingJobID != env.JobID {
		var exists bool
		_ = q.QueryRowContext(ctx, `
			SELECT EXISTS(SELECT 1 FROM runiq_jobs WHERE job_id = $1)`,
			existingJobID,
		).Scan(&exists)

		if time.Now().Before(existingExpiresAt) && exists {
			return fmt.Errorf("%w: key=%q, existing=%q", ErrDuplicateJob, lockKey, existingJobID)
		}

		_, err = q.ExecContext(ctx, `
			INSERT INTO runiq_unique_locks (lock_key, job_id, expires_at)
			VALUES ($1, $2, $3)
			ON CONFLICT (lock_key) DO UPDATE SET job_id = $2, expires_at = $3`,
			lockKey, env.JobID, expiresAt,
		)
		if err != nil {
			return err
		}
	}
	return nil
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

	if err := p.acquireUniqueLock(ctx, p.db, env); err != nil {
		return err
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
		heartbeat_at TIMESTAMP WITH TIME ZONE NOT NULL,
		min_concurrency INT NOT NULL DEFAULT 0,
		max_concurrency INT NOT NULL DEFAULT 0
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
		expires_at TIMESTAMP WITH TIME ZONE,
		created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
	);

	CREATE TABLE IF NOT EXISTS runiq_paused_queues (
		queue VARCHAR(255) PRIMARY KEY
	);

	CREATE TABLE IF NOT EXISTS runiq_cron_jobs (
		name VARCHAR(255) PRIMARY KEY,
		expression VARCHAR(255) NOT NULL,
		queue VARCHAR(255) NOT NULL,
		payload TEXT NOT NULL,
		timezone VARCHAR(50) DEFAULT 'UTC',
		updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
	);

	CREATE TABLE IF NOT EXISTS runiq_cron_schedules (
		name VARCHAR(255) PRIMARY KEY,
		spec VARCHAR(255) NOT NULL,
		queue VARCHAR(255) NOT NULL,
		payload BYTEA NOT NULL,
		timezone VARCHAR(50) NOT NULL DEFAULT 'UTC',
		paused BOOLEAN NOT NULL DEFAULT FALSE,
		created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
	);

	CREATE TABLE IF NOT EXISTS runiq_job_dependencies (
		job_id VARCHAR(255) NOT NULL,
		parent_job_id VARCHAR(255) NOT NULL,
		PRIMARY KEY (job_id, parent_job_id),
		FOREIGN KEY (job_id) REFERENCES runiq_jobs(job_id) ON DELETE CASCADE
	);
	`
	_, err := db.ExecContext(ctx, schema)
	if err != nil {
		return err
	}
	runPostgresMigrations(ctx, db)
	return nil
}

func runPostgresMigrations(ctx context.Context, db *sql.DB) {
	_, _ = db.ExecContext(ctx, "ALTER TABLE runiq_batches ADD COLUMN IF NOT EXISTS expires_at TIMESTAMP WITH TIME ZONE")
	_, _ = db.ExecContext(ctx, "ALTER TABLE runiq_cron_jobs ADD COLUMN IF NOT EXISTS timezone VARCHAR(50) DEFAULT 'UTC'")
	_, _ = db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS runiq_cron_schedules (
			name VARCHAR(255) PRIMARY KEY,
			spec VARCHAR(255) NOT NULL,
			queue VARCHAR(255) NOT NULL,
			payload BYTEA NOT NULL,
			timezone VARCHAR(50) NOT NULL DEFAULT 'UTC',
			paused BOOLEAN NOT NULL DEFAULT FALSE,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
		)`)
	_, _ = db.ExecContext(ctx, "ALTER TABLE runiq_processes ADD COLUMN IF NOT EXISTS min_concurrency INT NOT NULL DEFAULT 0")
	_, _ = db.ExecContext(ctx, "ALTER TABLE runiq_processes ADD COLUMN IF NOT EXISTS max_concurrency INT NOT NULL DEFAULT 0")
}



