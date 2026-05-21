package queue

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// SqliteStorage implements Storage interfaces using SQLite.
type SqliteStorage struct {
	db *sql.DB
}

// NewSqliteStorage instantiates a new SQLite storage engine.
func NewSqliteStorage(db *sql.DB) (*SqliteStorage, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Enable WAL mode for better concurrency in SQLite.
	_, _ = db.ExecContext(ctx, "PRAGMA journal_mode=WAL;")
	_, _ = db.ExecContext(ctx, "PRAGMA busy_timeout=5000;")

	if err := createSqliteTables(ctx, db); err != nil {
		return nil, err
	}
	return &SqliteStorage{db: db}, nil
}

func (s *SqliteStorage) acquireUniqueLock(ctx context.Context, q sqlExecutor, env *JobEnvelope) error {
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
		VALUES (?, ?, ?)
		ON CONFLICT (lock_key) DO NOTHING`,
		lockKey, env.JobID, expiresAt,
	)
	if err != nil {
		return err
	}

	var existingJobID string
	var existingExpiresAt time.Time
	err = q.QueryRowContext(ctx, `
		SELECT job_id, expires_at FROM runiq_unique_locks WHERE lock_key = ?`,
		lockKey,
	).Scan(&existingJobID, &existingExpiresAt)
	if err != nil {
		return err
	}

	if existingJobID != env.JobID {
		var exists bool
		_ = q.QueryRowContext(ctx, `
			SELECT EXISTS(SELECT 1 FROM runiq_jobs WHERE job_id = ?)`,
			existingJobID,
		).Scan(&exists)

		if time.Now().Before(existingExpiresAt) && exists {
			return fmt.Errorf("%w: key=%q, existing=%q", ErrDuplicateJob, lockKey, existingJobID)
		}

		_, err = q.ExecContext(ctx, `
			INSERT INTO runiq_unique_locks (lock_key, job_id, expires_at)
			VALUES (?, ?, ?)
			ON CONFLICT (lock_key) DO UPDATE SET job_id = ?, expires_at = ?`,
			lockKey, env.JobID, expiresAt, env.JobID, expiresAt,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *SqliteStorage) Enqueue(ctx context.Context, env *JobEnvelope) error {
	runAt := time.Now()
	if env.RunAt != nil {
		runAt = *env.RunAt
	}
	maxAttempts := env.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 3
	}

	if err := s.acquireUniqueLock(ctx, s.db, env); err != nil {
		return err
	}

	query := `
		INSERT INTO runiq_jobs (job_id, queue, name, args, trace_id, span_id, status, attempts, max_attempts, run_at, unique_key)
		VALUES (?, ?, ?, ?, ?, ?, 'pending', 0, ?, ?, ?)
		ON CONFLICT (job_id) DO UPDATE SET
			status = 'pending', attempts = 0, max_attempts = ?, run_at = ?, unique_key = ?, updated_at = CURRENT_TIMESTAMP`
	_, err := s.db.ExecContext(ctx, query,
		env.JobID, env.Queue, env.Name, env.Args,
		env.TraceContext.TraceID, env.TraceContext.SpanID,
		maxAttempts, runAt, env.UniqueKey,
		maxAttempts, runAt, env.UniqueKey,
	)
	return err
}

func (s *SqliteStorage) Dequeue(ctx context.Context, queueName string) (*JobEnvelope, error) {
	// Execute update & returning in a single transaction or statement.
	// Since SQLite 3.35.0+, RETURNING works. Let's use BEGIN IMMEDIATE transaction for SQLite Dequeue
	// to make sure concurrent workers do not execute duplicate selects.
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	query := `
		UPDATE runiq_jobs
		SET status = 'running', locked_at = CURRENT_TIMESTAMP
		WHERE job_id = (
			SELECT job_id FROM runiq_jobs
			WHERE queue = ? AND status = 'pending' AND (run_at IS NULL OR strftime('%s', run_at) <= strftime('%s', 'now'))
			ORDER BY created_at ASC
			LIMIT 1
		)
		RETURNING job_id, queue, name, args, trace_id, span_id, attempts, max_attempts, unique_key, batch_id`
	
	row := tx.QueryRowContext(ctx, query, queueName)
	var env JobEnvelope
	err = row.Scan(&env.JobID, &env.Queue, &env.Name, &env.Args, &env.TraceContext.TraceID, &env.TraceContext.SpanID, &env.Attempts, &env.MaxAttempts, &env.UniqueKey, &env.BatchID)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return &env, nil
}

func (s *SqliteStorage) PollScheduled(ctx context.Context, queue string) error {
	return nil
}

func createSqliteTables(ctx context.Context, db *sql.DB) error {
	schema := `
	CREATE TABLE IF NOT EXISTS runiq_jobs (
		job_id TEXT PRIMARY KEY,
		queue TEXT NOT NULL,
		name TEXT NOT NULL,
		args BLOB NOT NULL,
		trace_id TEXT DEFAULT '',
		span_id TEXT DEFAULT '',
		status TEXT NOT NULL DEFAULT 'pending',
		attempts INTEGER DEFAULT 0,
		max_attempts INTEGER DEFAULT 3,
		error_message TEXT DEFAULT '',
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
		updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
		locked_at DATETIME,
		run_at DATETIME DEFAULT CURRENT_TIMESTAMP,
		unique_key TEXT DEFAULT '',
		batch_id TEXT DEFAULT ''
	);

	CREATE TABLE IF NOT EXISTS runiq_unique_locks (
		lock_key TEXT PRIMARY KEY,
		job_id TEXT NOT NULL,
		expires_at DATETIME NOT NULL
	);

	CREATE TABLE IF NOT EXISTS runiq_processes (
		process_id TEXT PRIMARY KEY,
		concurrency INTEGER NOT NULL,
		queues TEXT NOT NULL,
		heartbeat_at DATETIME NOT NULL
	);

	CREATE TABLE IF NOT EXISTS runiq_cron_locks (
		cron_name TEXT NOT NULL,
		execution_minute DATETIME NOT NULL,
		acquired_at DATETIME NOT NULL,
		PRIMARY KEY (cron_name, execution_minute)
	);

	CREATE TABLE IF NOT EXISTS runiq_rate_limits (
		job_name TEXT NOT NULL,
		request_timestamp INTEGER NOT NULL
	);
	CREATE INDEX IF NOT EXISTS idx_runiq_rate_limits_ts ON runiq_rate_limits (job_name, request_timestamp);

	CREATE TABLE IF NOT EXISTS runiq_batches (
		batch_id TEXT PRIMARY KEY,
		callback_queue TEXT NOT NULL,
		callback_name TEXT NOT NULL,
		callback_args BLOB NOT NULL,
		total_jobs INTEGER NOT NULL DEFAULT 0,
		pending_jobs INTEGER NOT NULL DEFAULT 0,
		status TEXT NOT NULL DEFAULT 'open',
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP
	);

	CREATE TABLE IF NOT EXISTS runiq_paused_queues (
		queue TEXT PRIMARY KEY
	);

	CREATE TABLE IF NOT EXISTS runiq_cron_jobs (
		name TEXT PRIMARY KEY,
		expression TEXT NOT NULL,
		queue TEXT NOT NULL,
		payload TEXT NOT NULL,
		updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
	);
	`
	_, err := db.ExecContext(ctx, schema)
	return err
}
