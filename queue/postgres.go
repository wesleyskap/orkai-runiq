package queue

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// PostgresStorage implements Storage interface using PostgreSQL.
type PostgresStorage struct {
	prefix string
	db     *sql.DB
}

func init() {
	RegisterStorageDriver("postgres", func(conn interface{}) (interface{}, error) {
		db, ok := conn.(*sql.DB)
		if !ok {
			return nil, fmt.Errorf("postgres driver requires *sql.DB connection")
		}
		return NewPostgresStorage(db)
	})
}

// NewPostgresStorage instantiates a new PostgreSQL storage engine.
func NewPostgresStorage(db *sql.DB) (*PostgresStorage, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	storage := &PostgresStorage{db: db, prefix: "runiq"}
	if err := storage.initTables(ctx); err != nil {
		return nil, err
	}
	return storage, nil
}

func (p *PostgresStorage) SetNamespace(ns string) {
	if ns == "" {
		p.prefix = "runiq"
	} else {
		p.prefix = ns
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = p.initTables(ctx)
}

func (p *PostgresStorage) q(query string) string {
	if p.prefix == "" || p.prefix == "runiq" {
		return query
	}
	return strings.ReplaceAll(query, "runiq_", p.prefix+"_")
}

func (p *PostgresStorage) initTables(ctx context.Context) error {
	for _, query := range postgresTables {
		if _, err := p.db.ExecContext(ctx, p.q(query)); err != nil {
			return err
		}
	}
	p.runMigrations(ctx)
	return nil
}

func (p *PostgresStorage) runMigrations(ctx context.Context) {
	_, _ = p.db.ExecContext(ctx, p.q("ALTER TABLE runiq_batches ADD COLUMN IF NOT EXISTS expires_at TIMESTAMP WITH TIME ZONE"))
	_, _ = p.db.ExecContext(ctx, p.q("ALTER TABLE runiq_cron_jobs ADD COLUMN IF NOT EXISTS timezone VARCHAR(50) DEFAULT 'UTC'"))
	_, _ = p.db.ExecContext(ctx, p.q("ALTER TABLE runiq_processes ADD COLUMN IF NOT EXISTS min_concurrency INT NOT NULL DEFAULT 0"))
	_, _ = p.db.ExecContext(ctx, p.q("ALTER TABLE runiq_processes ADD COLUMN IF NOT EXISTS max_concurrency INT NOT NULL DEFAULT 0"))
}

func (p *PostgresStorage) acquireUniqueLock(ctx context.Context, q sqlExecutor, env *JobEnvelope) error {
	if env.UniqueKey == "" {
		return nil
	}
	lockKey := env.Queue + ":" + env.UniqueKey
	expiresAt := p.getLockExpires(env)
	if err := p.tryInsertLock(ctx, q, lockKey, env.JobID, expiresAt); err != nil {
		return err
	}
	return p.checkLockCollision(ctx, q, env, lockKey, expiresAt)
}

func (p *PostgresStorage) getLockExpires(env *JobEnvelope) time.Time {
	ttl := env.UniqueTTL
	if ttl <= 0 {
		ttl = 24 * time.Hour
	}
	return time.Now().Add(ttl)
}

func (p *PostgresStorage) tryInsertLock(ctx context.Context, q sqlExecutor, lockKey, jobID string, expiresAt time.Time) error {
	_, err := q.ExecContext(ctx, p.q(`
		INSERT INTO runiq_unique_locks (lock_key, job_id, expires_at)
		VALUES ($1, $2, $3)
		ON CONFLICT (lock_key) DO NOTHING`), lockKey, jobID, expiresAt)
	return err
}

func (p *PostgresStorage) checkLockCollision(ctx context.Context, q sqlExecutor, env *JobEnvelope, lockKey string, expiresAt time.Time) error {
	var existingJobID string
	var existingExpiresAt time.Time
	err := q.QueryRowContext(ctx, p.q("SELECT job_id, expires_at FROM runiq_unique_locks WHERE lock_key = $1"), lockKey).Scan(&existingJobID, &existingExpiresAt)
	if err != nil {
		return err
	}
	if existingJobID == env.JobID {
		return nil
	}
	return p.handleLockCollision(ctx, q, env, lockKey, existingJobID, expiresAt, existingExpiresAt)
}

func (p *PostgresStorage) handleLockCollision(ctx context.Context, q sqlExecutor, env *JobEnvelope, lockKey, existingJobID string, expiresAt, existingExpiresAt time.Time) error {
	var exists bool
	_ = q.QueryRowContext(ctx, p.q("SELECT EXISTS(SELECT 1 FROM runiq_jobs WHERE job_id = $1)"), existingJobID).Scan(&exists)
	if time.Now().Before(existingExpiresAt) && exists {
		return fmt.Errorf("%w: key=%q, existing=%q", ErrDuplicateJob, lockKey, existingJobID)
	}
	_, err := q.ExecContext(ctx, p.q(`
		INSERT INTO runiq_unique_locks (lock_key, job_id, expires_at)
		VALUES ($1, $2, $3)
		ON CONFLICT (lock_key) DO UPDATE SET job_id = $2, expires_at = $3`),
		lockKey, env.JobID, expiresAt)
	return err
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
	return p.insertJob(ctx, env, runAt, maxAttempts)
}

func (p *PostgresStorage) insertJob(ctx context.Context, env *JobEnvelope, runAt time.Time, maxAttempts int) error {
	query := `
		INSERT INTO runiq_jobs (job_id, queue, name, args, trace_id, span_id, status, attempts, max_attempts, run_at, unique_key)
		VALUES ($1, $2, $3, $4, $5, $6, 'pending', 0, $7, $8, $9)
		ON CONFLICT (job_id) DO UPDATE SET
			status = 'pending', attempts = 0, max_attempts = $7, run_at = $8, unique_key = $9, updated_at = CURRENT_TIMESTAMP`
	_, err := p.db.ExecContext(ctx, p.q(query),
		env.JobID, env.Queue, env.Name, env.Args, env.TraceContext.TraceID, env.TraceContext.SpanID,
		maxAttempts, runAt, env.UniqueKey)
	return err
}

// Dequeue fetches the next pending job from PostgreSQL using FOR UPDATE SKIP LOCKED.
func (p *PostgresStorage) Dequeue(ctx context.Context, queueName string) (*JobEnvelope, error) {
	query := `
		UPDATE runiq_jobs SET status = 'running', locked_at = CURRENT_TIMESTAMP
		WHERE job_id = (
			SELECT job_id FROM runiq_jobs
			WHERE queue = $1 AND status = 'pending' AND (run_at IS NULL OR run_at <= CURRENT_TIMESTAMP)
			ORDER BY created_at ASC FOR UPDATE SKIP LOCKED LIMIT 1
		)
		RETURNING job_id, queue, name, args, trace_id, span_id, attempts, max_attempts, unique_key, batch_id`
	row := p.db.QueryRowContext(ctx, p.q(query), queueName)
	var env JobEnvelope
	err := row.Scan(&env.JobID, &env.Queue, &env.Name, &env.Args, &env.TraceContext.TraceID, &env.TraceContext.SpanID, &env.Attempts, &env.MaxAttempts, &env.UniqueKey, &env.BatchID)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &env, err
}

// PollScheduled is a no-op for PostgreSQL since Dequeue filters by run_at natively.
func (p *PostgresStorage) PollScheduled(ctx context.Context, queue string) error {
	// PostgreSQL filters by run_at natively in Dequeue.
	return nil
}

func (p *PostgresStorage) AcquireLeader(ctx context.Context, clientID string, ttl time.Duration) (bool, error) {
	query := `
		INSERT INTO runiq_leader_leases (lease_key, holder_id, expires_at)
		VALUES ('leader', $1, $2)
		ON CONFLICT (lease_key) DO UPDATE
		SET holder_id = EXCLUDED.holder_id, expires_at = EXCLUDED.expires_at
		WHERE runiq_leader_leases.holder_id = $1 OR runiq_leader_leases.expires_at <= CURRENT_TIMESTAMP`
	expiresAt := time.Now().Add(ttl)
	res, err := p.db.ExecContext(ctx, p.q(query), clientID, expiresAt)
	if err != nil {
		return false, err
	}
	rows, err := res.RowsAffected()
	return rows > 0, err
}

func (p *PostgresStorage) ReleaseLeader(ctx context.Context, clientID string) error {
	_, err := p.db.ExecContext(ctx, p.q("DELETE FROM runiq_leader_leases WHERE lease_key = 'leader' AND holder_id = $1"), clientID)
	return err
}

func (p *PostgresStorage) ArchiveJobs(ctx context.Context, age time.Duration) (int64, error) {
	cutoff := time.Now().UTC().Add(-age)
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	ins := "INSERT INTO runiq_archived_jobs SELECT * FROM runiq_jobs WHERE status IN ('processed', 'dead') AND updated_at <= $1 ON CONFLICT (job_id) DO NOTHING"
	if _, err = tx.ExecContext(ctx, p.q(ins), cutoff); err != nil {
		return 0, err
	}
	del := "DELETE FROM runiq_jobs WHERE status IN ('processed', 'dead') AND updated_at <= $1"
	res, err := tx.ExecContext(ctx, p.q(del), cutoff)
	if err != nil {
		return 0, err
	}
	rows, _ := res.RowsAffected()
	return rows, tx.Commit()
}

var postgresTables = []string{
	`CREATE TABLE IF NOT EXISTS runiq_jobs (
		job_id VARCHAR(255) PRIMARY KEY, queue VARCHAR(255) NOT NULL, name VARCHAR(255) NOT NULL, args BYTEA NOT NULL,
		trace_id VARCHAR(255) DEFAULT '', span_id VARCHAR(255) DEFAULT '', status VARCHAR(50) NOT NULL DEFAULT 'pending',
		attempts INT DEFAULT 0, max_attempts INT DEFAULT 3, error_message TEXT DEFAULT '',
		created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
		locked_at TIMESTAMP WITH TIME ZONE, run_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
		unique_key VARCHAR(255) DEFAULT '', batch_id VARCHAR(255) DEFAULT ''
	)`,
	`CREATE TABLE IF NOT EXISTS runiq_archived_jobs (
		job_id VARCHAR(255) PRIMARY KEY, queue VARCHAR(255) NOT NULL, name VARCHAR(255) NOT NULL, args BYTEA NOT NULL,
		trace_id VARCHAR(255) DEFAULT '', span_id VARCHAR(255) DEFAULT '', status VARCHAR(50) NOT NULL,
		attempts INT DEFAULT 0, max_attempts INT DEFAULT 3, error_message TEXT DEFAULT '',
		created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
		locked_at TIMESTAMP WITH TIME ZONE, run_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
		unique_key VARCHAR(255) DEFAULT '', batch_id VARCHAR(255) DEFAULT ''
	)`,
	`CREATE TABLE IF NOT EXISTS runiq_unique_locks (
		lock_key VARCHAR(255) PRIMARY KEY, job_id VARCHAR(255) NOT NULL, expires_at TIMESTAMP WITH TIME ZONE NOT NULL
	)`,
	`CREATE TABLE IF NOT EXISTS runiq_processes (
		process_id VARCHAR(255) PRIMARY KEY, concurrency INT NOT NULL, queues TEXT NOT NULL,
		heartbeat_at TIMESTAMP WITH TIME ZONE NOT NULL, min_concurrency INT NOT NULL DEFAULT 0, max_concurrency INT NOT NULL DEFAULT 0
	)`,
	`CREATE TABLE IF NOT EXISTS runiq_cron_locks (
		cron_name VARCHAR(255) NOT NULL, execution_minute TIMESTAMP WITH TIME ZONE NOT NULL, acquired_at TIMESTAMP WITH TIME ZONE NOT NULL,
		PRIMARY KEY (cron_name, execution_minute)
	)`,
	`CREATE TABLE IF NOT EXISTS runiq_rate_limits (
		job_name VARCHAR(255) NOT NULL, request_timestamp BIGINT NOT NULL
	)`,
	`CREATE INDEX IF NOT EXISTS idx_runiq_rate_limits_ts ON runiq_rate_limits (job_name, request_timestamp)`,
	`CREATE TABLE IF NOT EXISTS runiq_batches (
		batch_id VARCHAR(255) PRIMARY KEY, callback_queue VARCHAR(255) NOT NULL, callback_name VARCHAR(255) NOT NULL, callback_args BYTEA NOT NULL,
		total_jobs INT NOT NULL DEFAULT 0, pending_jobs INT NOT NULL DEFAULT 0, status VARCHAR(50) NOT NULL DEFAULT 'open',
		expires_at TIMESTAMP WITH TIME ZONE, created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
	)`,
	`CREATE TABLE IF NOT EXISTS runiq_paused_queues (
		queue VARCHAR(255) PRIMARY KEY
	)`,
	`CREATE TABLE IF NOT EXISTS runiq_cron_jobs (
		name VARCHAR(255) PRIMARY KEY, expression VARCHAR(255) NOT NULL, queue VARCHAR(255) NOT NULL, payload TEXT NOT NULL,
		timezone VARCHAR(50) DEFAULT 'UTC', updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
	)`,
	`CREATE TABLE IF NOT EXISTS runiq_cron_schedules (
		name VARCHAR(255) PRIMARY KEY, spec VARCHAR(255) NOT NULL, queue VARCHAR(255) NOT NULL, payload BYTEA NOT NULL,
		timezone VARCHAR(50) NOT NULL DEFAULT 'UTC', paused BOOLEAN NOT NULL DEFAULT FALSE,
		created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP, updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
	)`,
	`CREATE TABLE IF NOT EXISTS runiq_job_dependencies (
		job_id VARCHAR(255) NOT NULL, parent_job_id VARCHAR(255) NOT NULL,
		PRIMARY KEY (job_id, parent_job_id),
		FOREIGN KEY (job_id) REFERENCES runiq_jobs(job_id) ON DELETE CASCADE
	)`,
	`CREATE TABLE IF NOT EXISTS runiq_leader_leases (
		lease_key VARCHAR(255) PRIMARY KEY, holder_id VARCHAR(255) NOT NULL, expires_at TIMESTAMP WITH TIME ZONE NOT NULL
	)`,
}
