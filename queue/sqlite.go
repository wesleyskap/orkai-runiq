package queue

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// SqliteStorage implements Storage interfaces using SQLite.
type SqliteStorage struct {
	prefix string
	db     *sql.DB
}

func init() {
	RegisterStorageDriver("sqlite", func(conn interface{}) (interface{}, error) {
		db, ok := conn.(*sql.DB)
		if !ok {
			return nil, fmt.Errorf("sqlite driver requires *sql.DB connection")
		}
		return NewSqliteStorage(db)
	})
}

// NewSqliteStorage instantiates a new SQLite storage engine.
func NewSqliteStorage(db *sql.DB) (*SqliteStorage, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _ = db.ExecContext(ctx, "PRAGMA journal_mode=WAL;")
	_, _ = db.ExecContext(ctx, "PRAGMA busy_timeout=5000;")
	storage := &SqliteStorage{db: db, prefix: "runiq"}
	if err := storage.initTables(ctx); err != nil {
		return nil, err
	}
	return storage, nil
}

func (s *SqliteStorage) SetNamespace(ns string) {
	if ns == "" {
		s.prefix = "runiq"
	} else {
		s.prefix = ns
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = s.initTables(ctx)
}

func (s *SqliteStorage) q(query string) string {
	if s.prefix == "" || s.prefix == "runiq" {
		return query
	}
	return strings.ReplaceAll(query, "runiq_", s.prefix+"_")
}

func (s *SqliteStorage) initTables(ctx context.Context) error {
	for _, query := range sqliteTables {
		if _, err := s.db.ExecContext(ctx, s.q(query)); err != nil {
			return err
		}
	}
	s.runMigrations(ctx)
	return nil
}

func (s *SqliteStorage) runMigrations(ctx context.Context) {
	_, _ = s.db.ExecContext(ctx, s.q("ALTER TABLE runiq_batches ADD COLUMN expires_at DATETIME"))
	_, _ = s.db.ExecContext(ctx, s.q("ALTER TABLE runiq_cron_jobs ADD COLUMN timezone TEXT DEFAULT 'UTC'"))
	_, _ = s.db.ExecContext(ctx, s.q("ALTER TABLE runiq_processes ADD COLUMN min_concurrency INTEGER NOT NULL DEFAULT 0"))
	_, _ = s.db.ExecContext(ctx, s.q("ALTER TABLE runiq_processes ADD COLUMN max_concurrency INTEGER NOT NULL DEFAULT 0"))
}

func (s *SqliteStorage) acquireUniqueLock(ctx context.Context, q sqlExecutor, env *JobEnvelope) error {
	if env.UniqueKey == "" {
		return nil
	}
	lockKey := env.Queue + ":" + env.UniqueKey
	expiresAt := s.getLockExpires(env)
	if err := s.tryInsertLock(ctx, q, lockKey, env.JobID, expiresAt); err != nil {
		return err
	}
	return s.checkLockCollision(ctx, q, env, lockKey, expiresAt)
}

func (s *SqliteStorage) getLockExpires(env *JobEnvelope) time.Time {
	ttl := env.UniqueTTL
	if ttl <= 0 {
		ttl = 24 * time.Hour
	}
	return time.Now().Add(ttl)
}

func (s *SqliteStorage) tryInsertLock(ctx context.Context, q sqlExecutor, lockKey, jobID string, expiresAt time.Time) error {
	_, err := q.ExecContext(ctx, s.q(`
		INSERT INTO runiq_unique_locks (lock_key, job_id, expires_at)
		VALUES (?, ?, ?)
		ON CONFLICT (lock_key) DO NOTHING`), lockKey, jobID, expiresAt)
	return err
}

func (s *SqliteStorage) checkLockCollision(ctx context.Context, q sqlExecutor, env *JobEnvelope, lockKey string, expiresAt time.Time) error {
	var existingJobID string
	var existingExpiresAt time.Time
	err := q.QueryRowContext(ctx, s.q("SELECT job_id, expires_at FROM runiq_unique_locks WHERE lock_key = ?"), lockKey).Scan(&existingJobID, &existingExpiresAt)
	if err != nil {
		return err
	}
	if existingJobID == env.JobID {
		return nil
	}
	return s.handleLockCollision(ctx, q, env, lockKey, existingJobID, expiresAt, existingExpiresAt)
}

func (s *SqliteStorage) handleLockCollision(ctx context.Context, q sqlExecutor, env *JobEnvelope, lockKey, existingJobID string, expiresAt, existingExpiresAt time.Time) error {
	var exists bool
	_ = q.QueryRowContext(ctx, s.q("SELECT EXISTS(SELECT 1 FROM runiq_jobs WHERE job_id = ?)"), existingJobID).Scan(&exists)
	if time.Now().Before(existingExpiresAt) && exists {
		return fmt.Errorf("%w: key=%q, existing=%q", ErrDuplicateJob, lockKey, existingJobID)
	}
	_, err := q.ExecContext(ctx, s.q(`
		INSERT INTO runiq_unique_locks (lock_key, job_id, expires_at)
		VALUES (?, ?, ?)
		ON CONFLICT (lock_key) DO UPDATE SET job_id = ?, expires_at = ?`),
		lockKey, env.JobID, expiresAt, env.JobID, expiresAt)
	return err
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
	return s.insertJob(ctx, env, runAt, maxAttempts)
}

func (s *SqliteStorage) insertJob(ctx context.Context, env *JobEnvelope, runAt time.Time, maxAttempts int) error {
	query := `
		INSERT INTO runiq_jobs (job_id, queue, name, args, trace_id, span_id, status, attempts, max_attempts, run_at, unique_key)
		VALUES (?, ?, ?, ?, ?, ?, 'pending', 0, ?, ?, ?)
		ON CONFLICT (job_id) DO UPDATE SET
			status = 'pending', attempts = 0, max_attempts = ?, run_at = ?, unique_key = ?, updated_at = CURRENT_TIMESTAMP`
	_, err := s.db.ExecContext(ctx, s.q(query),
		env.JobID, env.Queue, env.Name, env.Args, env.TraceContext.TraceID, env.TraceContext.SpanID,
		maxAttempts, runAt, env.UniqueKey, maxAttempts, runAt, env.UniqueKey)
	return err
}

func (s *SqliteStorage) Dequeue(ctx context.Context, queueName string) (*JobEnvelope, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	env, err := s.dequeueTx(ctx, tx, queueName)
	if err != nil || env == nil {
		return env, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return env, nil
}

func (s *SqliteStorage) dequeueTx(ctx context.Context, tx *sql.Tx, queueName string) (*JobEnvelope, error) {
	query := `
		UPDATE runiq_jobs SET status = 'running', locked_at = CURRENT_TIMESTAMP
		WHERE job_id = (
			SELECT job_id FROM runiq_jobs
			WHERE queue = ? AND status = 'pending' AND (run_at IS NULL OR strftime('%s', run_at) <= strftime('%s', 'now'))
			ORDER BY created_at ASC LIMIT 1
		)
		RETURNING job_id, queue, name, args, trace_id, span_id, attempts, max_attempts, unique_key, batch_id`
	var env JobEnvelope
	err := tx.QueryRowContext(ctx, s.q(query), queueName).Scan(
		&env.JobID, &env.Queue, &env.Name, &env.Args, &env.TraceContext.TraceID,
		&env.TraceContext.SpanID, &env.Attempts, &env.MaxAttempts, &env.UniqueKey, &env.BatchID)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return &env, err
}

func (s *SqliteStorage) PollScheduled(ctx context.Context, queue string) error {
	// SQLite uses inline scheduling/run_at check in Dequeue.
	return nil
}

func (s *SqliteStorage) AcquireLeader(ctx context.Context, clientID string, ttl time.Duration) (bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	ok, err := s.tryAcquire(ctx, tx, clientID, ttl)
	if err != nil || !ok {
		return false, err
	}
	return tx.Commit() == nil, nil
}

func (s *SqliteStorage) tryAcquire(ctx context.Context, tx *sql.Tx, clientID string, ttl time.Duration) (bool, error) {
	var holder string
	var expires time.Time
	err := tx.QueryRowContext(ctx, s.q("SELECT holder_id, expires_at FROM runiq_leader_leases WHERE lease_key = 'leader'")).Scan(&holder, &expires)
	if err == sql.ErrNoRows {
		_, err = tx.ExecContext(ctx, s.q("INSERT INTO runiq_leader_leases (lease_key, holder_id, expires_at) VALUES ('leader', ?, ?)"), clientID, time.Now().Add(ttl))
		return err == nil, err
	}
	if err != nil || (holder != clientID && time.Now().Before(expires)) {
		return false, err
	}
	_, err = tx.ExecContext(ctx, s.q("UPDATE runiq_leader_leases SET holder_id = ?, expires_at = ? WHERE lease_key = 'leader'"), clientID, time.Now().Add(ttl))
	return err == nil, err
}

func (s *SqliteStorage) ReleaseLeader(ctx context.Context, clientID string) error {
	_, err := s.db.ExecContext(ctx, s.q("DELETE FROM runiq_leader_leases WHERE lease_key = 'leader' AND holder_id = ?"), clientID)
	return err
}

func (s *SqliteStorage) ArchiveJobs(ctx context.Context, age time.Duration) (int64, error) {
	cutoff := time.Now().UTC().Add(-age)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	ins := "INSERT INTO runiq_archived_jobs SELECT * FROM runiq_jobs WHERE status IN ('processed', 'dead') AND updated_at <= ?"
	if _, err = tx.ExecContext(ctx, s.q(ins), cutoff); err != nil {
		return 0, err
	}
	del := "DELETE FROM runiq_jobs WHERE status IN ('processed', 'dead') AND updated_at <= ?"
	res, err := tx.ExecContext(ctx, s.q(del), cutoff)
	if err != nil {
		return 0, err
	}
	rows, _ := res.RowsAffected()
	return rows, tx.Commit()
}

var sqliteTables = []string{
	`CREATE TABLE IF NOT EXISTS runiq_jobs (
		job_id TEXT PRIMARY KEY, queue TEXT NOT NULL, name TEXT NOT NULL, args BLOB NOT NULL,
		trace_id TEXT DEFAULT '', span_id TEXT DEFAULT '', status TEXT NOT NULL DEFAULT 'pending',
		attempts INTEGER DEFAULT 0, max_attempts INTEGER DEFAULT 3, error_message TEXT DEFAULT '',
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP, updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
		locked_at DATETIME, run_at DATETIME DEFAULT CURRENT_TIMESTAMP, unique_key TEXT DEFAULT '', batch_id TEXT DEFAULT ''
	)`,
	`CREATE TABLE IF NOT EXISTS runiq_archived_jobs (
		job_id TEXT PRIMARY KEY, queue TEXT NOT NULL, name TEXT NOT NULL, args BLOB NOT NULL,
		trace_id TEXT DEFAULT '', span_id TEXT DEFAULT '', status TEXT NOT NULL,
		attempts INTEGER DEFAULT 0, max_attempts INTEGER DEFAULT 3, error_message TEXT DEFAULT '',
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP, updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
		locked_at DATETIME, run_at DATETIME DEFAULT CURRENT_TIMESTAMP, unique_key TEXT DEFAULT '', batch_id TEXT DEFAULT ''
	)`,
	`CREATE TABLE IF NOT EXISTS runiq_unique_locks (
		lock_key TEXT PRIMARY KEY, job_id TEXT NOT NULL, expires_at DATETIME NOT NULL
	)`,
	`CREATE TABLE IF NOT EXISTS runiq_processes (
		process_id TEXT PRIMARY KEY, concurrency INTEGER NOT NULL, queues TEXT NOT NULL,
		heartbeat_at DATETIME NOT NULL, min_concurrency INTEGER NOT NULL DEFAULT 0, max_concurrency INTEGER NOT NULL DEFAULT 0
	)`,
	`CREATE TABLE IF NOT EXISTS runiq_cron_locks (
		cron_name TEXT NOT NULL, execution_minute DATETIME NOT NULL, acquired_at DATETIME NOT NULL,
		PRIMARY KEY (cron_name, execution_minute)
	)`,
	`CREATE TABLE IF NOT EXISTS runiq_rate_limits (
		job_name TEXT NOT NULL, request_timestamp INTEGER NOT NULL
	)`,
	`CREATE INDEX IF NOT EXISTS idx_runiq_rate_limits_ts ON runiq_rate_limits (job_name, request_timestamp)`,
	`CREATE TABLE IF NOT EXISTS runiq_batches (
		batch_id TEXT PRIMARY KEY, callback_queue TEXT NOT NULL, callback_name TEXT NOT NULL, callback_args BLOB NOT NULL,
		total_jobs INTEGER NOT NULL DEFAULT 0, pending_jobs INTEGER NOT NULL DEFAULT 0, status TEXT NOT NULL DEFAULT 'open',
		expires_at DATETIME, created_at DATETIME DEFAULT CURRENT_TIMESTAMP
	)`,
	`CREATE TABLE IF NOT EXISTS runiq_paused_queues (
		queue TEXT PRIMARY KEY
	)`,
	`CREATE TABLE IF NOT EXISTS runiq_cron_jobs (
		name TEXT PRIMARY KEY, expression TEXT NOT NULL, queue TEXT NOT NULL, payload TEXT NOT NULL,
		timezone TEXT DEFAULT 'UTC', updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
	)`,
	`CREATE TABLE IF NOT EXISTS runiq_cron_schedules (
		name TEXT PRIMARY KEY, spec TEXT NOT NULL, queue TEXT NOT NULL, payload BLOB NOT NULL,
		timezone TEXT NOT NULL DEFAULT 'UTC', paused INTEGER NOT NULL DEFAULT 0,
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP, updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
	)`,
	`CREATE TABLE IF NOT EXISTS runiq_job_dependencies (
		job_id TEXT NOT NULL, parent_job_id TEXT NOT NULL,
		PRIMARY KEY (job_id, parent_job_id),
		FOREIGN KEY (job_id) REFERENCES runiq_jobs(job_id) ON DELETE CASCADE
	)`,
	`CREATE TABLE IF NOT EXISTS runiq_leader_leases (
		lease_key TEXT PRIMARY KEY, holder_id TEXT NOT NULL, expires_at DATETIME NOT NULL
	)`,
}
