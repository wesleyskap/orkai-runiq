package queue

import (
	"context"
	"database/sql"
	"time"
)

func (s *SqliteStorage) CreateBatch(ctx context.Context, batchID string, callback *JobEnvelope) error {
	query := `
		INSERT INTO runiq_batches (batch_id, callback_queue, callback_name, callback_args, total_jobs, pending_jobs, status)
		VALUES (?, ?, ?, ?, 0, 0, 'open')`
	_, err := s.db.ExecContext(ctx, query, batchID, callback.Queue, callback.Name, callback.Args)
	return err
}

func (s *SqliteStorage) EnqueueInBatch(ctx context.Context, batchID string, env *JobEnvelope) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := s.updateBatchCount(ctx, tx, batchID); err != nil {
		return err
	}
	if err := s.acquireUniqueLock(ctx, tx, env); err != nil {
		return err
	}
	if err := s.insertBatchJob(ctx, tx, batchID, env); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *SqliteStorage) updateBatchCount(ctx context.Context, tx *sql.Tx, batchID string) error {
	query := "UPDATE runiq_batches SET total_jobs = total_jobs + 1, pending_jobs = pending_jobs + 1 WHERE batch_id = ?"
	_, err := tx.ExecContext(ctx, query, batchID)
	return err
}

func (s *SqliteStorage) insertBatchJob(ctx context.Context, tx *sql.Tx, batchID string, env *JobEnvelope) error {
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
		VALUES (?, ?, ?, ?, ?, ?, 'pending', 0, ?, ?, ?, ?)`
	_, err := tx.ExecContext(ctx, query,
		env.JobID, env.Queue, env.Name, env.Args,
		env.TraceContext.TraceID, env.TraceContext.SpanID,
		maxAttempts, runAt, env.UniqueKey, batchID,
	)
	return err
}

func (s *SqliteStorage) SubmitBatch(ctx context.Context, batchID string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	pending, cq, cn, ca, err := s.sealBatch(ctx, tx, batchID)
	if err != nil {
		return err
	}
	if pending == 0 {
		if err := s.completeBatchAndEnqueue(ctx, tx, batchID, cq, cn, ca); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *SqliteStorage) sealBatch(ctx context.Context, tx *sql.Tx, batchID string) (int, string, string, []byte, error) {
	var pendingJobs int
	var callbackQueue, callbackName string
	var callbackArgs []byte
	query := `
		UPDATE runiq_batches
		SET status = 'sealed'
		WHERE batch_id = ?
		RETURNING pending_jobs, callback_queue, callback_name, callback_args`
	err := tx.QueryRowContext(ctx, query, batchID).Scan(&pendingJobs, &callbackQueue, &callbackName, &callbackArgs)
	return pendingJobs, callbackQueue, callbackName, callbackArgs, err
}

func (s *SqliteStorage) completeBatchAndEnqueue(ctx context.Context, tx *sql.Tx, batchID, cq, cn string, ca []byte) error {
	_, err := tx.ExecContext(ctx, "UPDATE runiq_batches SET status = 'completed' WHERE batch_id = ?", batchID)
	if err != nil {
		return err
	}
	callbackJobID := generateJobID()
	query := `
		INSERT INTO runiq_jobs (job_id, queue, name, args, status, attempts, max_attempts, run_at)
		VALUES (?, ?, ?, ?, 'pending', 0, 3, CURRENT_TIMESTAMP)`
	_, err = tx.ExecContext(ctx, query, callbackJobID, cq, cn, ca)
	return err
}
