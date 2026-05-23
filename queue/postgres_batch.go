package queue

import (
	"context"
	"database/sql"
	"time"
)

// CreateBatch registers a new batch record with open status and callback details.
// Usage example:
//	err := storage.CreateBatch(ctx, "batch-123", callback, expiresAt)
func (p *PostgresStorage) CreateBatch(ctx context.Context, batchID string, callback *JobEnvelope, expiresAt *time.Time) error {
	query := `
		INSERT INTO runiq_batches (batch_id, callback_queue, callback_name, callback_args, total_jobs, pending_jobs, status, expires_at)
		VALUES ($1, $2, $3, $4, 0, 0, 'open', $5)`
	_, err := p.db.ExecContext(ctx, p.q(query), batchID, callback.Queue, callback.Name, callback.Args, expiresAt)
	return err
}

// EnqueueInBatch associates a job envelope with a batch and enqueues it, incrementing batch job counts.
// Usage example:
//	err := storage.EnqueueInBatch(ctx, "batch-123", env)
func (p *PostgresStorage) EnqueueInBatch(ctx context.Context, batchID string, env *JobEnvelope) error {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if err := p.updateBatchCount(ctx, tx, batchID); err != nil {
		return err
	}
	if err := p.acquireUniqueLock(ctx, tx, env); err != nil {
		return err
	}
	if err := p.insertBatchJob(ctx, tx, batchID, env); err != nil {
		return err
	}
	return tx.Commit()
}

func (p *PostgresStorage) updateBatchCount(ctx context.Context, tx *sql.Tx, batchID string) error {
	query := "UPDATE runiq_batches SET total_jobs = total_jobs + 1, pending_jobs = pending_jobs + 1 WHERE batch_id = $1"
	_, err := tx.ExecContext(ctx, p.q(query), batchID)
	return err
}

func (p *PostgresStorage) insertBatchJob(ctx context.Context, tx *sql.Tx, batchID string, env *JobEnvelope) error {
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
	_, err := tx.ExecContext(ctx, p.q(query),
		env.JobID, env.Queue, env.Name, env.Args,
		env.TraceContext.TraceID, env.TraceContext.SpanID,
		maxAttempts, runAt, env.UniqueKey, batchID,
	)
	return err
}

// SubmitBatch seals the batch enqueuing phase and triggers callback if all jobs have already completed.
// Usage example:
//	err := storage.SubmitBatch(ctx, "batch-123")
func (p *PostgresStorage) SubmitBatch(ctx context.Context, batchID string) error {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := p.processSubmit(ctx, tx, batchID); err != nil {
		return err
	}
	return tx.Commit()
}

func (p *PostgresStorage) processSubmit(ctx context.Context, tx *sql.Tx, batchID string) error {
	expired, err := p.checkBatchExpired(ctx, tx, batchID)
	if err != nil || expired {
		return err
	}
	pending, cq, cn, ca, err := p.sealBatch(ctx, tx, batchID)
	if err != nil {
		return err
	}
	if pending == 0 {
		return p.completeBatchAndEnqueue(ctx, tx, batchID, cq, cn, ca)
	}
	return nil
}

func (p *PostgresStorage) checkBatchExpired(ctx context.Context, tx *sql.Tx, batchID string) (bool, error) {
	var expiresAt *time.Time
	err := tx.QueryRowContext(ctx, p.q("SELECT expires_at FROM runiq_batches WHERE batch_id = $1"), batchID).Scan(&expiresAt)
	if err != nil {
		return false, err
	}
	if expiresAt != nil && time.Now().After(*expiresAt) {
		_, err = tx.ExecContext(ctx, p.q("UPDATE runiq_batches SET status = 'failed' WHERE batch_id = $1"), batchID)
		return true, err
	}
	return false, nil
}

func (p *PostgresStorage) sealBatch(ctx context.Context, tx *sql.Tx, batchID string) (int, string, string, []byte, error) {
	var pendingJobs int
	var callbackQueue, callbackName string
	var callbackArgs []byte
	query := `
		UPDATE runiq_batches
		SET status = 'sealed'
		WHERE batch_id = $1
		RETURNING pending_jobs, callback_queue, callback_name, callback_args`
	err := tx.QueryRowContext(ctx, p.q(query), batchID).Scan(&pendingJobs, &callbackQueue, &callbackName, &callbackArgs)
	return pendingJobs, callbackQueue, callbackName, callbackArgs, err
}

func (p *PostgresStorage) completeBatchAndEnqueue(ctx context.Context, tx *sql.Tx, batchID, cq, cn string, ca []byte) error {
	_, err := tx.ExecContext(ctx, p.q("UPDATE runiq_batches SET status = 'completed' WHERE batch_id = $1"), batchID)
	if err != nil {
		return err
	}
	callbackJobID := generateJobID()
	query := `
		INSERT INTO runiq_jobs (job_id, queue, name, args, status, attempts, max_attempts, run_at)
		VALUES ($1, $2, $3, $4, 'pending', 0, 3, CURRENT_TIMESTAMP)`
	_, err = tx.ExecContext(ctx, p.q(query), callbackJobID, cq, cn, ca)
	return err
}
