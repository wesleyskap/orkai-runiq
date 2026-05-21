package queue

import (
	"context"
	"time"
)

func (p *PostgresStorage) CreateBatch(ctx context.Context, batchID string, callback *JobEnvelope) error {
	query := `
		INSERT INTO runiq_batches (batch_id, callback_queue, callback_name, callback_args, total_jobs, pending_jobs, status)
		VALUES ($1, $2, $3, $4, 0, 0, 'open')`
	_, err := p.db.ExecContext(ctx, query, batchID, callback.Queue, callback.Name, callback.Args)
	return err
}

func (p *PostgresStorage) EnqueueInBatch(ctx context.Context, batchID string, env *JobEnvelope) error {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(ctx, `
		UPDATE runiq_batches
		SET total_jobs = total_jobs + 1, pending_jobs = pending_jobs + 1
		WHERE batch_id = $1`, batchID)
	if err != nil {
		return err
	}

	if err := p.acquireUniqueLock(ctx, tx, env); err != nil {
		return err
	}

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

func (p *PostgresStorage) SubmitBatch(ctx context.Context, batchID string) error {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

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
