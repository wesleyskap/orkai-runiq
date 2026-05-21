package queue

import (
	"context"
	"database/sql"
	"time"
)

func (p *PostgresStorage) deleteUniqueLock(ctx context.Context, tx *sql.Tx, queueName, uniqueKey string) error {
	if uniqueKey == "" {
		return nil
	}
	lockKey := queueName + ":" + uniqueKey
	_, err := tx.ExecContext(ctx, "DELETE FROM runiq_unique_locks WHERE lock_key = $1", lockKey)
	return err
}

func (p *PostgresStorage) handleBatchAck(ctx context.Context, tx *sql.Tx, batchID string) error {
	var pendingJobs int
	var status, callbackQueue, callbackName string
	var callbackArgs []byte
	err := tx.QueryRowContext(ctx, `
		UPDATE runiq_batches
		SET pending_jobs = pending_jobs - 1
		WHERE batch_id = $1
		RETURNING pending_jobs, status, callback_queue, callback_name, callback_args`,
		batchID,
	).Scan(&pendingJobs, &status, &callbackQueue, &callbackName, &callbackArgs)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil
		}
		return err
	}
	if status != "sealed" || pendingJobs != 0 {
		return nil
	}
	_, err = tx.ExecContext(ctx, `UPDATE runiq_batches SET status = 'completed' WHERE batch_id = $1`, batchID)
	if err != nil {
		return err
	}
	callbackJobID := generateJobID()
	query := `
		INSERT INTO runiq_jobs (job_id, queue, name, args, status, attempts, max_attempts, run_at)
		VALUES ($1, $2, $3, $4, 'pending', 0, 3, CURRENT_TIMESTAMP)`
	_, err = tx.ExecContext(ctx, query, callbackJobID, callbackQueue, callbackName, callbackArgs)
	return err
}

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

	if err := p.deleteUniqueLock(ctx, tx, queueName, uniqueKey); err != nil {
		return err
	}

	if batchID != "" {
		if err := p.handleBatchAck(ctx, tx, batchID); err != nil {
			return err
		}
	}

	return tx.Commit()
}

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
		nextRun := time.Now().Add(computeBackoffDelay(attempts))

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

		if err := p.deleteUniqueLock(ctx, tx, queueName, uniqueKey); err != nil {
			return err
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

func (p *PostgresStorage) Retry(ctx context.Context, jobID string) error {
	query := `
		UPDATE runiq_jobs
		SET status = 'pending', attempts = 0, run_at = CURRENT_TIMESTAMP, error_message = ''
		WHERE job_id = $1`
	_, err := p.db.ExecContext(ctx, query, jobID)
	return err
}

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

	if err := p.deleteUniqueLock(ctx, tx, queueName, uniqueKey); err != nil {
		return err
	}
	return tx.Commit()
}

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

func (p *PostgresStorage) GetStats(ctx context.Context) (*Stats, error) {
	var stats Stats
	queueMap, err := p.fetchQueueStats(ctx, &stats)
	if err != nil {
		return nil, err
	}
	p.applyPausedQueues(ctx, queueMap)
	for _, qs := range queueMap {
		stats.Queues = append(stats.Queues, *qs)
	}
	if err := p.fetchRecentJobs(ctx, &stats); err != nil {
		return nil, err
	}
	p.loadActiveProcesses(ctx, &stats)
	return &stats, nil
}

func (p *PostgresStorage) fetchQueueStats(ctx context.Context, stats *Stats) (map[string]*QueueStats, error) {
	query := `
		SELECT queue, status, COUNT(*)
		FROM runiq_jobs
		GROUP BY queue, status`
	rows, err := p.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return p.scanQueueStatsRows(rows, stats)
}

func (p *PostgresStorage) scanQueueStatsRows(rows *sql.Rows, stats *Stats) (map[string]*QueueStats, error) {
	queueMap := make(map[string]*QueueStats)
	for rows.Next() {
		var qName, status string
		var count int64
		if err := rows.Scan(&qName, &status, &count); err != nil {
			return nil, err
		}
		p.accumulateStats(stats, queueMap, qName, status, count)
	}
	return queueMap, nil
}

func (p *PostgresStorage) applyPausedQueues(ctx context.Context, queueMap map[string]*QueueStats) {
	rows, err := p.db.QueryContext(ctx, "SELECT queue FROM runiq_paused_queues")
	if err != nil {
		return
	}
	defer rows.Close()
	for rows.Next() {
		var qName string
		if err := rows.Scan(&qName); err == nil {
			p.getOrCreateQueueStats(queueMap, qName).Paused = true
		}
	}
}

func (p *PostgresStorage) getOrCreateQueueStats(queueMap map[string]*QueueStats, qName string) *QueueStats {
	qs, ok := queueMap[qName]
	if !ok {
		qs = &QueueStats{Name: qName}
		queueMap[qName] = qs
	}
	return qs
}

func (p *PostgresStorage) fetchRecentJobs(ctx context.Context, stats *Stats) error {
	query := `
		SELECT job_id, queue, name, status, trace_id, error_message, created_at
		FROM runiq_jobs
		ORDER BY created_at DESC
		LIMIT 100`
	rows, err := p.db.QueryContext(ctx, query)
	if err != nil {
		return err
	}
	defer rows.Close()
	return p.scanRecentJobsRows(rows, stats)
}

func (p *PostgresStorage) scanRecentJobsRows(rows *sql.Rows, stats *Stats) error {
	for rows.Next() {
		var jd JobDetail
		var createdAt time.Time
		var errMsg sql.NullString
		err := rows.Scan(&jd.JobID, &jd.Queue, &jd.Name, &jd.Status, &jd.TraceID, &errMsg, &createdAt)
		if err != nil {
			return err
		}
		jd.ErrorMessage = errMsg.String
		jd.CreatedAt = createdAt.Format(time.RFC3339)
		stats.Jobs = append(stats.Jobs, jd)
	}
	return nil
}

func (p *PostgresStorage) loadActiveProcesses(ctx context.Context, stats *Stats) {
	activeProcesses, err := p.GetActiveProcesses(ctx)
	if err == nil {
		stats.Processes = activeProcesses
	}
}

func (p *PostgresStorage) IsQueuePaused(ctx context.Context, queue string) (bool, error) {
	var exists bool
	query := "SELECT EXISTS(SELECT 1 FROM runiq_paused_queues WHERE queue = $1)"
	err := p.db.QueryRowContext(ctx, query, queue).Scan(&exists)
	return exists, err
}

func (p *PostgresStorage) PauseQueue(ctx context.Context, queue string) error {
	query := `
		INSERT INTO runiq_paused_queues (queue)
		VALUES ($1)
		ON CONFLICT (queue) DO NOTHING`
	_, err := p.db.ExecContext(ctx, query, queue)
	return err
}

func (p *PostgresStorage) ResumeQueue(ctx context.Context, queue string) error {
	query := "DELETE FROM runiq_paused_queues WHERE queue = $1"
	_, err := p.db.ExecContext(ctx, query, queue)
	return err
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
