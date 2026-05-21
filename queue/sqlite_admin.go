package queue

import (
	"context"
	"database/sql"
	"time"
)

func (s *SqliteStorage) deleteUniqueLock(ctx context.Context, tx *sql.Tx, queueName, uniqueKey string) error {
	if uniqueKey == "" {
		return nil
	}
	lockKey := queueName + ":" + uniqueKey
	_, err := tx.ExecContext(ctx, "DELETE FROM runiq_unique_locks WHERE lock_key = ?", lockKey)
	return err
}

func (s *SqliteStorage) handleBatchAck(ctx context.Context, tx *sql.Tx, batchID string) error {
	pending, status, cq, cn, ca, err := s.updateBatchPending(ctx, tx, batchID)
	if err != nil || status != "sealed" || pending != 0 {
		return err
	}
	if err := s.markBatchCompleted(ctx, tx, batchID); err != nil {
		return err
	}
	return s.enqueueCallback(ctx, tx, cq, cn, ca)
}

func (s *SqliteStorage) updateBatchPending(ctx context.Context, tx *sql.Tx, batchID string) (int, string, string, string, []byte, error) {
	var pendingJobs int
	var status, callbackQueue, callbackName string
	var callbackArgs []byte
	err := tx.QueryRowContext(ctx, `
		UPDATE runiq_batches
		SET pending_jobs = pending_jobs - 1
		WHERE batch_id = ?
		RETURNING pending_jobs, status, callback_queue, callback_name, callback_args`,
		batchID,
	).Scan(&pendingJobs, &status, &callbackQueue, &callbackName, &callbackArgs)
	if err == sql.ErrNoRows {
		return 0, "", "", "", nil, nil
	}
	return pendingJobs, status, callbackQueue, callbackName, callbackArgs, err
}

func (s *SqliteStorage) markBatchCompleted(ctx context.Context, tx *sql.Tx, batchID string) error {
	_, err := tx.ExecContext(ctx, `UPDATE runiq_batches SET status = 'completed' WHERE batch_id = ?`, batchID)
	return err
}

func (s *SqliteStorage) enqueueCallback(ctx context.Context, tx *sql.Tx, queue, name string, args []byte) error {
	callbackJobID := generateJobID()
	query := `
		INSERT INTO runiq_jobs (job_id, queue, name, args, status, attempts, max_attempts, run_at)
		VALUES (?, ?, ?, ?, 'pending', 0, 3, CURRENT_TIMESTAMP)`
	_, err := tx.ExecContext(ctx, query, callbackJobID, queue, name, args)
	return err
}

func (s *SqliteStorage) Ack(ctx context.Context, jobID string) error {
	var uniqueKey, queueName, batchID string
	err := s.db.QueryRowContext(ctx, "SELECT unique_key, queue, batch_id FROM runiq_jobs WHERE job_id = ?", jobID).Scan(&uniqueKey, &queueName, &batchID)
	if err != nil && err != sql.ErrNoRows {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := s.performAck(ctx, tx, jobID, queueName, uniqueKey, batchID); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *SqliteStorage) performAck(ctx context.Context, tx *sql.Tx, jobID, queueName, uniqueKey, batchID string) error {
	query := "UPDATE runiq_jobs SET status = 'processed', locked_at = NULL, updated_at = CURRENT_TIMESTAMP WHERE job_id = ?"
	if _, err := tx.ExecContext(ctx, query, jobID); err != nil {
		return err
	}
	if err := s.deleteUniqueLock(ctx, tx, queueName, uniqueKey); err != nil {
		return err
	}
	if batchID != "" {
		return s.handleBatchAck(ctx, tx, batchID)
	}
	return nil
}

func (s *SqliteStorage) Fail(ctx context.Context, jobID string, runErr error) error {
	var attempts, maxAttempts int
	var uniqueKey, queueName, batchID string
	query := "SELECT attempts, max_attempts, unique_key, queue, batch_id FROM runiq_jobs WHERE job_id = ?"
	err := s.db.QueryRowContext(ctx, query, jobID).Scan(&attempts, &maxAttempts, &uniqueKey, &queueName, &batchID)
	if err != nil {
		return err
	}
	if maxAttempts <= 0 {
		maxAttempts = 3
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := s.performFail(ctx, tx, jobID, runErr.Error(), attempts, maxAttempts, uniqueKey, queueName, batchID); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *SqliteStorage) performFail(ctx context.Context, tx *sql.Tx, jobID, errMsg string, attempts, maxAttempts int, uniqueKey, queueName, batchID string) error {
	if attempts+1 < maxAttempts {
		nextRun := time.Now().Add(computeBackoffDelay(attempts))
		query := `
			UPDATE runiq_jobs
			SET status = 'pending', attempts = attempts + 1, run_at = ?, error_message = ?, locked_at = NULL, updated_at = CURRENT_TIMESTAMP
			WHERE job_id = ?`
		_, err := tx.ExecContext(ctx, query, nextRun, errMsg, jobID)
		return err
	}
	return s.transitionToDead(ctx, tx, jobID, errMsg, uniqueKey, queueName, batchID)
}

func (s *SqliteStorage) transitionToDead(ctx context.Context, tx *sql.Tx, jobID, errMsg, uniqueKey, queueName, batchID string) error {
	query := `
		UPDATE runiq_jobs
		SET status = 'dead', error_message = ?, attempts = attempts + 1, locked_at = NULL, updated_at = CURRENT_TIMESTAMP
		WHERE job_id = ?`
	if _, err := tx.ExecContext(ctx, query, errMsg, jobID); err != nil {
		return err
	}
	if err := s.deleteUniqueLock(ctx, tx, queueName, uniqueKey); err != nil {
		return err
	}
	if batchID != "" {
		_, err := tx.ExecContext(ctx, "UPDATE runiq_batches SET status = 'failed' WHERE batch_id = ?", batchID)
		return err
	}
	return nil
}

func (s *SqliteStorage) Retry(ctx context.Context, jobID string) error {
	query := `
		UPDATE runiq_jobs
		SET status = 'pending', attempts = 0, run_at = CURRENT_TIMESTAMP, error_message = ''
		WHERE job_id = ?`
	_, err := s.db.ExecContext(ctx, query, jobID)
	return err
}

func (s *SqliteStorage) Cancel(ctx context.Context, jobID string) error {
	var uniqueKey, queueName string
	err := s.db.QueryRowContext(ctx, "SELECT unique_key, queue FROM runiq_jobs WHERE job_id = ?", jobID).Scan(&uniqueKey, &queueName)
	if err != nil && err != sql.ErrNoRows {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, "DELETE FROM runiq_jobs WHERE job_id = ?", jobID); err != nil {
		return err
	}
	if err := s.deleteUniqueLock(ctx, tx, queueName, uniqueKey); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *SqliteStorage) ClearQueue(ctx context.Context, queue string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, "DELETE FROM runiq_jobs WHERE queue = ?", queue); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM runiq_unique_locks WHERE lock_key LIKE ?", queue+":%"); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *SqliteStorage) GetStats(ctx context.Context) (*Stats, error) {
	var stats Stats
	queueMap, err := s.fetchQueueStats(ctx, &stats)
	if err != nil {
		return nil, err
	}
	s.applyPausedQueues(ctx, queueMap)
	for _, qs := range queueMap {
		stats.Queues = append(stats.Queues, *qs)
	}
	if err := s.fetchRecentJobs(ctx, &stats); err != nil {
		return nil, err
	}
	s.loadActiveProcesses(ctx, &stats)
	return &stats, nil
}

func (s *SqliteStorage) fetchQueueStats(ctx context.Context, stats *Stats) (map[string]*QueueStats, error) {
	query := `
		SELECT queue, status, COUNT(*)
		FROM runiq_jobs
		GROUP BY queue, status`
	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return s.scanQueueStatsRows(rows, stats)
}

func (s *SqliteStorage) scanQueueStatsRows(rows *sql.Rows, stats *Stats) (map[string]*QueueStats, error) {
	queueMap := make(map[string]*QueueStats)
	for rows.Next() {
		var qName, status string
		var count int64
		if err := rows.Scan(&qName, &status, &count); err != nil {
			return nil, err
		}
		s.accumulateStats(stats, queueMap, qName, status, count)
	}
	return queueMap, nil
}

func (s *SqliteStorage) applyPausedQueues(ctx context.Context, queueMap map[string]*QueueStats) {
	rows, err := s.db.QueryContext(ctx, "SELECT queue FROM runiq_paused_queues")
	if err != nil {
		return
	}
	defer rows.Close()
	for rows.Next() {
		var qName string
		if err := rows.Scan(&qName); err == nil {
			s.getOrCreateQueueStats(queueMap, qName).Paused = true
		}
	}
}

func (s *SqliteStorage) getOrCreateQueueStats(queueMap map[string]*QueueStats, qName string) *QueueStats {
	qs, ok := queueMap[qName]
	if !ok {
		qs = &QueueStats{Name: qName}
		queueMap[qName] = qs
	}
	return qs
}

func (s *SqliteStorage) fetchRecentJobs(ctx context.Context, stats *Stats) error {
	query := `
		SELECT job_id, queue, name, status, trace_id, error_message, created_at
		FROM runiq_jobs
		ORDER BY created_at DESC
		LIMIT 100`
	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return err
	}
	defer rows.Close()
	return s.scanRecentJobsRows(rows, stats)
}

func (s *SqliteStorage) scanRecentJobsRows(rows *sql.Rows, stats *Stats) error {
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

func (s *SqliteStorage) loadActiveProcesses(ctx context.Context, stats *Stats) {
	activeProcesses, err := s.GetActiveProcesses(ctx)
	if err == nil {
		stats.Processes = activeProcesses
	}
}

func (s *SqliteStorage) IsQueuePaused(ctx context.Context, queue string) (bool, error) {
	var exists bool
	query := "SELECT EXISTS(SELECT 1 FROM runiq_paused_queues WHERE queue = ?)"
	err := s.db.QueryRowContext(ctx, query, queue).Scan(&exists)
	return exists, err
}

func (s *SqliteStorage) PauseQueue(ctx context.Context, queue string) error {
	query := `
		INSERT INTO runiq_paused_queues (queue)
		VALUES (?)
		ON CONFLICT (queue) DO NOTHING`
	_, err := s.db.ExecContext(ctx, query, queue)
	return err
}

func (s *SqliteStorage) ResumeQueue(ctx context.Context, queue string) error {
	query := "DELETE FROM runiq_paused_queues WHERE queue = ?"
	_, err := s.db.ExecContext(ctx, query, queue)
	return err
}

func (s *SqliteStorage) accumulateStats(stats *Stats, queueMap map[string]*QueueStats, qName, status string, count int64) {
	qs := s.getOrCreateQueueStats(queueMap, qName)
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
