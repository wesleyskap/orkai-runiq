package queue

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"
)

func (s *SqliteStorage) RegisterProcess(ctx context.Context, info *ProcessInfo) error {
	queuesJSON, err := json.Marshal(info.Queues)
	if err != nil {
		return err
	}
	query := `
		INSERT INTO runiq_processes (process_id, concurrency, queues, heartbeat_at)
		VALUES (?, ?, ?, ?)
		ON CONFLICT (process_id) DO UPDATE SET
			concurrency = ?, queues = ?, heartbeat_at = ?`
	_, err = s.db.ExecContext(ctx, query, info.ProcessID, info.Concurrency, string(queuesJSON), info.HeartbeatAt, info.Concurrency, string(queuesJSON), info.HeartbeatAt)
	return err
}

func (s *SqliteStorage) HeartbeatProcess(ctx context.Context, processID string) error {
	query := `UPDATE runiq_processes SET heartbeat_at = ? WHERE process_id = ?`
	_, err := s.db.ExecContext(ctx, query, time.Now(), processID)
	return err
}

func (s *SqliteStorage) GetActiveProcesses(ctx context.Context) ([]ProcessInfo, error) {
	deadTimeLimit := time.Now().Add(-15 * time.Second)
	_, _ = s.db.ExecContext(ctx, "DELETE FROM runiq_processes WHERE heartbeat_at < ?", deadTimeLimit)

	rows, err := s.db.QueryContext(ctx, "SELECT process_id, concurrency, queues, heartbeat_at FROM runiq_processes ORDER BY heartbeat_at DESC")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var processes []ProcessInfo
	for rows.Next() {
		var info ProcessInfo
		var queuesJSON string
		if err := rows.Scan(&info.ProcessID, &info.Concurrency, &queuesJSON, &info.HeartbeatAt); err != nil {
			return nil, err
		}
		_ = json.Unmarshal([]byte(queuesJSON), &info.Queues)
		processes = append(processes, info)
	}
	return processes, nil
}

func (s *SqliteStorage) LockCronExecution(ctx context.Context, cronName string, executionMinute time.Time) (bool, error) {
	_, _ = s.db.ExecContext(ctx, "DELETE FROM runiq_cron_locks WHERE execution_minute < ?", time.Now().Add(-1*time.Hour))
	res, err := s.db.ExecContext(ctx, `
		INSERT INTO runiq_cron_locks (cron_name, execution_minute, acquired_at)
		VALUES (?, ?, ?)
		ON CONFLICT (cron_name, execution_minute) DO NOTHING`,
		cronName, executionMinute.Truncate(time.Minute), time.Now(),
	)
	if err != nil {
		return false, err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return rows > 0, nil
}

func (s *SqliteStorage) GetRunningJobsCount(ctx context.Context, jobName string) (int, error) {
	var count int
	err := s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM runiq_jobs WHERE name = ? AND status = 'running'", jobName).Scan(&count)
	return count, err
}

func (s *SqliteStorage) CheckRateLimit(ctx context.Context, jobName string, limit int, period time.Duration) (bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	allowed, err := s.evaluateRateLimit(ctx, tx, jobName, limit, period)
	if err != nil || !allowed {
		return allowed, err
	}
	err = tx.Commit()
	return err == nil, err
}

func (s *SqliteStorage) evaluateRateLimit(ctx context.Context, tx *sql.Tx, jobName string, limit int, period time.Duration) (bool, error) {
	now := time.Now().UnixNano()
	clearBefore := now - period.Nanoseconds()
	_, err := tx.ExecContext(ctx, "DELETE FROM runiq_rate_limits WHERE job_name = ? AND request_timestamp < ?", jobName, clearBefore)
	if err != nil {
		return false, err
	}
	var count int
	err = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM runiq_rate_limits WHERE job_name = ?", jobName).Scan(&count)
	if err != nil || count >= limit {
		return false, err
	}
	_, err = tx.ExecContext(ctx, "INSERT INTO runiq_rate_limits (job_name, request_timestamp) VALUES (?, ?)", jobName, now)
	return err == nil, err
}

func (s *SqliteStorage) PostponeJob(ctx context.Context, jobID string, queueName string, delay time.Duration) error {
	runAt := time.Now().Add(delay)
	_, err := s.db.ExecContext(ctx, "UPDATE runiq_jobs SET status = 'pending', run_at = ?, locked_at = NULL WHERE job_id = ?", runAt, jobID)
	return err
}
