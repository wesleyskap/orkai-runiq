package queue

import (
	"context"
	"encoding/json"
	"time"
)

func (p *PostgresStorage) RegisterProcess(ctx context.Context, info *ProcessInfo) error {
	queuesJSON, err := json.Marshal(info.Queues)
	if err != nil {
		return err
	}
	query := `
		INSERT INTO runiq_processes (process_id, concurrency, queues, heartbeat_at)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (process_id) DO UPDATE SET
			concurrency = $2, queues = $3, heartbeat_at = $4`
	_, err = p.db.ExecContext(ctx, query, info.ProcessID, info.Concurrency, string(queuesJSON), info.HeartbeatAt)
	return err
}

func (p *PostgresStorage) HeartbeatProcess(ctx context.Context, processID string) error {
	query := `UPDATE runiq_processes SET heartbeat_at = $2 WHERE process_id = $1`
	_, err := p.db.ExecContext(ctx, query, processID, time.Now())
	return err
}

func (p *PostgresStorage) GetActiveProcesses(ctx context.Context) ([]ProcessInfo, error) {
	deadTimeLimit := time.Now().Add(-15 * time.Second)
	_, _ = p.db.ExecContext(ctx, "DELETE FROM runiq_processes WHERE heartbeat_at < $1", deadTimeLimit)

	rows, err := p.db.QueryContext(ctx, "SELECT process_id, concurrency, queues, heartbeat_at FROM runiq_processes ORDER BY heartbeat_at DESC")
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

func (p *PostgresStorage) LockCronExecution(ctx context.Context, cronName string, executionMinute time.Time) (bool, error) {
	_, _ = p.db.ExecContext(ctx, "DELETE FROM runiq_cron_locks WHERE execution_minute < $1", time.Now().Add(-1*time.Hour))
	res, err := p.db.ExecContext(ctx, `
		INSERT INTO runiq_cron_locks (cron_name, execution_minute, acquired_at)
		VALUES ($1, $2, $3)
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

func (p *PostgresStorage) GetRunningJobsCount(ctx context.Context, jobName string) (int, error) {
	var count int
	err := p.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM runiq_jobs WHERE name = $1 AND status = 'running'", jobName).Scan(&count)
	return count, err
}

func (p *PostgresStorage) CheckRateLimit(ctx context.Context, jobName string, limit int, period time.Duration) (bool, error) {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()

	now := time.Now().UnixNano()
	clearBefore := now - period.Nanoseconds()

	_, err = tx.ExecContext(ctx, "DELETE FROM runiq_rate_limits WHERE job_name = $1 AND request_timestamp < $2", jobName, clearBefore)
	if err != nil {
		return false, err
	}

	var count int
	err = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM runiq_rate_limits WHERE job_name = $1", jobName).Scan(&count)
	if err != nil {
		return false, err
	}

	if count >= limit {
		return false, nil
	}

	_, err = tx.ExecContext(ctx, "INSERT INTO runiq_rate_limits (job_name, request_timestamp) VALUES ($1, $2)", jobName, now)
	if err != nil {
		return false, err
	}

	err = tx.Commit()
	return err == nil, err
}

func (p *PostgresStorage) PostponeJob(ctx context.Context, jobID string, queueName string, delay time.Duration) error {
	runAt := time.Now().Add(delay)
	_, err := p.db.ExecContext(ctx, "UPDATE runiq_jobs SET status = 'pending', run_at = $2, locked_at = NULL WHERE job_id = $1", jobID, runAt)
	return err
}
