package queue

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"
)

func (p *PostgresStorage) RegisterProcess(ctx context.Context, info *ProcessInfo) error {
	queuesJSON, err := json.Marshal(info.Queues)
	if err != nil {
		return err
	}
	query := `
		INSERT INTO runiq_processes (process_id, concurrency, queues, heartbeat_at, min_concurrency, max_concurrency)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (process_id) DO UPDATE SET
			concurrency = $2, queues = $3, heartbeat_at = $4, min_concurrency = $5, max_concurrency = $6`
	_, err = p.db.ExecContext(ctx, p.q(query), info.ProcessID, info.Concurrency, string(queuesJSON), info.HeartbeatAt, info.MinConcurrency, info.MaxConcurrency)
	return err
}

func (p *PostgresStorage) HeartbeatProcess(ctx context.Context, processID string) error {
	query := `UPDATE runiq_processes SET heartbeat_at = $2 WHERE process_id = $1`
	_, err := p.db.ExecContext(ctx, p.q(query), processID, time.Now())
	return err
}

func (p *PostgresStorage) GetActiveProcesses(ctx context.Context) ([]ProcessInfo, error) {
	dead := time.Now().Add(-15 * time.Second)
	_, _ = p.db.ExecContext(ctx, p.q("DELETE FROM runiq_processes WHERE heartbeat_at < $1"), dead)
	rows, err := p.db.QueryContext(ctx, p.q("SELECT process_id, concurrency, queues, heartbeat_at, min_concurrency, max_concurrency FROM runiq_processes ORDER BY heartbeat_at DESC"))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	list, err := scanPgProcesses(rows)
	if err != nil {
		return nil, err
	}
	return p.markLeader(ctx, list)
}

func (p *PostgresStorage) markLeader(ctx context.Context, list []ProcessInfo) ([]ProcessInfo, error) {
	var leader string
	var expires time.Time
	err := p.db.QueryRowContext(ctx, p.q("SELECT holder_id, expires_at FROM runiq_leader_leases WHERE lease_key = 'leader'")).Scan(&leader, &expires)
	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}
	if err == sql.ErrNoRows || time.Now().After(expires) {
		return list, nil
	}
	for i := range list {
		if list[i].ProcessID == leader {
			list[i].IsLeader = true
		}
	}
	return list, nil
}

func scanPgProcesses(rows *sql.Rows) ([]ProcessInfo, error) {
	var list []ProcessInfo
	for rows.Next() {
		var p ProcessInfo
		var q string
		if err := rows.Scan(&p.ProcessID, &p.Concurrency, &q, &p.HeartbeatAt, &p.MinConcurrency, &p.MaxConcurrency); err != nil {
			return nil, err
		}
		_ = json.Unmarshal([]byte(q), &p.Queues)
		list = append(list, p)
	}
	return list, nil
}

func (p *PostgresStorage) LockCronExecution(ctx context.Context, cronName string, executionMinute time.Time) (bool, error) {
	_, _ = p.db.ExecContext(ctx, p.q("DELETE FROM runiq_cron_locks WHERE execution_minute < $1"), time.Now().Add(-1*time.Hour))
	res, err := p.db.ExecContext(ctx, p.q(`
		INSERT INTO runiq_cron_locks (cron_name, execution_minute, acquired_at)
		VALUES ($1, $2, $3)
		ON CONFLICT (cron_name, execution_minute) DO NOTHING`),
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
	err := p.db.QueryRowContext(ctx, p.q("SELECT COUNT(*) FROM runiq_jobs WHERE name = $1 AND status = 'running'"), jobName).Scan(&count)
	return count, err
}

func (p *PostgresStorage) CheckRateLimit(ctx context.Context, jobName string, limit int, period time.Duration) (bool, error) {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	allowed, err := p.evaluateRateLimit(ctx, tx, jobName, limit, period)
	if err != nil || !allowed {
		return allowed, err
	}
	err = tx.Commit()
	return err == nil, err
}

func (p *PostgresStorage) evaluateRateLimit(ctx context.Context, tx *sql.Tx, jobName string, limit int, period time.Duration) (bool, error) {
	now := time.Now().UnixNano()
	clearBefore := now - period.Nanoseconds()
	_, err := tx.ExecContext(ctx, p.q("DELETE FROM runiq_rate_limits WHERE job_name = $1 AND request_timestamp < $2"), jobName, clearBefore)
	if err != nil {
		return false, err
	}
	var count int
	err = tx.QueryRowContext(ctx, p.q("SELECT COUNT(*) FROM runiq_rate_limits WHERE job_name = $1"), jobName).Scan(&count)
	if err != nil || count >= limit {
		return false, err
	}
	_, err = tx.ExecContext(ctx, p.q("INSERT INTO runiq_rate_limits (job_name, request_timestamp) VALUES ($1, $2)"), jobName, now)
	return err == nil, err
}

func (p *PostgresStorage) PostponeJob(ctx context.Context, jobID string, queueName string, delay time.Duration) error {
	runAt := time.Now().Add(delay)
	_, err := p.db.ExecContext(ctx, p.q("UPDATE runiq_jobs SET status = 'pending', run_at = $2, locked_at = NULL WHERE job_id = $1"), jobID, runAt)
	return err
}
