package queue

import (
	"context"
)

// RetryModified resets a failed job back to pending state for re-execution with new args in Postgres.
func (p *PostgresStorage) RetryModified(ctx context.Context, jobID string, args []byte) error {
	query := `
		UPDATE runiq_jobs
		SET status = 'pending', attempts = 0, run_at = CURRENT_TIMESTAMP, error_message = '', args = $1
		WHERE job_id = $2`
	_, err := p.db.ExecContext(ctx, p.q(query), args, jobID)
	return err
}

// GetCronSchedules retrieves dynamic cron schedules from Postgres.
func (p *PostgresStorage) GetCronSchedules(ctx context.Context) ([]CronJob, error) {
	rows, err := p.db.QueryContext(ctx, p.q(`SELECT name, spec, queue, payload, timezone, paused FROM runiq_cron_schedules ORDER BY name ASC`))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var list []CronJob
	for rows.Next() {
		var c CronJob
		var payload []byte
		_ = rows.Scan(&c.Name, &c.Spec, &c.Queue, &payload, &c.Timezone, &c.Paused)
		c.Payload = payload
		list = append(list, c)
	}
	return list, nil
}

// SaveCronSchedule inserts or updates a dynamic cron schedule in Postgres.
func (p *PostgresStorage) SaveCronSchedule(ctx context.Context, cron CronJob) error {
	query := `
		INSERT INTO runiq_cron_schedules (name, spec, queue, payload, timezone, paused, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, CURRENT_TIMESTAMP)
		ON CONFLICT (name) DO UPDATE SET
			spec = EXCLUDED.spec,
			queue = EXCLUDED.queue,
			payload = EXCLUDED.payload,
			timezone = EXCLUDED.timezone,
			paused = EXCLUDED.paused,
			updated_at = CURRENT_TIMESTAMP`
	_, err := p.db.ExecContext(ctx, p.q(query), cron.Name, cron.Spec, cron.Queue, cron.Payload, cron.Timezone, cron.Paused)
	return err
}

// DeleteCronSchedule removes a dynamic cron schedule from Postgres.
func (p *PostgresStorage) DeleteCronSchedule(ctx context.Context, name string) error {
	_, err := p.db.ExecContext(ctx, p.q("DELETE FROM runiq_cron_schedules WHERE name = $1"), name)
	return err
}
