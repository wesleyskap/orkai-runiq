package queue

import (
	"context"
)

// RetryModified resets a failed job back to pending state for re-execution with new args.
func (s *SqliteStorage) RetryModified(ctx context.Context, jobID string, args []byte) error {
	query := `
		UPDATE runiq_jobs
		SET status = 'pending', attempts = 0, run_at = CURRENT_TIMESTAMP, error_message = '', args = ?
		WHERE job_id = ?`
	_, err := s.db.ExecContext(ctx, s.q(query), args, jobID)
	return err
}

// GetCronSchedules retrieves dynamic cron schedules.
func (s *SqliteStorage) GetCronSchedules(ctx context.Context) ([]CronJob, error) {
	rows, err := s.db.QueryContext(ctx, s.q(`SELECT name, spec, queue, payload, timezone, paused FROM runiq_cron_schedules ORDER BY name ASC`))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var list []CronJob
	for rows.Next() {
		var c CronJob
		var payload []byte
		var paused int
		_ = rows.Scan(&c.Name, &c.Spec, &c.Queue, &payload, &c.Timezone, &paused)
		c.Payload = payload
		c.Paused = (paused != 0)
		list = append(list, c)
	}
	return list, nil
}

// SaveCronSchedule inserts or updates a dynamic cron schedule.
func (s *SqliteStorage) SaveCronSchedule(ctx context.Context, cron CronJob) error {
	p := 0
	if cron.Paused {
		p = 1
	}
	query := `
		INSERT INTO runiq_cron_schedules (name, spec, queue, payload, timezone, paused, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
		ON CONFLICT (name) DO UPDATE SET
			spec = excluded.spec,
			queue = excluded.queue,
			payload = excluded.payload,
			timezone = excluded.timezone,
			paused = excluded.paused,
			updated_at = CURRENT_TIMESTAMP`
	_, err := s.db.ExecContext(ctx, s.q(query), cron.Name, cron.Spec, cron.Queue, cron.Payload, cron.Timezone, p)
	return err
}

// DeleteCronSchedule removes a dynamic cron schedule.
func (s *SqliteStorage) DeleteCronSchedule(ctx context.Context, name string) error {
	_, err := s.db.ExecContext(ctx, s.q("DELETE FROM runiq_cron_schedules WHERE name = ?"), name)
	return err
}
