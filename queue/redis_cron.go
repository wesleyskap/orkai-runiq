package queue

import (
	"context"
	"encoding/json"
	"sort"
)

// RetryModified resets a failed job back to pending state for re-execution with new args in Redis.
func (r *RedisStorage) RetryModified(ctx context.Context, jobID string, args []byte) error {
	val, err := r.client.HGet(ctx, r.k("runiq:jobs"), jobID).Result()
	if err != nil {
		return err
	}
	var env JobEnvelope
	if err := json.Unmarshal([]byte(val), &env); err != nil {
		return err
	}
	env.Attempts = 0
	env.RunAt = nil
	env.Args = args
	return r.executeRetryTx(ctx, &env)
}

// GetCronSchedules retrieves dynamic cron schedules from Redis.
func (r *RedisStorage) GetCronSchedules(ctx context.Context) ([]CronJob, error) {
	m, err := r.client.HGetAll(ctx, r.k("runiq:cron_schedules")).Result()
	if err != nil {
		return nil, err
	}
	var list []CronJob
	for _, val := range m {
		var c CronJob
		if err := json.Unmarshal([]byte(val), &c); err == nil {
			list = append(list, c)
		}
	}
	sort.Slice(list, func(i, j int) bool {
		return list[i].Name < list[j].Name
	})
	return list, nil
}

// SaveCronSchedule inserts or updates a dynamic cron schedule in Redis.
func (r *RedisStorage) SaveCronSchedule(ctx context.Context, cron CronJob) error {
	data, err := json.Marshal(&cron)
	if err != nil {
		return err
	}
	return r.client.HSet(ctx, r.k("runiq:cron_schedules"), cron.Name, data).Err()
}

// DeleteCronSchedule removes a dynamic cron schedule from Redis.
func (r *RedisStorage) DeleteCronSchedule(ctx context.Context, name string) error {
	err := r.client.HDel(ctx, r.k("runiq:cron_schedules"), name).Err()
	return err
}

