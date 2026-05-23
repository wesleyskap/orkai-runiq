package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

func (r *RedisStorage) scanUniqueKeys(ctx context.Context, queue string) []string {
	var cursor uint64
	var keys []string
	for {
		res, nextCursor, err := r.client.Scan(ctx, cursor, r.k("runiq:unique:"+queue+":*"), 100).Result()
		if err != nil {
			break
		}
		keys = append(keys, res...)
		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}
	return keys
}

func (r *RedisStorage) ClearQueue(ctx context.Context, queue string) error {
	allIDs, dIDs := r.collectQueueIDs(ctx, queue)
	keysToDelete := r.scanUniqueKeys(ctx, queue)
	pipe := r.client.TxPipeline()
	if len(allIDs) > 0 {
		pipe.HDel(ctx, r.k("runiq:jobs"), allIDs...)
		pipe.HDel(ctx, r.k("runiq:errors"), allIDs...)
	}
	for _, id := range dIDs {
		pipe.ZRem(ctx, r.k("runiq:dead_ttl"), queue+":"+id)
	}
	if len(keysToDelete) > 0 {
		pipe.Del(ctx, keysToDelete...)
	}
	r.deleteQueueKeys(ctx, pipe, queue)
	_, err := pipe.Exec(ctx)
	return err
}

func (r *RedisStorage) deleteQueueKeys(ctx context.Context, pipe redis.Pipeliner, queue string) {
	pipe.Del(ctx, r.k("runiq:queue:"+queue))
	pipe.Del(ctx, r.k("runiq:active:"+queue))
	pipe.Del(ctx, r.k("runiq:scheduled:"+queue))
	pipe.Del(ctx, r.k("runiq:processed:"+queue))
	pipe.Del(ctx, r.k("runiq:failed:"+queue))
	pipe.Del(ctx, r.k("runiq:dead:"+queue))
	pipe.Del(ctx, r.k("runiq:processed_count:"+queue))
	pipe.Del(ctx, r.k("runiq:dead_count:"+queue))
}

func (r *RedisStorage) IsQueuePaused(ctx context.Context, queue string) (bool, error) {
	res, err := r.client.SIsMember(ctx, r.k("runiq:paused_queues"), queue).Result()
	return res, err
}

func (r *RedisStorage) PauseQueue(ctx context.Context, queue string) error {
	err := r.client.SAdd(ctx, r.k("runiq:paused_queues"), queue).Err()
	return err
}

func (r *RedisStorage) ResumeQueue(ctx context.Context, queue string) error {
	err := r.client.SRem(ctx, r.k("runiq:paused_queues"), queue).Err()
	return err
}

func (r *RedisStorage) GetRunningJobsCount(ctx context.Context, jobName string) (int, error) {
	queues, err := r.client.SMembers(ctx, r.k("runiq:queues")).Result()
	if err != nil {
		return 0, err
	}
	activeIDs, err := r.fetchActiveIDs(ctx, queues)
	if err != nil || len(activeIDs) == 0 {
		return 0, err
	}
	envelopes, err := r.client.HMGet(ctx, r.k("runiq:jobs"), activeIDs...).Result()
	if err != nil {
		return 0, err
	}
	return parseRunningCount(envelopes, jobName), nil
}

func (r *RedisStorage) fetchActiveIDs(ctx context.Context, queues []string) ([]string, error) {
	var activeIDs []string
	for _, q := range queues {
		ids, err := r.client.SMembers(ctx, r.k("runiq:active:"+q)).Result()
		if err == nil {
			activeIDs = append(activeIDs, ids...)
		}
	}
	return activeIDs, nil
}

func parseRunningCount(envelopes []interface{}, jobName string) int {
	count := 0
	for _, val := range envelopes {
		if val == nil {
			continue
		}
		var env JobEnvelope
		if err := json.Unmarshal([]byte(val.(string)), &env); err == nil && env.Name == jobName {
			count++
		}
	}
	return count
}

func (r *RedisStorage) CheckRateLimit(ctx context.Context, jobName string, limit int, period time.Duration) (bool, error) {
	now := time.Now().UnixNano()
	clearBefore := now - period.Nanoseconds()
	key := r.k("runiq:ratelimit:" + jobName)
	pipe := r.client.TxPipeline()
	pipe.ZRemRangeByScore(ctx, key, "-inf", fmt.Sprintf("%d", clearBefore))
	zcard := pipe.ZCard(ctx, key)
	if _, err := pipe.Exec(ctx); err != nil {
		return false, err
	}
	if zcard.Val() >= int64(limit) {
		return false, nil
	}
	err := r.client.ZAdd(ctx, key, redis.Z{Score: float64(now), Member: fmt.Sprintf("%d", now)}).Err()
	if err != nil {
		return false, err
	}
	r.client.PExpire(ctx, key, period*2)
	return true, nil
}

func (r *RedisStorage) PostponeJob(ctx context.Context, jobID string, queueName string, delay time.Duration) error {
	runAt := time.Now().Add(delay).Unix()
	pipe := r.client.TxPipeline()
	pipe.SRem(ctx, r.k("runiq:active:"+queueName), jobID)
	pipe.ZAdd(ctx, r.k("runiq:scheduled:"+queueName), redis.Z{
		Score:  float64(runAt),
		Member: jobID,
	})
	_, err := pipe.Exec(ctx)
	return err
}

func (r *RedisStorage) LockCronExecution(ctx context.Context, cronName string, executionMinute time.Time) (bool, error) {
	minuteUnix := executionMinute.Truncate(time.Minute).Unix()
	lockKey := r.k(fmt.Sprintf("runiq:cron:lock:%s:%d", cronName, minuteUnix))
	ok, err := r.client.SetNX(ctx, lockKey, "1", 5*time.Minute).Result()
	if err != nil {
		return false, fmt.Errorf("failed to acquire cron lock for %q at %v: %w", cronName, executionMinute, err)
	}
	return ok, nil
}

func (r *RedisStorage) GetJobDetail(ctx context.Context, jobID string) (*JobEnvelope, error) {
	data, err := r.client.HGet(ctx, r.k("runiq:jobs"), jobID).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var env JobEnvelope
	err = json.Unmarshal([]byte(data), &env)
	return &env, err
}

func (r *RedisStorage) RegisterCronJobs(ctx context.Context, crons []CronJob) error {
	pipe := r.client.Pipeline()
	for _, c := range crons {
		detail := CronJobDetail{
			Name:       c.Name,
			Expression: c.Spec,
			Queue:      c.Queue,
			Payload:    string(c.Payload),
			Timezone:   c.Timezone,
		}
		data, _ := json.Marshal(&detail)
		pipe.HSet(ctx, r.k("runiq:cron_jobs"), c.Name, data)
	}
	_, err := pipe.Exec(ctx)
	return err
}

func (r *RedisStorage) FailExpiredBatches(ctx context.Context) error {
	now := time.Now().Unix()
	expired, err := r.client.ZRangeByScore(ctx, r.k("runiq:batches:expire"), &redis.ZRangeBy{
		Min: "-inf",
		Max: fmt.Sprintf("%d", now),
	}).Result()
	if err != nil || len(expired) == 0 {
		return err
	}
	return r.failExpiredBatchesTx(ctx, expired)
}

func (r *RedisStorage) failExpiredBatchesTx(ctx context.Context, expired []string) error {
	pipe := r.client.Pipeline()
	for _, batchID := range expired {
		pipe.HSet(ctx, r.k("runiq:batch:"+batchID), "status", "failed")
		pipe.ZRem(ctx, r.k("runiq:batches:expire"), batchID)
	}
	_, err := pipe.Exec(ctx)
	return err
}

func (r *RedisStorage) RetryAllFailed(ctx context.Context) error {
	queues, err := r.client.SMembers(ctx, r.k("runiq:queues")).Result()
	if err != nil {
		return err
	}
	for _, q := range queues {
		if err := r.retryQueueFailed(ctx, q); err != nil {
			return err
		}
	}
	return nil
}

func (r *RedisStorage) retryQueueFailed(ctx context.Context, q string) error {
	failedIDs, _ := r.client.LRange(ctx, r.k("runiq:failed:"+q), 0, -1).Result()
	deadIDs, _ := r.client.LRange(ctx, r.k("runiq:dead:"+q), 0, -1).Result()
	allIDs := append(failedIDs, deadIDs...)
	for _, id := range allIDs {
		if err := r.Retry(ctx, id); err != nil {
			return err
		}
	}
	return nil
}

func (r *RedisStorage) PurgeAllFailed(ctx context.Context) error {
	queues, err := r.client.SMembers(ctx, r.k("runiq:queues")).Result()
	if err != nil {
		return err
	}
	for _, q := range queues {
		if err := r.purgeQueueFailed(ctx, q); err != nil {
			return err
		}
	}
	return nil
}

func (r *RedisStorage) purgeQueueFailed(ctx context.Context, q string) error {
	failedIDs, _ := r.client.LRange(ctx, r.k("runiq:failed:"+q), 0, -1).Result()
	deadIDs, _ := r.client.LRange(ctx, r.k("runiq:dead:"+q), 0, -1).Result()
	allIDs := append(failedIDs, deadIDs...)
	if len(allIDs) == 0 {
		return nil
	}
	return r.deleteFailedJobs(ctx, q, allIDs, deadIDs)
}

func (r *RedisStorage) deleteFailedJobs(ctx context.Context, q string, ids []string, deadIDs []string) error {
	pipe := r.client.TxPipeline()
	pipe.HDel(ctx, r.k("runiq:jobs"), ids...)
	pipe.HDel(ctx, r.k("runiq:errors"), ids...)
	pipe.Del(ctx, r.k("runiq:failed:"+q))
	pipe.Del(ctx, r.k("runiq:dead:"+q))
	pipe.Del(ctx, r.k("runiq:dead_count:"+q))
	for _, id := range deadIDs {
		pipe.ZRem(ctx, r.k("runiq:dead_ttl"), q+":"+id)
	}
	_, err := pipe.Exec(ctx)
	return err
}

func (r *RedisStorage) Ping(ctx context.Context) error {
	err := r.client.Ping(ctx).Err()
	return err
}

func (r *RedisStorage) PurgeExpiredDLQ(ctx context.Context, ttl time.Duration) error {
	cutoff := time.Now().Add(-ttl)
	expired, err := r.client.ZRangeByScore(ctx, r.k("runiq:dead_ttl"), &redis.ZRangeBy{
		Min: "-inf",
		Max: fmt.Sprintf("%d", cutoff.Unix()),
	}).Result()
	if err != nil || len(expired) == 0 {
		return err
	}
	return r.executePurgeDLQ(ctx, expired)
}

func (r *RedisStorage) executePurgeDLQ(ctx context.Context, expired []string) error {
	pipe := r.client.TxPipeline()
	for _, member := range expired {
		pipe.ZRem(ctx, r.k("runiq:dead_ttl"), member)
		parts := strings.SplitN(member, ":", 2)
		if len(parts) == 2 {
			queue, jobID := parts[0], parts[1]
			pipe.LRem(ctx, r.k("runiq:dead:"+queue), 0, jobID)
			pipe.Decr(ctx, r.k("runiq:dead_count:"+queue))
			pipe.HDel(ctx, r.k("runiq:jobs"), jobID)
			pipe.HDel(ctx, r.k("runiq:errors"), jobID)
		}
	}
	_, err := pipe.Exec(ctx)
	return err
}
