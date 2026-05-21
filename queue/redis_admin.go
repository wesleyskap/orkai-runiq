package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

func (r *RedisStorage) handleBatchAck(ctx context.Context, batchID string) {
	batchKey := "runiq:batch:" + batchID
	pending, err := r.client.HIncrBy(ctx, batchKey, "pending", -1).Result()
	if err != nil {
		return
	}
	status, err := r.client.HGet(ctx, batchKey, "status").Result()
	if err != nil || status != "sealed" || pending != 0 {
		return
	}
	_ = r.client.HSet(ctx, batchKey, "status", "completed").Err()
	callbackJSON, err := r.client.HGet(ctx, batchKey, "callback").Result()
	if err != nil || callbackJSON == "" {
		return
	}
	var callbackEnv JobEnvelope
	if err := json.Unmarshal([]byte(callbackJSON), &callbackEnv); err != nil {
		return
	}
	callbackEnv.JobID = generateJobID()
	_ = r.Enqueue(ctx, &callbackEnv)
}

func (r *RedisStorage) Ack(ctx context.Context, jobID string) error {
	data, err := r.client.HGet(ctx, "runiq:jobs", jobID).Result()
	if err == redis.Nil {
		return nil
	}
	if err != nil {
		return err
	}
	var env JobEnvelope
	if err := json.Unmarshal([]byte(data), &env); err != nil {
		return err
	}
	pipe := r.client.Pipeline()
	pipe.SRem(ctx, "runiq:active:"+env.Queue, jobID)
	pipe.LPush(ctx, "runiq:processed:"+env.Queue, jobID)
	pipe.LTrim(ctx, "runiq:processed:"+env.Queue, 0, 49)
	if env.UniqueKey != "" {
		pipe.Del(ctx, "runiq:unique:"+env.Queue+":"+env.UniqueKey)
	}
	_, err = pipe.Exec(ctx)
	if err != nil {
		return err
	}

	if env.BatchID != "" {
		r.handleBatchAck(ctx, env.BatchID)
	}

	return nil
}

func (r *RedisStorage) Fail(ctx context.Context, jobID string, runErr error) error {
	data, err := r.client.HGet(ctx, "runiq:jobs", jobID).Result()
	if err != nil {
		if err == redis.Nil {
			return nil
		}
		return err
	}
	var env JobEnvelope
	if err := json.Unmarshal([]byte(data), &env); err != nil {
		return err
	}
	env.Attempts++
	return r.processFail(ctx, &env, runErr)
}

func (r *RedisStorage) processFail(ctx context.Context, env *JobEnvelope, runErr error) error {
	maxAttempts := env.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 3
	}
	pipe := r.client.Pipeline()
	pipe.SRem(ctx, "runiq:active:"+env.Queue, env.JobID)
	if env.Attempts < maxAttempts {
		nextRun := time.Now().Add(computeBackoffDelay(env.Attempts - 1))
		env.RunAt = &nextRun
		r.rescheduleJob(ctx, pipe, env)
	} else {
		r.handleDeadJob(ctx, pipe, env, runErr)
	}
	_, err := pipe.Exec(ctx)
	return err
}

func (r *RedisStorage) rescheduleJob(ctx context.Context, pipe redis.Pipeliner, env *JobEnvelope) {
	updatedData, _ := json.Marshal(env)
	pipe.HSet(ctx, "runiq:jobs", env.JobID, updatedData)
	pipe.ZAdd(ctx, "runiq:scheduled:"+env.Queue, redis.Z{
		Score:  float64(env.RunAt.Unix()),
		Member: env.JobID,
	})
}

func (r *RedisStorage) handleDeadJob(ctx context.Context, pipe redis.Pipeliner, env *JobEnvelope, runErr error) {
	updatedData, _ := json.Marshal(env)
	pipe.HSet(ctx, "runiq:jobs", env.JobID, updatedData)
	pipe.LPush(ctx, "runiq:dead:"+env.Queue, env.JobID)
	pipe.LTrim(ctx, "runiq:dead:"+env.Queue, 0, 49)
	pipe.HSet(ctx, "runiq:errors", env.JobID, runErr.Error())
	pipe.ZAdd(ctx, "runiq:dead_ttl", redis.Z{
		Score:  float64(time.Now().Unix()),
		Member: env.Queue + ":" + env.JobID,
	})
	if env.UniqueKey != "" {
		pipe.Del(ctx, "runiq:unique:"+env.Queue+":"+env.UniqueKey)
	}
	if env.BatchID != "" {
		pipe.HSet(ctx, "runiq:batch:"+env.BatchID, "status", "failed")
	}
}

func (r *RedisStorage) Retry(ctx context.Context, jobID string) error {
	val, err := r.client.HGet(ctx, "runiq:jobs", jobID).Result()
	if err != nil {
		return err
	}
	var env JobEnvelope
	if err := json.Unmarshal([]byte(val), &env); err != nil {
		return err
	}
	env.Attempts = 0
	env.RunAt = nil
	return r.executeRetryTx(ctx, &env)
}

func (r *RedisStorage) executeRetryTx(ctx context.Context, env *JobEnvelope) error {
	newVal, err := json.Marshal(env)
	if err != nil {
		return err
	}
	pipe := r.client.TxPipeline()
	pipe.HSet(ctx, "runiq:jobs", env.JobID, newVal)
	pipe.LRem(ctx, "runiq:failed:"+env.Queue, 0, env.JobID)
	pipe.LRem(ctx, "runiq:dead:"+env.Queue, 0, env.JobID)
	pipe.ZRem(ctx, "runiq:dead_ttl", env.Queue+":"+env.JobID)
	pipe.HDel(ctx, "runiq:errors", env.JobID)
	pipe.LPush(ctx, "runiq:queue:"+env.Queue, env.JobID)
	_, err = pipe.Exec(ctx)
	return err
}

func (r *RedisStorage) Cancel(ctx context.Context, jobID string) error {
	val, err := r.client.HGet(ctx, "runiq:jobs", jobID).Result()
	if err != nil {
		if err == redis.Nil {
			return nil
		}
		return err
	}
	var env JobEnvelope
	if err := json.Unmarshal([]byte(val), &env); err != nil {
		return err
	}
	return r.executeCancelTx(ctx, &env)
}

func (r *RedisStorage) executeCancelTx(ctx context.Context, env *JobEnvelope) error {
	pipe := r.client.TxPipeline()
	pipe.LRem(ctx, "runiq:queue:"+env.Queue, 0, env.JobID)
	pipe.SRem(ctx, "runiq:active:"+env.Queue, env.JobID)
	pipe.ZRem(ctx, "runiq:scheduled:"+env.Queue, env.JobID)
	pipe.LRem(ctx, "runiq:failed:"+env.Queue, 0, env.JobID)
	pipe.LRem(ctx, "runiq:dead:"+env.Queue, 0, env.JobID)
	pipe.ZRem(ctx, "runiq:dead_ttl", env.Queue+":"+env.JobID)
	pipe.LRem(ctx, "runiq:processed:"+env.Queue, 0, env.JobID)
	pipe.HDel(ctx, "runiq:jobs", env.JobID)
	pipe.HDel(ctx, "runiq:errors", env.JobID)
	if env.UniqueKey != "" {
		pipe.Del(ctx, "runiq:unique:"+env.Queue+":"+env.UniqueKey)
	}
	_, err := pipe.Exec(ctx)
	return err
}

func (r *RedisStorage) collectQueueIDs(ctx context.Context, queue string) ([]string, []string) {
	pIDs, _ := r.client.LRange(ctx, "runiq:queue:"+queue, 0, -1).Result()
	sIDs, _ := r.client.ZRange(ctx, "runiq:scheduled:"+queue, 0, -1).Result()
	aIDs, _ := r.client.SMembers(ctx, "runiq:active:"+queue).Result()
	prIDs, _ := r.client.LRange(ctx, "runiq:processed:"+queue, 0, -1).Result()
	fIDs, _ := r.client.LRange(ctx, "runiq:failed:"+queue, 0, -1).Result()
	dIDs, _ := r.client.LRange(ctx, "runiq:dead:"+queue, 0, -1).Result()

	var allJobIDs []string
	allJobIDs = append(allJobIDs, pIDs...)
	allJobIDs = append(allJobIDs, sIDs...)
	allJobIDs = append(allJobIDs, aIDs...)
	allJobIDs = append(allJobIDs, prIDs...)
	allJobIDs = append(allJobIDs, fIDs...)
	allJobIDs = append(allJobIDs, dIDs...)
	return allJobIDs, dIDs
}

func (r *RedisStorage) scanUniqueKeys(ctx context.Context, queue string) []string {
	var cursor uint64
	var keysToDelete []string
	for {
		keys, nextCursor, err := r.client.Scan(ctx, cursor, "runiq:unique:"+queue+":*", 100).Result()
		if err != nil {
			break
		}
		keysToDelete = append(keysToDelete, keys...)
		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}
	return keysToDelete
}

func (r *RedisStorage) ClearQueue(ctx context.Context, queue string) error {
	allJobIDs, dIDs := r.collectQueueIDs(ctx, queue)
	keysToDelete := r.scanUniqueKeys(ctx, queue)

	pipe := r.client.TxPipeline()
	if len(allJobIDs) > 0 {
		pipe.HDel(ctx, "runiq:jobs", allJobIDs...)
		pipe.HDel(ctx, "runiq:errors", allJobIDs...)
	}
	for _, id := range dIDs {
		pipe.ZRem(ctx, "runiq:dead_ttl", queue+":"+id)
	}
	if len(keysToDelete) > 0 {
		pipe.Del(ctx, keysToDelete...)
	}
	r.deleteQueueKeys(ctx, pipe, queue)
	_, err := pipe.Exec(ctx)
	return err
}

func (r *RedisStorage) deleteQueueKeys(ctx context.Context, pipe redis.Pipeliner, queue string) {
	pipe.Del(ctx, "runiq:queue:"+queue)
	pipe.Del(ctx, "runiq:active:"+queue)
	pipe.Del(ctx, "runiq:scheduled:"+queue)
	pipe.Del(ctx, "runiq:processed:"+queue)
	pipe.Del(ctx, "runiq:failed:"+queue)
	pipe.Del(ctx, "runiq:dead:"+queue)
}


func (r *RedisStorage) IsQueuePaused(ctx context.Context, queue string) (bool, error) {
	res, err := r.client.SIsMember(ctx, "runiq:paused_queues", queue).Result()
	return res, err
}

func (r *RedisStorage) PauseQueue(ctx context.Context, queue string) error {
	err := r.client.SAdd(ctx, "runiq:paused_queues", queue).Err()
	return err
}

func (r *RedisStorage) ResumeQueue(ctx context.Context, queue string) error {
	err := r.client.SRem(ctx, "runiq:paused_queues", queue).Err()
	return err
}

func (r *RedisStorage) GetRunningJobsCount(ctx context.Context, jobName string) (int, error) {
	queues, err := r.client.SMembers(ctx, "runiq:queues").Result()
	if err != nil {
		return 0, err
	}
	var activeIDs []string
	for _, q := range queues {
		ids, err := r.client.SMembers(ctx, "runiq:active:"+q).Result()
		if err == nil {
			activeIDs = append(activeIDs, ids...)
		}
	}
	if len(activeIDs) == 0 {
		return 0, nil
	}
	envelopes, err := r.client.HMGet(ctx, "runiq:jobs", activeIDs...).Result()
	if err != nil {
		return 0, err
	}
	count := 0
	for _, val := range envelopes {
		if val == nil {
			continue
		}
		strVal, ok := val.(string)
		if !ok {
			continue
		}
		var env JobEnvelope
		if err := json.Unmarshal([]byte(strVal), &env); err == nil {
			if env.Name == jobName {
				count++
			}
		}
	}
	return count, nil
}

func (r *RedisStorage) CheckRateLimit(ctx context.Context, jobName string, limit int, period time.Duration) (bool, error) {
	now := time.Now().UnixNano()
	clearBefore := now - period.Nanoseconds()
	key := "runiq:ratelimit:" + jobName

	pipe := r.client.TxPipeline()
	pipe.ZRemRangeByScore(ctx, key, "-inf", fmt.Sprintf("%d", clearBefore))
	zcard := pipe.ZCard(ctx, key)
	_, err := pipe.Exec(ctx)
	if err != nil {
		return false, err
	}

	currentCount := zcard.Val()
	if currentCount >= int64(limit) {
		return false, nil
	}

	err = r.client.ZAdd(ctx, key, redis.Z{
		Score:  float64(now),
		Member: fmt.Sprintf("%d", now),
	}).Err()
	if err != nil {
		return false, err
	}
	r.client.PExpire(ctx, key, period*2)
	return true, nil
}

func (r *RedisStorage) PostponeJob(ctx context.Context, jobID string, queueName string, delay time.Duration) error {
	runAt := time.Now().Add(delay).Unix()
	pipe := r.client.TxPipeline()
	pipe.SRem(ctx, "runiq:active:"+queueName, jobID)
	pipe.ZAdd(ctx, "runiq:scheduled:"+queueName, redis.Z{
		Score:  float64(runAt),
		Member: jobID,
	})
	_, err := pipe.Exec(ctx)
	return err
}

func (r *RedisStorage) LockCronExecution(ctx context.Context, cronName string, executionMinute time.Time) (bool, error) {
	minuteUnix := executionMinute.Truncate(time.Minute).Unix()
	lockKey := fmt.Sprintf("runiq:cron:lock:%s:%d", cronName, minuteUnix)
	ok, err := r.client.SetNX(ctx, lockKey, "1", 5*time.Minute).Result()
	if err != nil {
		return false, fmt.Errorf("failed to acquire cron lock for %q at %v: %w", cronName, executionMinute, err)
	}
	return ok, nil
}

func (r *RedisStorage) GetJobDetail(ctx context.Context, jobID string) (*JobEnvelope, error) {
	data, err := r.client.HGet(ctx, "runiq:jobs", jobID).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var env JobEnvelope
	if err := json.Unmarshal([]byte(data), &env); err != nil {
		return nil, err
	}
	return &env, nil
}

func (r *RedisStorage) RegisterCronJobs(ctx context.Context, crons []CronJob) error {
	pipe := r.client.Pipeline()
	for _, c := range crons {
		detail := CronJobDetail{
			Name:       c.Name,
			Expression: c.Spec,
			Queue:      c.Queue,
			Payload:    string(c.Payload),
		}
		data, err := json.Marshal(&detail)
		if err != nil {
			return err
		}
		pipe.HSet(ctx, "runiq:cron_jobs", c.Name, data)
	}
	_, err := pipe.Exec(ctx)
	return err
}

func (r *RedisStorage) RetryAllFailed(ctx context.Context) error {
	queues, err := r.client.SMembers(ctx, "runiq:queues").Result()
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
	failedIDs, _ := r.client.LRange(ctx, "runiq:failed:"+q, 0, -1).Result()
	deadIDs, _ := r.client.LRange(ctx, "runiq:dead:"+q, 0, -1).Result()
	allIDs := append(failedIDs, deadIDs...)
	for _, id := range allIDs {
		if err := r.Retry(ctx, id); err != nil {
			return err
		}
	}
	return nil
}

func (r *RedisStorage) PurgeAllFailed(ctx context.Context) error {
	queues, err := r.client.SMembers(ctx, "runiq:queues").Result()
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
	failedIDs, _ := r.client.LRange(ctx, "runiq:failed:"+q, 0, -1).Result()
	deadIDs, _ := r.client.LRange(ctx, "runiq:dead:"+q, 0, -1).Result()
	allIDs := append(failedIDs, deadIDs...)
	if len(allIDs) == 0 {
		return nil
	}
	return r.deleteFailedJobs(ctx, q, allIDs, deadIDs)
}

func (r *RedisStorage) deleteFailedJobs(ctx context.Context, q string, ids []string, deadIDs []string) error {
	pipe := r.client.TxPipeline()
	pipe.HDel(ctx, "runiq:jobs", ids...)
	pipe.HDel(ctx, "runiq:errors", ids...)
	pipe.Del(ctx, "runiq:failed:"+q)
	pipe.Del(ctx, "runiq:dead:"+q)
	for _, id := range deadIDs {
		pipe.ZRem(ctx, "runiq:dead_ttl", q+":"+id)
	}
	_, err := pipe.Exec(ctx)
	return err
}

// Ping checks the Redis server connection.
func (r *RedisStorage) Ping(ctx context.Context) error {
	return r.client.Ping(ctx).Err()
}

// PurgeExpiredDLQ removes dead jobs older than the given TTL.
func (r *RedisStorage) PurgeExpiredDLQ(ctx context.Context, ttl time.Duration) error {
	cutoff := time.Now().Add(-ttl)
	expired, err := r.client.ZRangeByScore(ctx, "runiq:dead_ttl", &redis.ZRangeBy{
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
		pipe.ZRem(ctx, "runiq:dead_ttl", member)
		parts := strings.SplitN(member, ":", 2)
		if len(parts) == 2 {
			queue, jobID := parts[0], parts[1]
			pipe.LRem(ctx, "runiq:dead:"+queue, 0, jobID)
			pipe.HDel(ctx, "runiq:jobs", jobID)
			pipe.HDel(ctx, "runiq:errors", jobID)
		}
	}
	_, err := pipe.Exec(ctx)
	return err
}


