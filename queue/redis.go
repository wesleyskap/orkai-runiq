package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisStorage implements Storage interface using Redis.
type RedisStorage struct {
	client *redis.Client
}

// NewRedisStorage instantiates a new Redis storage engine.
// Usage example:
//	storage, err := queue.NewRedisStorage(client)
func NewRedisStorage(client *redis.Client) (*RedisStorage, error) {
	return &RedisStorage{client: client}, nil
}

// Enqueue persists a job envelope into Redis hash. If scheduled for future, adds to ZSet; otherwise pushes onto queue list.
func (r *RedisStorage) Enqueue(ctx context.Context, env *JobEnvelope) error {
	maxAttempts := env.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 3
	}
	env.MaxAttempts = maxAttempts

	if env.UniqueKey != "" {
		lockKey := "runiq:unique:" + env.Queue + ":" + env.UniqueKey
		ttl := env.UniqueTTL
		if ttl <= 0 {
			ttl = 24 * time.Hour
		}
		ok, err := r.client.SetNX(ctx, lockKey, env.JobID, ttl).Result()
		if err != nil {
			return err
		}
		if !ok {
			existingJobID, err := r.client.Get(ctx, lockKey).Result()
			if err == nil && existingJobID != "" {
				exists, err := r.client.HExists(ctx, "runiq:jobs", existingJobID).Result()
				if err == nil && exists {
					return ErrDuplicateJob
				}
			}
			if err := r.client.Set(ctx, lockKey, env.JobID, ttl).Err(); err != nil {
				return err
			}
		}
	}

	data, err := json.Marshal(env)
	if err != nil {
		return err
	}
	pipe := r.client.Pipeline()
	pipe.HSet(ctx, "runiq:jobs", env.JobID, data)
	pipe.SAdd(ctx, "runiq:queues", env.Queue)

	if env.RunAt != nil && env.RunAt.After(time.Now()) {
		pipe.ZAdd(ctx, "runiq:scheduled:"+env.Queue, redis.Z{
			Score:  float64(env.RunAt.Unix()),
			Member: env.JobID,
		})
	} else {
		pipe.LPush(ctx, "runiq:queue:"+env.Queue, env.JobID)
	}
	_, err = pipe.Exec(ctx)
	return err
}

// Dequeue pops the next job ID from the queue list and retrieves its details from the Redis hash.
func (r *RedisStorage) Dequeue(ctx context.Context, queueName string) (*JobEnvelope, error) {
	jobID, err := r.client.RPop(ctx, "runiq:queue:"+queueName).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
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
	if err := r.client.SAdd(ctx, "runiq:active:"+queueName, jobID).Err(); err != nil {
		return nil, err
	}
	return &env, nil
}

// Ack removes the job from the active jobs and pushes to processed list.
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
	return err
}

// Fail transitions the job from active set to failed list, or schedules a retry with exponential backoff if attempts < max_attempts.
func (r *RedisStorage) Fail(ctx context.Context, jobID string, runErr error) error {
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

	env.Attempts++
	maxAttempts := env.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 3
	}

	if env.Attempts < maxAttempts {
		delaySec := (1 << uint(env.Attempts-1)) * 10
		if delaySec > 3600 {
			delaySec = 3600
		}
		jitterSec := time.Now().Nanosecond() % 3
		nextRun := time.Now().Add(time.Duration(delaySec+jitterSec) * time.Second)
		env.RunAt = &nextRun

		updatedData, err := json.Marshal(&env)
		if err != nil {
			return err
		}

		pipe := r.client.Pipeline()
		pipe.HSet(ctx, "runiq:jobs", env.JobID, updatedData)
		pipe.SRem(ctx, "runiq:active:"+env.Queue, jobID)
		pipe.ZAdd(ctx, "runiq:scheduled:"+env.Queue, redis.Z{
			Score:  float64(nextRun.Unix()),
			Member: jobID,
		})
		_, err = pipe.Exec(ctx)
		return err
	} else {
		updatedData, err := json.Marshal(&env)
		if err != nil {
			return err
		}

		pipe := r.client.Pipeline()
		pipe.HSet(ctx, "runiq:jobs", env.JobID, updatedData)
		pipe.SRem(ctx, "runiq:active:"+env.Queue, jobID)
		pipe.LPush(ctx, "runiq:dead:"+env.Queue, jobID)
		pipe.LTrim(ctx, "runiq:dead:"+env.Queue, 0, 49)
		pipe.HSet(ctx, "runiq:errors", jobID, runErr.Error())
		if env.UniqueKey != "" {
			pipe.Del(ctx, "runiq:unique:"+env.Queue+":"+env.UniqueKey)
		}
		_, err = pipe.Exec(ctx)
		return err
	}
}

// PollScheduled atomically moves scheduled jobs that are due from ZSet to list.
func (r *RedisStorage) PollScheduled(ctx context.Context, queueName string) error {
	now := time.Now().Unix()
	jobIDs, err := r.client.ZRangeByScore(ctx, "runiq:scheduled:"+queueName, &redis.ZRangeBy{
		Min: "-inf",
		Max: fmt.Sprintf("%d", now),
	}).Result()
	if err != nil {
		return err
	}
	if len(jobIDs) == 0 {
		return nil
	}

	pipe := r.client.Pipeline()
	for _, id := range jobIDs {
		pipe.ZRem(ctx, "runiq:scheduled:"+queueName, id)
		pipe.LPush(ctx, "runiq:queue:"+queueName, id)
	}
	_, err = pipe.Exec(ctx)
	return err
}

// GetStats retrieves the current statistics and job listings of jobs in Redis.
func (r *RedisStorage) GetStats(ctx context.Context) (*Stats, error) {
	queues, err := r.client.SMembers(ctx, "runiq:queues").Result()
	if err != nil {
		return nil, err
	}
	var stats Stats
	var allJobIDs []string
	type jobMeta struct {
		id     string
		queue  string
		status string
	}
	var jobsMeta []jobMeta

	for _, q := range queues {
		pending, err := r.client.LLen(ctx, "runiq:queue:"+q).Result()
		if err != nil {
			return nil, err
		}
		scheduled, err := r.client.ZCard(ctx, "runiq:scheduled:"+q).Result()
		if err != nil {
			return nil, err
		}
		active, err := r.client.SCard(ctx, "runiq:active:"+q).Result()
		if err != nil {
			return nil, err
		}
		processed, err := r.client.LLen(ctx, "runiq:processed:"+q).Result()
		if err != nil {
			return nil, err
		}
		failed, err := r.client.LLen(ctx, "runiq:failed:"+q).Result()
		if err != nil {
			return nil, err
		}
		dead, err := r.client.LLen(ctx, "runiq:dead:"+q).Result()
		if err != nil {
			return nil, err
		}

		totalPending := pending + scheduled
		stats.Pending += totalPending
		stats.Running += active
		stats.Processed += processed
		totalFailed := failed + dead
		stats.Failed += totalFailed

		stats.Queues = append(stats.Queues, QueueStats{
			Name:      q,
			Pending:   totalPending,
			Running:   active,
			Processed: processed,
			Failed:    totalFailed,
		})

		pIDs, _ := r.client.LRange(ctx, "runiq:queue:"+q, 0, 49).Result()
		for _, id := range pIDs {
			allJobIDs = append(allJobIDs, id)
			jobsMeta = append(jobsMeta, jobMeta{id: id, queue: q, status: "pending"})
		}
		sIDs, _ := r.client.ZRange(ctx, "runiq:scheduled:"+q, 0, 49).Result()
		for _, id := range sIDs {
			allJobIDs = append(allJobIDs, id)
			jobsMeta = append(jobsMeta, jobMeta{id: id, queue: q, status: "pending"})
		}
		aIDs, _ := r.client.SMembers(ctx, "runiq:active:"+q).Result()
		for _, id := range aIDs {
			allJobIDs = append(allJobIDs, id)
			jobsMeta = append(jobsMeta, jobMeta{id: id, queue: q, status: "running"})
		}
		prIDs, _ := r.client.LRange(ctx, "runiq:processed:"+q, 0, 49).Result()
		for _, id := range prIDs {
			allJobIDs = append(allJobIDs, id)
			jobsMeta = append(jobsMeta, jobMeta{id: id, queue: q, status: "processed"})
		}
		fIDs, _ := r.client.LRange(ctx, "runiq:failed:"+q, 0, 49).Result()
		for _, id := range fIDs {
			allJobIDs = append(allJobIDs, id)
			jobsMeta = append(jobsMeta, jobMeta{id: id, queue: q, status: "failed"})
		}
		dIDs, _ := r.client.LRange(ctx, "runiq:dead:"+q, 0, 49).Result()
		for _, id := range dIDs {
			allJobIDs = append(allJobIDs, id)
			jobsMeta = append(jobsMeta, jobMeta{id: id, queue: q, status: "dead"})
		}
	}

	if len(allJobIDs) > 0 {
		envelopes, err := r.client.HMGet(ctx, "runiq:jobs", allJobIDs...).Result()
		if err == nil {
			errorsMap := make(map[string]string)
			errorsData, errErr := r.client.HGetAll(ctx, "runiq:errors").Result()
			if errErr == nil {
				errorsMap = errorsData
			}

			for i, val := range envelopes {
				if val == nil {
					continue
				}
				strVal, ok := val.(string)
				if !ok {
					continue
				}
				var env JobEnvelope
				if err := json.Unmarshal([]byte(strVal), &env); err == nil {
					meta := jobsMeta[i]
					jd := JobDetail{
						JobID:        env.JobID,
						Queue:        env.Queue,
						Name:         env.Name,
						Status:       meta.status,
						TraceID:      env.TraceContext.TraceID,
						ErrorMessage: errorsMap[env.JobID],
					}
					stats.Jobs = append(stats.Jobs, jd)
				}
			}
		}
	}

	activeProcesses, err := r.GetActiveProcesses(ctx)
	if err == nil {
		stats.Processes = activeProcesses
	}

	return &stats, nil
}

// Retry resets a failed job back to pending state for re-execution.
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

	newVal, err := json.Marshal(&env)
	if err != nil {
		return err
	}

	pipe := r.client.TxPipeline()
	pipe.HSet(ctx, "runiq:jobs", jobID, newVal)
	pipe.LRem(ctx, "runiq:failed:"+env.Queue, 0, jobID)
	pipe.LRem(ctx, "runiq:dead:"+env.Queue, 0, jobID)
	pipe.HDel(ctx, "runiq:errors", jobID)
	pipe.LPush(ctx, "runiq:queue:"+env.Queue, jobID)

	_, err = pipe.Exec(ctx)
	return err
}

// Cancel deletes a pending, scheduled, or failed job from storage.
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

	pipe := r.client.TxPipeline()
	pipe.LRem(ctx, "runiq:queue:"+env.Queue, 0, jobID)
	pipe.SRem(ctx, "runiq:active:"+env.Queue, jobID)
	pipe.ZRem(ctx, "runiq:scheduled:"+env.Queue, jobID)
	pipe.LRem(ctx, "runiq:failed:"+env.Queue, 0, jobID)
	pipe.LRem(ctx, "runiq:dead:"+env.Queue, 0, jobID)
	pipe.LRem(ctx, "runiq:processed:"+env.Queue, 0, jobID)
	pipe.HDel(ctx, "runiq:jobs", jobID)
	pipe.HDel(ctx, "runiq:errors", jobID)
	if env.UniqueKey != "" {
		pipe.Del(ctx, "runiq:unique:"+env.Queue+":"+env.UniqueKey)
	}

	_, err = pipe.Exec(ctx)
	return err
}

// ClearQueue removes all jobs belonging to the specified queue.
func (r *RedisStorage) ClearQueue(ctx context.Context, queue string) error {
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

	var cursor uint64
	var keysToDelete []string
	for {
		var keys []string
		var err error
		keys, cursor, err = r.client.Scan(ctx, cursor, "runiq:unique:"+queue+":*", 100).Result()
		if err != nil {
			return err
		}
		keysToDelete = append(keysToDelete, keys...)
		if cursor == 0 {
			break
		}
	}

	pipe := r.client.TxPipeline()
	if len(allJobIDs) > 0 {
		pipe.HDel(ctx, "runiq:jobs", allJobIDs...)
		pipe.HDel(ctx, "runiq:errors", allJobIDs...)
	}

	if len(keysToDelete) > 0 {
		pipe.Del(ctx, keysToDelete...)
	}

	pipe.Del(ctx, "runiq:queue:"+queue)
	pipe.Del(ctx, "runiq:active:"+queue)
	pipe.Del(ctx, "runiq:scheduled:"+queue)
	pipe.Del(ctx, "runiq:processed:"+queue)
	pipe.Del(ctx, "runiq:failed:"+queue)
	pipe.Del(ctx, "runiq:dead:"+queue)

	_, err := pipe.Exec(ctx)
	return err
}

// RegisterProcess stores a worker process info in Redis.
func (r *RedisStorage) RegisterProcess(ctx context.Context, info *ProcessInfo) error {
	data, err := json.Marshal(info)
	if err != nil {
		return err
	}
	pipe := r.client.Pipeline()
	pipe.HSet(ctx, "runiq:processes", info.ProcessID, data)
	pipe.ZAdd(ctx, "runiq:processes:heartbeat", redis.Z{
		Score:  float64(info.HeartbeatAt.Unix()),
		Member: info.ProcessID,
	})
	_, err = pipe.Exec(ctx)
	return err
}

// HeartbeatProcess updates the process heartbeat timestamp in Redis.
func (r *RedisStorage) HeartbeatProcess(ctx context.Context, processID string) error {
	now := time.Now()
	err := r.client.ZAdd(ctx, "runiq:processes:heartbeat", redis.Z{
		Score:  float64(now.Unix()),
		Member: processID,
	}).Err()
	if err != nil {
		return err
	}

	data, err := r.client.HGet(ctx, "runiq:processes", processID).Result()
	if err == redis.Nil {
		return nil
	}
	if err != nil {
		return err
	}
	var info ProcessInfo
	if err := json.Unmarshal([]byte(data), &info); err != nil {
		return err
	}
	info.HeartbeatAt = now
	updatedData, err := json.Marshal(&info)
	if err != nil {
		return err
	}
	return r.client.HSet(ctx, "runiq:processes", processID, updatedData).Err()
}

// GetActiveProcesses prunes dead processes and returns active ones from Redis.
func (r *RedisStorage) GetActiveProcesses(ctx context.Context) ([]ProcessInfo, error) {
	now := time.Now()
	deadTimeLimit := now.Add(-15 * time.Second).Unix()

	deadIDs, err := r.client.ZRangeByScore(ctx, "runiq:processes:heartbeat", &redis.ZRangeBy{
		Min: "-inf",
		Max: fmt.Sprintf("%d", deadTimeLimit),
	}).Result()
	if err == nil && len(deadIDs) > 0 {
		pipe := r.client.Pipeline()
		pipe.ZRem(ctx, "runiq:processes:heartbeat", r.sliceToInterfaces(deadIDs)...)
		pipe.HDel(ctx, "runiq:processes", deadIDs...)
		_, _ = pipe.Exec(ctx)
	}

	activeIDs, err := r.client.ZRange(ctx, "runiq:processes:heartbeat", 0, -1).Result()
	if err != nil {
		return nil, err
	}
	if len(activeIDs) == 0 {
		return nil, nil
	}

	dataSlice, err := r.client.HMGet(ctx, "runiq:processes", activeIDs...).Result()
	if err != nil {
		return nil, err
	}

	var activeProcesses []ProcessInfo
	for _, raw := range dataSlice {
		if raw == nil {
			continue
		}
		strVal, ok := raw.(string)
		if !ok {
			continue
		}
		var info ProcessInfo
		if err := json.Unmarshal([]byte(strVal), &info); err == nil {
			activeProcesses = append(activeProcesses, info)
		}
	}
	return activeProcesses, nil
}

func (r *RedisStorage) sliceToInterfaces(slice []string) []interface{} {
	result := make([]interface{}, len(slice))
	for i, v := range slice {
		result[i] = v
	}
	return result
}

// LockCronExecution attempts to acquire a unique execution lock for a cron job at a specific minute.
func (r *RedisStorage) LockCronExecution(ctx context.Context, cronName string, executionMinute time.Time) (bool, error) {
	minuteUnix := executionMinute.Truncate(time.Minute).Unix()
	lockKey := fmt.Sprintf("runiq:cron:lock:%s:%d", cronName, minuteUnix)
	ok, err := r.client.SetNX(ctx, lockKey, "1", 5*time.Minute).Result()
	if err != nil {
		return false, fmt.Errorf("failed to acquire cron lock for %q at %v: %w", cronName, executionMinute, err)
	}
	return ok, nil
}

// GetRunningJobsCount returns the number of currently running jobs with the specified name.
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

// CheckRateLimit checks and increments/updates the rate limit window for a job name.
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
	r.client.Expire(ctx, key, period*2)
	return true, nil
}

// PostponeJob postpones a job to be executed in the future without failing it.
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
