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

func (r *RedisStorage) acquireUniqueLock(ctx context.Context, env *JobEnvelope) error {
	if env.UniqueKey == "" {
		return nil
	}
	lockKey := "runiq:unique:" + env.Queue + ":" + env.UniqueKey
	ttl := env.UniqueTTL
	if ttl <= 0 {
		ttl = 24 * time.Hour
	}
	ok, err := r.client.SetNX(ctx, lockKey, env.JobID, ttl).Result()
	if err != nil {
		return err
	}
	if ok {
		return nil
	}

	existingJobID, err := r.client.Get(ctx, lockKey).Result()
	if err == nil && existingJobID != "" {
		exists, err := r.client.HExists(ctx, "runiq:jobs", existingJobID).Result()
		if err == nil && exists {
			return fmt.Errorf("%w: key=%q, existing=%q", ErrDuplicateJob, lockKey, existingJobID)
		}
	}
	return r.client.Set(ctx, lockKey, env.JobID, ttl).Err()
}

// Enqueue persists a job envelope into Redis hash. If scheduled for future, adds to ZSet; otherwise pushes onto queue list.
func (r *RedisStorage) Enqueue(ctx context.Context, env *JobEnvelope) error {
	maxAttempts := env.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 3
	}
	env.MaxAttempts = maxAttempts

	if err := r.acquireUniqueLock(ctx, env); err != nil {
		return err
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
