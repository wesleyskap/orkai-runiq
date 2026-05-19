package queue

import (
	"context"
	"encoding/json"

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

// Enqueue persists a job envelope into Redis hash and pushes the job ID onto the queue list.
func (r *RedisStorage) Enqueue(ctx context.Context, env *JobEnvelope) error {
	data, err := json.Marshal(env)
	if err != nil {
		return err
	}
	pipe := r.client.Pipeline()
	pipe.HSet(ctx, "runiq:jobs", env.JobID, data)
	pipe.LPush(ctx, "runiq:queue:"+env.Queue, env.JobID)
	pipe.SAdd(ctx, "runiq:queues", env.Queue)
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
	return &env, nil
}

// Ack removes the job from the active jobs hash.
func (r *RedisStorage) Ack(ctx context.Context, jobID string) error {
	return r.client.HDel(ctx, "runiq:jobs", jobID).Err()
}

// Fail deletes or transitions the job details on failure.
func (r *RedisStorage) Fail(ctx context.Context, jobID string, runErr error) error {
	return r.client.HDel(ctx, "runiq:jobs", jobID).Err()
}

// GetStats retrieves the current statistics of jobs in Redis.
func (r *RedisStorage) GetStats(ctx context.Context) (*Stats, error) {
	queues, err := r.client.SMembers(ctx, "runiq:queues").Result()
	if err != nil {
		return nil, err
	}
	var stats Stats
	for _, q := range queues {
		pending, err := r.client.LLen(ctx, "runiq:queue:"+q).Result()
		if err != nil {
			return nil, err
		}
		stats.Pending += pending
		stats.Queues = append(stats.Queues, QueueStats{
			Name:    q,
			Pending: pending,
		})
	}
	return &stats, nil
}
