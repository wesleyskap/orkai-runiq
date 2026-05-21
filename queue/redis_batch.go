package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

func (r *RedisStorage) CreateBatch(ctx context.Context, batchID string, callback *JobEnvelope) error {
	callbackJSON, err := json.Marshal(callback)
	if err != nil {
		return err
	}
	batchKey := "runiq:batch:" + batchID
	pipe := r.client.Pipeline()
	pipe.HSet(ctx, batchKey, "status", "open")
	pipe.HSet(ctx, batchKey, "total", 0)
	pipe.HSet(ctx, batchKey, "pending", 0)
	pipe.HSet(ctx, batchKey, "callback", callbackJSON)
	_, err = pipe.Exec(ctx)
	return err
}

func (r *RedisStorage) EnqueueInBatch(ctx context.Context, batchID string, env *JobEnvelope) error {
	env.BatchID = batchID
	maxAttempts := env.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 3
	}
	env.MaxAttempts = maxAttempts

	batchKey := "runiq:batch:" + batchID
	pipe := r.client.Pipeline()
	pipe.HIncrBy(ctx, batchKey, "total", 1)
	pipe.HIncrBy(ctx, batchKey, "pending", 1)
	_, err := pipe.Exec(ctx)
	if err != nil {
		return err
	}

	if err := r.acquireUniqueLock(ctx, env); err != nil {
		return err
	}

	data, err := json.Marshal(env)
	if err != nil {
		return err
	}
	enqueuePipe := r.client.Pipeline()
	enqueuePipe.HSet(ctx, "runiq:jobs", env.JobID, data)
	enqueuePipe.SAdd(ctx, "runiq:queues", env.Queue)

	if env.RunAt != nil && env.RunAt.After(time.Now()) {
		enqueuePipe.ZAdd(ctx, "runiq:scheduled:"+env.Queue, redis.Z{
			Score:  float64(env.RunAt.Unix()),
			Member: env.JobID,
		})
	} else {
		enqueuePipe.LPush(ctx, "runiq:queue:"+env.Queue, env.JobID)
	}
	_, err = enqueuePipe.Exec(ctx)
	return err
}

func (r *RedisStorage) SubmitBatch(ctx context.Context, batchID string) error {
	batchKey := "runiq:batch:" + batchID
	err := r.client.HSet(ctx, batchKey, "status", "sealed").Err()
	if err != nil {
		return err
	}

	pendingStr, err := r.client.HGet(ctx, batchKey, "pending").Result()
	if err != nil {
		return err
	}

	var pending int
	_, _ = fmt.Sscanf(pendingStr, "%d", &pending)

	if pending == 0 {
		err = r.client.HSet(ctx, batchKey, "status", "completed").Err()
		if err != nil {
			return err
		}

		callbackJSON, err := r.client.HGet(ctx, batchKey, "callback").Result()
		if err == nil && callbackJSON != "" {
			var callbackEnv JobEnvelope
			if err := json.Unmarshal([]byte(callbackJSON), &callbackEnv); err == nil {
				callbackEnv.JobID = generateJobID()
				_ = r.Enqueue(ctx, &callbackEnv)
			}
		}
	}

	return nil
}
