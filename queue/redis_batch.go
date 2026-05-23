package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// CreateBatch registers a new batch record with open status and callback details.
func (r *RedisStorage) CreateBatch(ctx context.Context, batchID string, callback *JobEnvelope, expiresAt *time.Time) error {
	callbackJSON, err := json.Marshal(callback)
	if err != nil {
		return err
	}
	batchKey := r.k("runiq:batch:" + batchID)
	pipe := r.client.Pipeline()
	pipe.HSet(ctx, batchKey, "status", "open")
	pipe.HSet(ctx, batchKey, "total", 0)
	pipe.HSet(ctx, batchKey, "pending", 0)
	pipe.HSet(ctx, batchKey, "callback", callbackJSON)
	if expiresAt != nil {
		pipe.HSet(ctx, batchKey, "expires_at", expiresAt.Format(time.RFC3339))
		pipe.ZAdd(ctx, r.k("runiq:batches:expire"), redis.Z{
			Score:  float64(expiresAt.Unix()),
			Member: batchID,
		})
	}
	_, err = pipe.Exec(ctx)
	return err
}

// EnqueueInBatch associates a job envelope with a batch and enqueues it, incrementing batch job counts.
func (r *RedisStorage) EnqueueInBatch(ctx context.Context, batchID string, env *JobEnvelope) error {
	env.BatchID = batchID
	if env.MaxAttempts <= 0 {
		env.MaxAttempts = 3
	}
	if err := r.incrementBatchCounts(ctx, batchID); err != nil {
		return err
	}
	if err := r.acquireUniqueLock(ctx, env); err != nil {
		return err
	}
	return r.enqueueBatchJob(ctx, env)
}

func (r *RedisStorage) incrementBatchCounts(ctx context.Context, batchID string) error {
	batchKey := r.k("runiq:batch:" + batchID)
	pipe := r.client.Pipeline()
	pipe.HIncrBy(ctx, batchKey, "total", 1)
	pipe.HIncrBy(ctx, batchKey, "pending", 1)
	_, err := pipe.Exec(ctx)
	return err
}

func (r *RedisStorage) enqueueBatchJob(ctx context.Context, env *JobEnvelope) error {
	data, err := json.Marshal(env)
	if err != nil {
		return err
	}
	pipe := r.client.Pipeline()
	pipe.HSet(ctx, r.k("runiq:jobs"), env.JobID, data)
	pipe.SAdd(ctx, r.k("runiq:queues"), env.Queue)
	r.addJobToQueue(ctx, pipe, env)
	_, err = pipe.Exec(ctx)
	return err
}

func (r *RedisStorage) addJobToQueue(ctx context.Context, pipe redis.Pipeliner, env *JobEnvelope) {
	if env.RunAt != nil && env.RunAt.After(time.Now()) {
		pipe.ZAdd(ctx, r.k("runiq:scheduled:"+env.Queue), redis.Z{
			Score:  float64(env.RunAt.Unix()),
			Member: env.JobID,
		})
		return
	}
	pipe.LPush(ctx, r.k("runiq:queue:"+env.Queue), env.JobID)
}

// SubmitBatch seals the batch enqueuing phase and triggers callback if all jobs have already completed.
func (r *RedisStorage) SubmitBatch(ctx context.Context, batchID string) error {
	expired, err := r.checkBatchExpired(ctx, batchID)
	if err != nil || expired {
		return err
	}
	batchKey := r.k("runiq:batch:" + batchID)
	if err := r.client.HSet(ctx, batchKey, "status", "sealed").Err(); err != nil {
		return err
	}
	pending, err := r.getPendingCount(ctx, batchKey)
	if err != nil {
		return err
	}
	if pending == 0 {
		return r.completeBatch(ctx, batchKey)
	}
	return nil
}

func (r *RedisStorage) getPendingCount(ctx context.Context, batchKey string) (int, error) {
	pendingStr, err := r.client.HGet(ctx, batchKey, "pending").Result()
	if err != nil {
		return 0, err
	}
	var pending int
	_, _ = fmt.Sscanf(pendingStr, "%d", &pending)
	return pending, nil
}

func (r *RedisStorage) completeBatch(ctx context.Context, batchKey string) error {
	if err := r.client.HSet(ctx, batchKey, "status", "completed").Err(); err != nil {
		return err
	}
	callbackJSON, err := r.client.HGet(ctx, batchKey, "callback").Result()
	if err != nil || callbackJSON == "" {
		return nil
	}
	var callbackEnv JobEnvelope
	if err := json.Unmarshal([]byte(callbackJSON), &callbackEnv); err != nil {
		return nil
	}
	callbackEnv.JobID = generateJobID()
	return r.Enqueue(ctx, &callbackEnv)
}

func (r *RedisStorage) checkBatchExpired(ctx context.Context, batchID string) (bool, error) {
	batchKey := r.k("runiq:batch:" + batchID)
	val, err := r.client.HGet(ctx, batchKey, "expires_at").Result()
	if err == redis.Nil || val == "" {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	exp, err := time.Parse(time.RFC3339, val)
	if err != nil {
		return false, err
	}
	if time.Now().After(exp) {
		return r.failExpiredBatch(ctx, batchKey, batchID)
	}
	return false, nil
}

func (r *RedisStorage) failExpiredBatch(ctx context.Context, key, id string) (bool, error) {
	pipe := r.client.Pipeline()
	pipe.HSet(ctx, key, "status", "failed")
	pipe.ZRem(ctx, r.k("runiq:batches:expire"), id)
	_, err := pipe.Exec(ctx)
	return true, err
}

