package queue

import (
	"context"
	"encoding/json"
	"time"

	"github.com/redis/go-redis/v9"
)

// EnqueueWorkflow enqueues a group of dependent jobs transactionally.
func (r *RedisStorage) EnqueueWorkflow(ctx context.Context, jobs ...*JobEnvelope) error {
	if len(jobs) == 0 {
		return nil
	}
	pipe := r.client.TxPipeline()
	for _, env := range jobs {
		if err := r.enqueueWorkflowJob(ctx, pipe, env); err != nil {
			return err
		}
	}
	_, err := pipe.Exec(ctx)
	return err
}

func (r *RedisStorage) enqueueWorkflowJob(ctx context.Context, pipe redis.Pipeliner, env *JobEnvelope) error {
	env.MaxAttempts = getMaxAttempts(env.MaxAttempts)
	if err := r.acquireUniqueLock(ctx, env); err != nil {
		return err
	}
	data, err := json.Marshal(env)
	if err != nil {
		return err
	}
	pipe.HSet(ctx, r.k("runiq:jobs"), env.JobID, data)
	pipe.SAdd(ctx, r.k("runiq:queues"), env.Queue)
	if len(env.Dependencies) > 0 {
		return r.enqueueBlockedJob(ctx, pipe, env)
	}
	r.enqueueReadyJob(ctx, pipe, env)
	return nil
}

func (r *RedisStorage) enqueueBlockedJob(ctx context.Context, pipe redis.Pipeliner, env *JobEnvelope) error {
	var parentIDs []interface{}
	for _, pID := range env.Dependencies {
		parentIDs = append(parentIDs, pID)
		pipe.SAdd(ctx, r.k("runiq:job:"+pID+":dependents"), env.JobID)
	}
	pipe.SAdd(ctx, r.k("runiq:job:"+env.JobID+":dependencies"), parentIDs...)
	return nil
}

func (r *RedisStorage) enqueueReadyJob(ctx context.Context, pipe redis.Pipeliner, env *JobEnvelope) {
	if env.RunAt != nil && env.RunAt.After(time.Now()) {
		pipe.ZAdd(ctx, r.k("runiq:scheduled:"+env.Queue), redis.Z{
			Score:  float64(env.RunAt.Unix()),
			Member: env.JobID,
		})
		return
	}
	score := float64(env.Priority) - float64(time.Now().Unix())/1e11
	pipe.ZAdd(ctx, r.k("runiq:queue:"+env.Queue), redis.Z{Score: score, Member: env.JobID})
}

func (r *RedisStorage) resolveDependencies(ctx context.Context, jobID string) error {
	childIDs, err := r.client.SMembers(ctx, r.k("runiq:job:"+jobID+":dependents")).Result()
	if err != nil || len(childIDs) == 0 {
		return err
	}
	for _, cid := range childIDs {
		if err := r.resolveChildDependency(ctx, cid, jobID); err != nil {
			return err
		}
	}
	return r.client.Del(ctx, r.k("runiq:job:"+jobID+":dependents")).Err()
}

func (r *RedisStorage) resolveChildDependency(ctx context.Context, childID, parentID string) error {
	rem, err := r.client.SRem(ctx, r.k("runiq:job:"+childID+":dependencies"), parentID).Result()
	if err != nil || rem == 0 {
		return err
	}
	count, err := r.client.SCard(ctx, r.k("runiq:job:"+childID+":dependencies")).Result()
	if err != nil || count > 0 {
		return err
	}
	return r.enqueueChildJob(ctx, childID)
}

func (r *RedisStorage) enqueueChildJob(ctx context.Context, childID string) error {
	val, err := r.client.HGet(ctx, r.k("runiq:jobs"), childID).Result()
	if err != nil {
		return err
	}
	var env JobEnvelope
	if err := json.Unmarshal([]byte(val), &env); err != nil {
		return err
	}
	pipe := r.client.Pipeline()
	pipe.Del(ctx, r.k("runiq:job:"+childID+":dependencies"))
	r.enqueueReadyJob(ctx, pipe, &env)
	_, err = pipe.Exec(ctx)
	return err
}

func (r *RedisStorage) cascadeDependencyFailure(ctx context.Context, parentID string) error {
	childIDs, err := r.client.SMembers(ctx, r.k("runiq:job:"+parentID+":dependents")).Result()
	if err != nil || len(childIDs) == 0 {
		return err
	}
	for _, cid := range childIDs {
		if err := r.failChildJob(ctx, cid, parentID); err != nil {
			return err
		}
	}
	return r.client.Del(ctx, r.k("runiq:job:"+parentID+":dependents")).Err()
}

func (r *RedisStorage) failChildJob(ctx context.Context, childID, parentID string) error {
	val, err := r.client.HGet(ctx, r.k("runiq:jobs"), childID).Result()
	if err != nil {
		return err
	}
	var env JobEnvelope
	if err := json.Unmarshal([]byte(val), &env); err != nil {
		return err
	}
	if err := r.execFailChildJob(ctx, &env, parentID); err != nil {
		return err
	}
	return r.cascadeDependencyFailure(ctx, childID)
}

func (r *RedisStorage) execFailChildJob(ctx context.Context, env *JobEnvelope, parentID string) error {
	pipe := r.client.Pipeline()
	pipe.LPush(ctx, r.k("runiq:dead:"+env.Queue), env.JobID)
	pipe.LTrim(ctx, r.k("runiq:dead:"+env.Queue), 0, 49)
	pipe.HSet(ctx, r.k("runiq:errors"), env.JobID, "Dependency "+parentID+" failed")
	pipe.ZAdd(ctx, r.k("runiq:dead_ttl"), redis.Z{
		Score:  float64(time.Now().Unix()),
		Member: env.Queue + ":" + env.JobID,
	})
	if env.UniqueKey != "" {
		pipe.Del(ctx, r.k("runiq:unique:"+env.Queue+":"+env.UniqueKey))
	}
	pipe.Del(ctx, r.k("runiq:job:"+env.JobID+":dependencies"))
	_, err := pipe.Exec(ctx)
	return err
}
