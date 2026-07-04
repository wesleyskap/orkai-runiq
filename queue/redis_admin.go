package queue

import (
	"context"
	"encoding/json"
	"time"

	"github.com/redis/go-redis/v9"
)

func (r *RedisStorage) handleBatchAck(ctx context.Context, batchID string) {
	expired, err := r.checkBatchExpired(ctx, batchID)
	if err != nil || expired {
		return
	}
	batchKey := r.k("runiq:batch:" + batchID)
	pending, err := r.client.HIncrBy(ctx, batchKey, "pending", -1).Result()
	if err != nil {
		return
	}
	r.completeBatchIfSealedAndDone(ctx, batchKey, pending)
}

func (r *RedisStorage) completeBatchIfSealedAndDone(ctx context.Context, batchKey string, pending int64) {
	status, err := r.client.HGet(ctx, batchKey, "status").Result()
	if err != nil || status != "sealed" || pending != 0 {
		return
	}
	_ = r.client.HSet(ctx, batchKey, "status", "completed").Err()
	r.enqueueBatchCallback(ctx, batchKey)
}

func (r *RedisStorage) enqueueBatchCallback(ctx context.Context, batchKey string) {
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
	data, err := r.client.HGet(ctx, r.k("runiq:jobs"), jobID).Result()
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
	if err := r.performAck(ctx, &env); err != nil {
		return err
	}
	return r.resolveDependencies(ctx, jobID)
}

func (r *RedisStorage) performAck(ctx context.Context, env *JobEnvelope) error {
	pipe := r.client.Pipeline()
	pipe.SRem(ctx, r.k("runiq:active:"+env.Queue), env.JobID)
	pipe.LPush(ctx, r.k("runiq:processed:"+env.Queue), env.JobID)
	pipe.LTrim(ctx, r.k("runiq:processed:"+env.Queue), 0, 49)
	pipe.ZAdd(ctx, r.k("runiq:processed_ttl"), redis.Z{Score: float64(time.Now().Unix()), Member: env.Queue + ":" + env.JobID})
	pipe.Incr(ctx, r.k("runiq:processed_count:"+env.Queue))
	if env.UniqueKey != "" {
		pipe.Del(ctx, r.k("runiq:unique:"+env.Queue+":"+env.UniqueKey))
	}
	_, err := pipe.Exec(ctx)
	if err != nil {
		return err
	}
	if env.BatchID != "" {
		r.handleBatchAck(ctx, env.BatchID)
	}
	return nil
}

func (r *RedisStorage) Fail(ctx context.Context, jobID string, runErr error) error {
	data, err := r.client.HGet(ctx, r.k("runiq:jobs"), jobID).Result()
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
	pipe := r.client.Pipeline()
	pipe.SRem(ctx, r.k("runiq:active:"+env.Queue), env.JobID)
	isDead := r.prepareFailStep(ctx, pipe, env, runErr)
	_, err := pipe.Exec(ctx)
	if err != nil {
		return err
	}
	if isDead {
		return r.cascadeDependencyFailure(ctx, env.JobID)
	}
	return nil
}

func (r *RedisStorage) prepareFailStep(ctx context.Context, pipe redis.Pipeliner, env *JobEnvelope, runErr error) bool {
	maxAttempts := env.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 3
	}
	if env.Attempts < maxAttempts {
		nextRun := time.Now().Add(ComputeBackoffDelay(env.Attempts - 1))
		env.RunAt = &nextRun
		r.rescheduleJob(ctx, pipe, env)
		return false
	}
	r.handleDeadJob(ctx, pipe, env, runErr)
	return true
}

func (r *RedisStorage) rescheduleJob(ctx context.Context, pipe redis.Pipeliner, env *JobEnvelope) {
	updatedData, _ := json.Marshal(env)
	pipe.HSet(ctx, r.k("runiq:jobs"), env.JobID, updatedData)
	pipe.ZAdd(ctx, r.k("runiq:scheduled:"+env.Queue), redis.Z{
		Score:  float64(env.RunAt.Unix()),
		Member: env.JobID,
	})
}

func (r *RedisStorage) handleDeadJob(ctx context.Context, pipe redis.Pipeliner, env *JobEnvelope, runErr error) {
	updatedData, _ := json.Marshal(env)
	pipe.HSet(ctx, r.k("runiq:jobs"), env.JobID, updatedData)
	pipe.LPush(ctx, r.k("runiq:dead:"+env.Queue), env.JobID)
	pipe.LTrim(ctx, r.k("runiq:dead:"+env.Queue), 0, 49)
	pipe.HSet(ctx, r.k("runiq:errors"), env.JobID, runErr.Error())
	pipe.ZAdd(ctx, r.k("runiq:dead_ttl"), redis.Z{
		Score:  float64(time.Now().Unix()),
		Member: env.Queue + ":" + env.JobID,
	})
	pipe.Incr(ctx, r.k("runiq:dead_count:"+env.Queue))
	if env.UniqueKey != "" {
		pipe.Del(ctx, r.k("runiq:unique:"+env.Queue+":"+env.UniqueKey))
	}
	if env.BatchID != "" {
		pipe.HSet(ctx, r.k("runiq:batch:"+env.BatchID), "status", "failed")
		pipe.ZRem(ctx, r.k("runiq:batches:expire"), env.BatchID)
	}
}

func (r *RedisStorage) Retry(ctx context.Context, jobID string) error {
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
	return r.executeRetryTx(ctx, &env)
}

func (r *RedisStorage) executeRetryTx(ctx context.Context, env *JobEnvelope) error {
	newVal, err := json.Marshal(env)
	if err != nil {
		return err
	}
	pipe := r.client.TxPipeline()
	pipe.HSet(ctx, r.k("runiq:jobs"), env.JobID, newVal)
	pipe.LRem(ctx, r.k("runiq:failed:"+env.Queue), 0, env.JobID)
	pipe.LRem(ctx, r.k("runiq:dead:"+env.Queue), 0, env.JobID)
	pipe.ZRem(ctx, r.k("runiq:dead_ttl"), env.Queue+":"+env.JobID)
	pipe.HDel(ctx, r.k("runiq:errors"), env.JobID)
	pipe.Decr(ctx, r.k("runiq:dead_count:"+env.Queue))
	score := float64(env.Priority) - float64(time.Now().Unix())/1e11
	pipe.ZAdd(ctx, r.k("runiq:queue:"+env.Queue), redis.Z{Score: score, Member: env.JobID})
	_, err = pipe.Exec(ctx)
	return err
}

func (r *RedisStorage) Cancel(ctx context.Context, jobID string) error {
	val, err := r.client.HGet(ctx, r.k("runiq:jobs"), jobID).Result()
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
	if err := r.executeCancelTx(ctx, &env); err != nil {
		return err
	}
	return r.cascadeDependencyFailure(ctx, jobID)
}

func (r *RedisStorage) executeCancelTx(ctx context.Context, env *JobEnvelope) error {
	pipe := r.client.TxPipeline()
	pipe.ZRem(ctx, r.k("runiq:queue:"+env.Queue), env.JobID)
	pipe.SRem(ctx, r.k("runiq:active:"+env.Queue), env.JobID)
	pipe.ZRem(ctx, r.k("runiq:scheduled:"+env.Queue), env.JobID)
	pipe.LRem(ctx, r.k("runiq:failed:"+env.Queue), 0, env.JobID)
	pipe.LRem(ctx, r.k("runiq:dead:"+env.Queue), 0, env.JobID)
	pipe.ZRem(ctx, r.k("runiq:dead_ttl"), env.Queue+":"+env.JobID)
	pipe.LRem(ctx, r.k("runiq:processed:"+env.Queue), 0, env.JobID)
	pipe.Decr(ctx, r.k("runiq:dead_count:"+env.Queue))
	pipe.HDel(ctx, r.k("runiq:jobs"), env.JobID)
	pipe.HDel(ctx, r.k("runiq:errors"), env.JobID)
	if env.UniqueKey != "" {
		pipe.Del(ctx, r.k("runiq:unique:"+env.Queue+":"+env.UniqueKey))
	}
	_, err := pipe.Exec(ctx)
	return err
}

func (r *RedisStorage) collectQueueIDs(ctx context.Context, queue string) ([]string, []string) {
	pIDs, _ := r.client.ZRange(ctx, r.k("runiq:queue:"+queue), 0, -1).Result()
	sIDs, _ := r.client.ZRange(ctx, r.k("runiq:scheduled:"+queue), 0, -1).Result()
	aIDs, _ := r.client.SMembers(ctx, r.k("runiq:active:"+queue)).Result()
	prIDs, _ := r.client.LRange(ctx, r.k("runiq:processed:"+queue), 0, -1).Result()
	fIDs, _ := r.client.LRange(ctx, r.k("runiq:failed:"+queue), 0, -1).Result()
	dIDs, _ := r.client.LRange(ctx, r.k("runiq:dead:"+queue), 0, -1).Result()

	var allJobIDs []string
	allJobIDs = append(allJobIDs, pIDs...)
	allJobIDs = append(allJobIDs, sIDs...)
	allJobIDs = append(allJobIDs, aIDs...)
	allJobIDs = append(allJobIDs, prIDs...)
	allJobIDs = append(allJobIDs, fIDs...)
	allJobIDs = append(allJobIDs, dIDs...)
	return allJobIDs, dIDs
}
