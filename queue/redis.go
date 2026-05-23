package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisStorage implements Storage interface using Redis.
type RedisStorage struct {
	client *redis.Client
	prefix string
}

func init() {
	RegisterStorageDriver("redis", func(conn interface{}) (interface{}, error) {
		client, ok := conn.(*redis.Client)
		if !ok {
			return nil, fmt.Errorf("redis driver requires *redis.Client connection")
		}
		return NewRedisStorage(client)
	})
}

// NewRedisStorage instantiates a new Redis storage engine.
func NewRedisStorage(client *redis.Client) (*RedisStorage, error) {
	storage := &RedisStorage{client: client, prefix: "runiq"}
	return storage, nil
}

func (r *RedisStorage) SetNamespace(ns string) {
	if ns == "" {
		r.prefix = "runiq"
	} else {
		r.prefix = ns
	}
}

func (r *RedisStorage) k(key string) string {
	if r.prefix == "" || r.prefix == "runiq" {
		return key
	}
	return strings.Replace(key, "runiq:", r.prefix+":", 1)
}

func (r *RedisStorage) acquireUniqueLock(ctx context.Context, env *JobEnvelope) error {
	if env.UniqueKey == "" {
		return nil
	}
	lockKey := r.k("runiq:unique:" + env.Queue + ":" + env.UniqueKey)
	ttl := env.UniqueTTL
	if ttl <= 0 {
		ttl = 24 * time.Hour
	}
	ok, err := r.client.SetNX(ctx, lockKey, env.JobID, ttl).Result()
	if err != nil || ok {
		return err
	}
	if err := r.checkLockDuplicate(ctx, lockKey); err != nil {
		return err
	}
	return r.client.Set(ctx, lockKey, env.JobID, ttl).Err()
}

func (r *RedisStorage) checkLockDuplicate(ctx context.Context, lockKey string) error {
	existingJobID, err := r.client.Get(ctx, lockKey).Result()
	if err == nil && existingJobID != "" {
		exists, err := r.client.HExists(ctx, r.k("runiq:jobs"), existingJobID).Result()
		if err == nil && exists {
			return fmt.Errorf("%w: key=%q, existing=%q", ErrDuplicateJob, lockKey, existingJobID)
		}
	}
	return nil
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
	return r.execEnqueue(ctx, env, data)
}

func (r *RedisStorage) execEnqueue(ctx context.Context, env *JobEnvelope, data []byte) error {
	pipe := r.client.Pipeline()
	pipe.HSet(ctx, r.k("runiq:jobs"), env.JobID, data)
	pipe.SAdd(ctx, r.k("runiq:queues"), env.Queue)
	if env.RunAt != nil && env.RunAt.After(time.Now()) {
		pipe.ZAdd(ctx, r.k("runiq:scheduled:"+env.Queue), redis.Z{Score: float64(env.RunAt.Unix()), Member: env.JobID})
	} else {
		pipe.LPush(ctx, r.k("runiq:queue:"+env.Queue), env.JobID)
	}
	_, err := pipe.Exec(ctx)
	return err
}

// Dequeue pops the next job ID from the queue list and retrieves its details from the Redis hash.
func (r *RedisStorage) Dequeue(ctx context.Context, queueName string) (*JobEnvelope, error) {
	jobID, err := r.client.RPop(ctx, r.k("runiq:queue:"+queueName)).Result()
	if err != nil {
		return nil, checkRedisNil(err)
	}
	env, err := r.getJobEnvelope(ctx, jobID)
	if err != nil {
		return nil, err
	}
	if err := r.client.SAdd(ctx, r.k("runiq:active:"+queueName), jobID).Err(); err != nil {
		return nil, err
	}
	return env, nil
}

func checkRedisNil(err error) error {
	if err == redis.Nil {
		return nil
	}
	return err
}

func (r *RedisStorage) getJobEnvelope(ctx context.Context, jobID string) (*JobEnvelope, error) {
	data, err := r.client.HGet(ctx, r.k("runiq:jobs"), jobID).Result()
	if err != nil {
		return nil, checkRedisNil(err)
	}
	var env JobEnvelope
	err = json.Unmarshal([]byte(data), &env)
	return &env, err
}

// PollScheduled atomically moves scheduled jobs that are due from ZSet to list.
func (r *RedisStorage) PollScheduled(ctx context.Context, queueName string) error {
	now := time.Now().Unix()
	jobIDs, err := r.client.ZRangeByScore(ctx, r.k("runiq:scheduled:"+queueName), &redis.ZRangeBy{
		Min: "-inf",
		Max: fmt.Sprintf("%d", now),
	}).Result()
	if err != nil || len(jobIDs) == 0 {
		return err
	}
	pipe := r.client.Pipeline()
	for _, id := range jobIDs {
		pipe.ZRem(ctx, r.k("runiq:scheduled:"+queueName), id)
		pipe.LPush(ctx, r.k("runiq:queue:"+queueName), id)
	}
	_, err = pipe.Exec(ctx)
	return err
}

func (r *RedisStorage) AcquireLeader(ctx context.Context, clientID string, ttl time.Duration) (bool, error) {
	script := `
          local val = redis.call('get', KEYS[1])
          if not val or val == ARGV[1] then
              redis.call('set', KEYS[1], ARGV[1], 'PX', ARGV[2])
              return 1
          else
              return 0
          end
      `
	res, err := r.client.Eval(ctx, script, []string{r.k("runiq:leader")}, clientID, int64(ttl/time.Millisecond)).Int64()
	return res == 1, err
}

func (r *RedisStorage) ReleaseLeader(ctx context.Context, clientID string) error {
	script := `
          if redis.call('get', KEYS[1]) == ARGV[1] then
              return redis.call('del', KEYS[1])
          else
              return 0
          end`
	return r.client.Eval(ctx, script, []string{r.k("runiq:leader")}, clientID).Err()
}

func (r *RedisStorage) ArchiveJobs(ctx context.Context, age time.Duration) (int64, error) {
	cutoff := time.Now().Add(-age).Unix()
	expired, err := r.getExpiredJobs(ctx, cutoff)
	if err != nil {
		return 0, err
	}
	count, err := r.archiveExpiredJobs(ctx, expired)
	return count, err
}

func (r *RedisStorage) getExpiredJobs(ctx context.Context, cutoff int64) ([]string, error) {
	proc, err := r.client.ZRangeByScore(ctx, r.k("runiq:processed_ttl"), &redis.ZRangeBy{
		Min: "-inf",
		Max: fmt.Sprintf("%d", cutoff),
	}).Result()
	if err != nil {
		return nil, err
	}
	dead, err := r.client.ZRangeByScore(ctx, r.k("runiq:dead_ttl"), &redis.ZRangeBy{
		Min: "-inf",
		Max: fmt.Sprintf("%d", cutoff),
	}).Result()
	return append(proc, dead...), err
}

func (r *RedisStorage) archiveExpiredJobs(ctx context.Context, expired []string) (int64, error) {
	var count int64
	for _, member := range expired {
		ok, err := r.archiveSingleJob(ctx, member)
		if err != nil {
			return count, err
		}
		if ok {
			count++
		}
	}
	return count, nil
}

func (r *RedisStorage) archiveSingleJob(ctx context.Context, member string) (bool, error) {
	parts := strings.SplitN(member, ":", 2)
	if len(parts) != 2 {
		return false, nil
	}
	queue, jobID := parts[0], parts[1]
	data, err := r.client.HGet(ctx, r.k("runiq:jobs"), jobID).Result()
	if err == nil {
		_ = r.client.HSet(ctx, r.k("runiq:archived_jobs"), jobID, data).Err()
	}
	pipe := r.client.Pipeline()
	pipe.HDel(ctx, r.k("runiq:jobs"), jobID)
	pipe.HDel(ctx, r.k("runiq:errors"), jobID)
	pipe.LRem(ctx, r.k("runiq:processed:"+queue), 0, jobID)
	pipe.LRem(ctx, r.k("runiq:dead:"+queue), 0, jobID)
	pipe.ZRem(ctx, r.k("runiq:processed_ttl"), member)
	pipe.ZRem(ctx, r.k("runiq:dead_ttl"), member)
	_, err = pipe.Exec(ctx)
	return err == nil && data != "", err
}

