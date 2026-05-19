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
	_, err = pipe.Exec(ctx)
	return err
}

// Fail transitions the job from active set to failed list.
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
	pipe := r.client.Pipeline()
	pipe.SRem(ctx, "runiq:active:"+env.Queue, jobID)
	pipe.LPush(ctx, "runiq:failed:"+env.Queue, jobID)
	pipe.LTrim(ctx, "runiq:failed:"+env.Queue, 0, 49)
	pipe.HSet(ctx, "runiq:errors", jobID, runErr.Error())
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

		stats.Pending += pending
		stats.Running += active
		stats.Processed += processed
		stats.Failed += failed

		stats.Queues = append(stats.Queues, QueueStats{
			Name:      q,
			Pending:   pending,
			Running:   active,
			Processed: processed,
			Failed:    failed,
		})

		pIDs, _ := r.client.LRange(ctx, "runiq:queue:"+q, 0, 49).Result()
		for _, id := range pIDs {
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

	return &stats, nil
}
