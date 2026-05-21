package queue

import (
	"context"
	"encoding/json"

	"github.com/redis/go-redis/v9"
)

type jobMeta struct {
	id     string
	queue  string
	status string
}

func (r *RedisStorage) GetStats(ctx context.Context) (*Stats, error) {
	queues, err := r.client.SMembers(ctx, "runiq:queues").Result()
	if err != nil {
		return nil, err
	}
	var stats Stats
	var allJobIDs []string
	var jobsMeta []jobMeta
	if err := r.loadQueuesStats(ctx, &stats, queues, &allJobIDs, &jobsMeta); err != nil {
		return nil, err
	}
	r.loadRecentJobs(ctx, &stats, allJobIDs, jobsMeta)
	r.loadActiveProcesses(ctx, &stats)
	_ = r.loadCronJobs(ctx, &stats)
	return &stats, nil
}

func (r *RedisStorage) loadCronJobs(ctx context.Context, stats *Stats) error {
	allCrons, err := r.client.HVals(ctx, "runiq:cron_jobs").Result()
	if err != nil && err != redis.Nil {
		return err
	}
	for _, data := range allCrons {
		var cd CronJobDetail
		if err := json.Unmarshal([]byte(data), &cd); err == nil {
			stats.CronJobs = append(stats.CronJobs, cd)
		}
	}
	return nil
}

func (r *RedisStorage) loadQueuesStats(ctx context.Context, stats *Stats, queues []string, allJobIDs *[]string, jobsMeta *[]jobMeta) error {
	pausedMap, err := r.getPausedQueuesMap(ctx)
	if err != nil {
		return err
	}
	return r.processAllQueues(ctx, stats, queues, pausedMap, allJobIDs, jobsMeta)
}

func (r *RedisStorage) processAllQueues(ctx context.Context, stats *Stats, queues []string, pausedMap map[string]bool, allJobIDs *[]string, jobsMeta *[]jobMeta) error {
	var err error
	i := 0
	for err == nil && i < len(queues) {
		err = r.processQueue(ctx, queues[i], stats, pausedMap, allJobIDs, jobsMeta)
		i++
	}
	return err
}

func (r *RedisStorage) processQueue(ctx context.Context, q string, stats *Stats, pausedMap map[string]bool, allJobIDs *[]string, jobsMeta *[]jobMeta) error {
	qs, err := r.getQueueStats(ctx, q)
	if err != nil {
		return err
	}
	qs.Paused = pausedMap[q]
	stats.Queues = append(stats.Queues, *qs)
	r.accumulateQueueTotals(stats, qs)
	r.collectRecentJobIDs(ctx, q, allJobIDs, jobsMeta)
	return nil
}

func (r *RedisStorage) getPausedQueuesMap(ctx context.Context) (map[string]bool, error) {
	paused, err := r.client.SMembers(ctx, "runiq:paused_queues").Result()
	if err != nil {
		return nil, err
	}
	m := make(map[string]bool)
	for _, q := range paused {
		m[q] = true
	}
	return m, nil
}

func (r *RedisStorage) getQueueStats(ctx context.Context, q string) (*QueueStats, error) {
	pipe := r.client.Pipeline()
	pendingCmd := pipe.LLen(ctx, "runiq:queue:"+q)
	scheduledCmd := pipe.ZCard(ctx, "runiq:scheduled:"+q)
	activeCmd := pipe.SCard(ctx, "runiq:active:"+q)
	processedCmd := pipe.LLen(ctx, "runiq:processed:"+q)
	failedCmd := pipe.LLen(ctx, "runiq:failed:"+q)
	deadCmd := pipe.LLen(ctx, "runiq:dead:"+q)
	_, err := pipe.Exec(ctx)
	if err != nil {
		return nil, err
	}
	return &QueueStats{
		Name:      q,
		Pending:   pendingCmd.Val() + scheduledCmd.Val(),
		Running:   activeCmd.Val(),
		Processed: processedCmd.Val(),
		Failed:    failedCmd.Val() + deadCmd.Val(),
	}, nil
}

func (r *RedisStorage) accumulateQueueTotals(stats *Stats, qs *QueueStats) {
	stats.Pending += qs.Pending
	stats.Running += qs.Running
	stats.Processed += qs.Processed
	stats.Failed += qs.Failed
}

func (r *RedisStorage) collectRecentJobIDs(ctx context.Context, q string, allJobIDs *[]string, jobsMeta *[]jobMeta) {
	r.appendJobIDs(r.fetchList(ctx, "runiq:queue:"+q), q, "pending", allJobIDs, jobsMeta)
	r.appendJobIDs(r.fetchZSet(ctx, "runiq:scheduled:"+q), q, "pending", allJobIDs, jobsMeta)
	r.appendJobIDs(r.fetchSet(ctx, "runiq:active:"+q), q, "running", allJobIDs, jobsMeta)
	r.appendJobIDs(r.fetchList(ctx, "runiq:processed:"+q), q, "processed", allJobIDs, jobsMeta)
	r.appendJobIDs(r.fetchList(ctx, "runiq:failed:"+q), q, "failed", allJobIDs, jobsMeta)
	r.appendJobIDs(r.fetchList(ctx, "runiq:dead:"+q), q, "dead", allJobIDs, jobsMeta)
}

func (r *RedisStorage) fetchList(ctx context.Context, key string) []string {
	res, _ := r.client.LRange(ctx, key, 0, 49).Result()
	return res
}

func (r *RedisStorage) fetchZSet(ctx context.Context, key string) []string {
	res, _ := r.client.ZRange(ctx, key, 0, 49).Result()
	return res
}

func (r *RedisStorage) fetchSet(ctx context.Context, key string) []string {
	res, _ := r.client.SMembers(ctx, key).Result()
	return res
}

func (r *RedisStorage) appendJobIDs(ids []string, q, status string, allJobIDs *[]string, jobsMeta *[]jobMeta) {
	for _, id := range ids {
		*allJobIDs = append(*allJobIDs, id)
		*jobsMeta = append(*jobsMeta, jobMeta{id: id, queue: q, status: status})
	}
}

func (r *RedisStorage) loadRecentJobs(ctx context.Context, stats *Stats, ids []string, meta []jobMeta) {
	if len(ids) == 0 {
		return
	}
	envs, err := r.client.HMGet(ctx, "runiq:jobs", ids...).Result()
	if err != nil {
		return
	}
	errsMap, _ := r.client.HGetAll(ctx, "runiq:errors").Result()
	r.parseEnvelopes(envs, meta, errsMap, stats)
}

func (r *RedisStorage) parseEnvelopes(envs []interface{}, meta []jobMeta, errsMap map[string]string, stats *Stats) {
	for i, val := range envs {
		if val != nil {
			r.parseSingleEnvelope(val, meta[i], errsMap, stats)
		}
	}
}

func (r *RedisStorage) parseSingleEnvelope(val interface{}, m jobMeta, errsMap map[string]string, stats *Stats) {
	strVal, ok := val.(string)
	if !ok {
		return
	}
	var env JobEnvelope
	if err := json.Unmarshal([]byte(strVal), &env); err != nil {
		return
	}
	stats.Jobs = append(stats.Jobs, JobDetail{
		JobID:        env.JobID,
		Queue:        env.Queue,
		Name:         env.Name,
		Status:       m.status,
		TraceID:      env.TraceContext.TraceID,
		ErrorMessage: errsMap[env.JobID],
	})
}

func (r *RedisStorage) loadActiveProcesses(ctx context.Context, stats *Stats) {
	active, err := r.GetActiveProcesses(ctx)
	if err == nil {
		stats.Processes = active
	}
}

func (r *RedisStorage) sliceToInterfaces(slice []string) []interface{} {
	result := make([]interface{}, len(slice))
	for i, v := range slice {
		result[i] = v
	}
	return result
}
