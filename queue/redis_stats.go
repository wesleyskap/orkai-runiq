package queue

import (
	"context"
	"encoding/json"
	"strconv"

	"github.com/redis/go-redis/v9"
)

type jobMeta struct {
	id     string
	queue  string
	status string
}

func (r *RedisStorage) GetStats(ctx context.Context) (*Stats, error) {
	queues, err := r.client.SMembers(ctx, r.k("runiq:queues")).Result()
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
	m := make(map[string]CronJobDetail)
	r.loadStaticCrons(ctx, m)
	r.loadDynamicCrons(ctx, m)
	stats.CronJobs = sortCronJobs(m)
	return nil
}

func (r *RedisStorage) loadStaticCrons(ctx context.Context, m map[string]CronJobDetail) {
	allCrons, err := r.client.HVals(ctx, r.k("runiq:cron_jobs")).Result()
	if err != nil {
		return
	}
	for _, data := range allCrons {
		var cd CronJobDetail
		if err := json.Unmarshal([]byte(data), &cd); err == nil {
			cd.Source = "static"
			m[cd.Name] = cd
		}
	}
}

func (r *RedisStorage) loadDynamicCrons(ctx context.Context, m map[string]CronJobDetail) {
	allCrons, err := r.client.HVals(ctx, r.k("runiq:cron_schedules")).Result()
	if err != nil {
		return
	}
	for _, data := range allCrons {
		var c CronJob
		if err := json.Unmarshal([]byte(data), &c); err == nil {
			m[c.Name] = CronJobDetail{
				Name: c.Name, Expression: c.Spec, Queue: c.Queue,
				Payload: string(c.Payload), Timezone: c.Timezone,
				Source: "dynamic", Paused: c.Paused,
			}
		}
	}
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
	paused, err := r.client.SMembers(ctx, r.k("runiq:paused_queues")).Result()
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
	pendingCmd := pipe.LLen(ctx, r.k("runiq:queue:"+q))
	scheduledCmd := pipe.ZCard(ctx, r.k("runiq:scheduled:"+q))
	activeCmd := pipe.SCard(ctx, r.k("runiq:active:"+q))
	processedCmd := pipe.LLen(ctx, r.k("runiq:processed:"+q))
	failedCmd := pipe.LLen(ctx, r.k("runiq:failed:"+q))
	deadCmd := pipe.LLen(ctx, r.k("runiq:dead:"+q))
	processedCountCmd := pipe.Get(ctx, r.k("runiq:processed_count:"+q))
	deadCountCmd := pipe.Get(ctx, r.k("runiq:dead_count:"+q))
	_, err := pipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		return nil, err
	}
	processedVal, _ := strconv.ParseInt(processedCountCmd.Val(), 10, 64)
	if processedVal <= 0 {
		processedVal = processedCmd.Val()
	}
	deadVal, _ := strconv.ParseInt(deadCountCmd.Val(), 10, 64)
	if deadVal <= 0 {
		deadVal = deadCmd.Val()
	}
	return &QueueStats{
		Name:      q,
		Pending:   pendingCmd.Val() + scheduledCmd.Val(),
		Running:   activeCmd.Val(),
		Processed: processedVal,
		Failed:    failedCmd.Val() + deadVal,
	}, nil
}

func (r *RedisStorage) accumulateQueueTotals(stats *Stats, qs *QueueStats) {
	stats.Pending += qs.Pending
	stats.Running += qs.Running
	stats.Processed += qs.Processed
	stats.Failed += qs.Failed
}

func (r *RedisStorage) collectRecentJobIDs(ctx context.Context, q string, allJobIDs *[]string, jobsMeta *[]jobMeta) {
	r.appendJobIDs(r.fetchList(ctx, r.k("runiq:queue:"+q)), q, "pending", allJobIDs, jobsMeta)
	r.appendJobIDs(r.fetchZSet(ctx, r.k("runiq:scheduled:"+q)), q, "pending", allJobIDs, jobsMeta)
	r.appendJobIDs(r.fetchSet(ctx, r.k("runiq:active:"+q)), q, "running", allJobIDs, jobsMeta)
	r.appendJobIDs(r.fetchList(ctx, r.k("runiq:processed:"+q)), q, "processed", allJobIDs, jobsMeta)
	r.appendJobIDs(r.fetchList(ctx, r.k("runiq:failed:"+q)), q, "failed", allJobIDs, jobsMeta)
	r.appendJobIDs(r.fetchList(ctx, r.k("runiq:dead:"+q)), q, "dead", allJobIDs, jobsMeta)
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
	envs, err := r.client.HMGet(ctx, r.k("runiq:jobs"), ids...).Result()
	if err != nil {
		return
	}
	errsMap, _ := r.client.HGetAll(ctx, r.k("runiq:errors")).Result()
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

