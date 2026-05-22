package queue

import (
	"context"
	"encoding/json"
	"strings"
)

func (r *RedisStorage) collectJobIDsForStatus(ctx context.Context, status string, queues []string) []jobMeta {
	var metas []jobMeta
	for _, q := range queues {
		if status == "" || status == "pending" {
			metas = append(metas, r.fetchMeta(ctx, "runiq:queue:"+q, q, "pending")...)
			metas = append(metas, r.fetchZSetMeta(ctx, "runiq:scheduled:"+q, q, "pending")...)
		}
		if status == "" || status == "running" {
			metas = append(metas, r.fetchSetMeta(ctx, "runiq:active:"+q, q, "running")...)
		}
		if status == "" || status == "processed" {
			metas = append(metas, r.fetchMeta(ctx, "runiq:processed:"+q, q, "processed")...)
		}
		if status == "" || status == "failed" {
			metas = append(metas, r.fetchMeta(ctx, "runiq:failed:"+q, q, "failed")...)
		}
		if status == "" || status == "dead" {
			metas = append(metas, r.fetchMeta(ctx, "runiq:dead:"+q, q, "dead")...)
		}
	}
	return metas
}

func (r *RedisStorage) fetchMeta(ctx context.Context, key, q, status string) []jobMeta {
	ids, _ := r.client.LRange(ctx, key, 0, -1).Result()
	var metas []jobMeta
	for _, id := range ids {
		metas = append(metas, jobMeta{id: id, queue: q, status: status})
	}
	return metas
}

func (r *RedisStorage) fetchZSetMeta(ctx context.Context, key, q, status string) []jobMeta {
	ids, _ := r.client.ZRange(ctx, key, 0, -1).Result()
	var metas []jobMeta
	for _, id := range ids {
		metas = append(metas, jobMeta{id: id, queue: q, status: status})
	}
	return metas
}

func (r *RedisStorage) fetchSetMeta(ctx context.Context, key, q, status string) []jobMeta {
	ids, _ := r.client.SMembers(ctx, key).Result()
	var metas []jobMeta
	for _, id := range ids {
		metas = append(metas, jobMeta{id: id, queue: q, status: status})
	}
	return metas
}

func matchQuery(env *JobEnvelope, q string) bool {
	if q == "" {
		return true
	}
	q = strings.ToLower(q)
	idMatch := strings.Contains(strings.ToLower(env.JobID), q)
	nameMatch := strings.Contains(strings.ToLower(env.Name), q)
	traceMatch := strings.Contains(strings.ToLower(env.TraceContext.TraceID), q)
	return idMatch || nameMatch || traceMatch
}

func filterJobs(envs []interface{}, metas []jobMeta, q string, errsMap map[string]string) []JobDetail {
	var details []JobDetail
	for i, val := range envs {
		if val == nil {
			continue
		}
		var env JobEnvelope
		_ = json.Unmarshal([]byte(val.(string)), &env)
		if matchQuery(&env, q) {
			details = append(details, JobDetail{
				JobID:        env.JobID,
				Queue:        env.Queue,
				Name:         env.Name,
				Status:       metas[i].status,
				TraceID:      env.TraceContext.TraceID,
				ErrorMessage: errsMap[env.JobID],
			})
		}
	}
	return details
}

func paginateJobs(jobs []JobDetail, page, limit int) ([]JobDetail, int64) {
	total := int64(len(jobs))
	limit, page = sanitizePageLimit(limit, page)
	start := (page - 1) * limit
	if start >= len(jobs) {
		return []JobDetail{}, total
	}
	end := start + limit
	if end > len(jobs) {
		end = len(jobs)
	}
	return jobs[start:end], total
}

func getMetasIDs(metas []jobMeta) []string {
	var ids []string
	for _, m := range metas {
		ids = append(ids, m.id)
	}
	return ids
}

// GetJobs queries jobs with pagination and filters for Redis.
func (r *RedisStorage) GetJobs(ctx context.Context, q, status string, page, limit int) ([]JobDetail, int64, error) {
	queues, err := r.client.SMembers(ctx, "runiq:queues").Result()
	if err != nil {
		return nil, 0, err
	}
	metas := r.collectJobIDsForStatus(ctx, status, queues)
	if len(metas) == 0 {
		return []JobDetail{}, 0, nil
	}
	envs, err := r.client.HMGet(ctx, "runiq:jobs", getMetasIDs(metas)...).Result()
	if err != nil {
		return nil, 0, err
	}
	errsMap, _ := r.client.HGetAll(ctx, "runiq:errors").Result()
	filtered := filterJobs(envs, metas, q, errsMap)
	paginated, total := paginateJobs(filtered, page, limit)
	return paginated, total, nil
}

// BulkRetry retries multiple jobs in Redis.
func (r *RedisStorage) BulkRetry(ctx context.Context, jobIDs []string) error {
	if len(jobIDs) == 0 {
		return nil
	}
	for _, id := range jobIDs {
		if err := r.Retry(ctx, id); err != nil {
			return err
		}
	}
	return nil
}

// BulkCancel cancels multiple jobs in Redis.
func (r *RedisStorage) BulkCancel(ctx context.Context, jobIDs []string) error {
	if len(jobIDs) == 0 {
		return nil
	}
	for _, id := range jobIDs {
		if err := r.Cancel(ctx, id); err != nil {
			return err
		}
	}
	return nil
}

// BulkPurge deletes multiple jobs in Redis.
func (r *RedisStorage) BulkPurge(ctx context.Context, jobIDs []string) error {
	if len(jobIDs) == 0 {
		return nil
	}
	for _, id := range jobIDs {
		if err := r.Cancel(ctx, id); err != nil {
			return err
		}
	}
	return nil
}
