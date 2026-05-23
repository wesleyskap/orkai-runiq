package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

func (r *RedisStorage) RegisterProcess(ctx context.Context, info *ProcessInfo) error {
	data, err := json.Marshal(info)
	if err != nil {
		return err
	}
	pipe := r.client.Pipeline()
	pipe.HSet(ctx, r.k("runiq:processes"), info.ProcessID, data)
	pipe.ZAdd(ctx, r.k("runiq:processes:heartbeat"), redis.Z{
		Score:  float64(info.HeartbeatAt.Unix()),
		Member: info.ProcessID,
	})
	_, err = pipe.Exec(ctx)
	return err
}

func (r *RedisStorage) HeartbeatProcess(ctx context.Context, processID string) error {
	now := time.Now()
	err := r.client.ZAdd(ctx, r.k("runiq:processes:heartbeat"), redis.Z{Score: float64(now.Unix()), Member: processID}).Err()
	if err != nil {
		return err
	}
	info, err := r.getProcessInfo(ctx, processID)
	if err != nil || info == nil {
		return err
	}
	info.HeartbeatAt = now
	updated, err := json.Marshal(info)
	if err != nil {
		return err
	}
	return r.client.HSet(ctx, r.k("runiq:processes"), processID, updated).Err()
}

func (r *RedisStorage) getProcessInfo(ctx context.Context, processID string) (*ProcessInfo, error) {
	data, err := r.client.HGet(ctx, r.k("runiq:processes"), processID).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var info ProcessInfo
	err = json.Unmarshal([]byte(data), &info)
	return &info, err
}

func (r *RedisStorage) GetActiveProcesses(ctx context.Context) ([]ProcessInfo, error) {
	if err := r.cleanupDeadProcesses(ctx); err != nil {
		return nil, err
	}
	activeIDs, err := r.client.ZRange(ctx, r.k("runiq:processes:heartbeat"), 0, -1).Result()
	if err != nil || len(activeIDs) == 0 {
		return nil, err
	}
	raws, err := r.client.HMGet(ctx, r.k("runiq:processes"), activeIDs...).Result()
	if err != nil {
		return nil, err
	}
	leaderID, _ := r.client.Get(ctx, r.k("runiq:leader")).Result()
	return r.parseActiveProcesses(raws, leaderID), nil
}

func (r *RedisStorage) cleanupDeadProcesses(ctx context.Context) error {
	limit := time.Now().Add(-15 * time.Second).Unix()
	dead, err := r.client.ZRangeByScore(ctx, r.k("runiq:processes:heartbeat"), &redis.ZRangeBy{
		Min: "-inf",
		Max: fmt.Sprintf("%d", limit),
	}).Result()
	if err == nil && len(dead) > 0 {
		pipe := r.client.Pipeline()
		pipe.ZRem(ctx, r.k("runiq:processes:heartbeat"), r.sliceToInterfaces(dead)...)
		pipe.HDel(ctx, r.k("runiq:processes"), dead...)
		_, err = pipe.Exec(ctx)
	}
	return err
}

func (r *RedisStorage) parseActiveProcesses(raws []interface{}, leaderID string) []ProcessInfo {
	var list []ProcessInfo
	for _, raw := range raws {
		if raw == nil {
			continue
		}
		str, ok := raw.(string)
		if !ok {
			continue
		}
		var info ProcessInfo
		if err := json.Unmarshal([]byte(str), &info); err == nil {
			info.IsLeader = (info.ProcessID == leaderID)
			list = append(list, info)
		}
	}
	return list
}

