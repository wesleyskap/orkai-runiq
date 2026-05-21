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
	pipe.HSet(ctx, "runiq:processes", info.ProcessID, data)
	pipe.ZAdd(ctx, "runiq:processes:heartbeat", redis.Z{
		Score:  float64(info.HeartbeatAt.Unix()),
		Member: info.ProcessID,
	})
	_, err = pipe.Exec(ctx)
	return err
}

func (r *RedisStorage) HeartbeatProcess(ctx context.Context, processID string) error {
	now := time.Now()
	err := r.client.ZAdd(ctx, "runiq:processes:heartbeat", redis.Z{
		Score:  float64(now.Unix()),
		Member: processID,
	}).Err()
	if err != nil {
		return err
	}

	data, err := r.client.HGet(ctx, "runiq:processes", processID).Result()
	if err == redis.Nil {
		return nil
	}
	if err != nil {
		return err
	}
	var info ProcessInfo
	if err := json.Unmarshal([]byte(data), &info); err != nil {
		return err
	}
	info.HeartbeatAt = now
	updatedData, err := json.Marshal(&info)
	if err != nil {
		return err
	}
	return r.client.HSet(ctx, "runiq:processes", processID, updatedData).Err()
}

func (r *RedisStorage) GetActiveProcesses(ctx context.Context) ([]ProcessInfo, error) {
	now := time.Now()
	deadTimeLimit := now.Add(-15 * time.Second).Unix()

	deadIDs, err := r.client.ZRangeByScore(ctx, "runiq:processes:heartbeat", &redis.ZRangeBy{
		Min: "-inf",
		Max: fmt.Sprintf("%d", deadTimeLimit),
	}).Result()
	if err == nil && len(deadIDs) > 0 {
		pipe := r.client.Pipeline()
		pipe.ZRem(ctx, "runiq:processes:heartbeat", r.sliceToInterfaces(deadIDs)...)
		pipe.HDel(ctx, "runiq:processes", deadIDs...)
		_, _ = pipe.Exec(ctx)
	}

	activeIDs, err := r.client.ZRange(ctx, "runiq:processes:heartbeat", 0, -1).Result()
	if err != nil {
		return nil, err
	}
	if len(activeIDs) == 0 {
		return nil, nil
	}

	dataSlice, err := r.client.HMGet(ctx, "runiq:processes", activeIDs...).Result()
	if err != nil {
		return nil, err
	}

	var activeProcesses []ProcessInfo
	for _, raw := range dataSlice {
		if raw == nil {
			continue
		}
		strVal, ok := raw.(string)
		if !ok {
			continue
		}
		var info ProcessInfo
		if err := json.Unmarshal([]byte(strVal), &info); err == nil {
			activeProcesses = append(activeProcesses, info)
		}
	}
	return activeProcesses, nil
}
