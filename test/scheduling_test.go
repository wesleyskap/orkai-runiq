package test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "github.com/glebarez/go-sqlite"
	"github.com/redis/go-redis/v9"
	"github.com/wesleyskap/orkai-runiq/v2/queue"
)

func TestEnqueueWithDelay(t *testing.T) {
	db, store := setupSqlite(t)
	defer db.Close()
	client := queue.NewClient(store)
	ctx := context.Background()
	err := client.EnqueueWithDelay(ctx, "delay-queue", "DelayedJob", []byte("{}"), 10*time.Second)
	if err != nil {
		t.Fatalf("failed to enqueue with delay: %v", err)
	}
	assertJobDelay(t, db)
}

func setupSqlite(t *testing.T) (*sql.DB, *queue.SqliteStorage) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("failed to open sqlite: %v", err)
	}
	db.SetMaxOpenConns(1)
	store, err := queue.NewSqliteStorage(db)
	if err != nil {
		t.Fatalf("failed to create sqlite storage: %v", err)
	}
	return db, store
}

func assertJobDelay(t *testing.T, db *sql.DB) {
	var runAt time.Time
	err := db.QueryRow("SELECT run_at FROM runiq_jobs WHERE name = 'DelayedJob'").Scan(&runAt)
	if err != nil {
		t.Fatalf("failed to find job in db: %v", err)
	}
	diff := time.Until(runAt)
	if diff < 8*time.Second || diff > 12*time.Second {
		t.Errorf("expected run_at to be around 10s, got diff %v", diff)
	}
}

func TestBatchTimeoutSqlite(t *testing.T) {
	db, store := setupSqlite(t)
	defer db.Close()
	ctx := context.Background()
	client := queue.NewClient(store)
	callback := &queue.JobEnvelope{Queue: "batch-timeout-sqlite-queue", Name: "CallbackJob", Args: []byte("{}")}
	batch, err := client.NewBatch(ctx, callback, queue.WithBatchTimeout(50*time.Millisecond))
	if err != nil {
		t.Fatalf("failed to create batch: %v", err)
	}
	if err := batch.Enqueue(ctx, "batch-timeout-sqlite-queue", "Job1", []byte("{}")); err != nil {
		t.Fatalf("failed to enqueue job: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	verifySqliteBatchFails(t, ctx, store, db, batch)
}

func verifySqliteBatchFails(t *testing.T, ctx context.Context, store *queue.SqliteStorage, db *sql.DB, batch *queue.Batch) {
	if err := store.FailExpiredBatches(ctx); err != nil {
		t.Fatalf("failed to run FailExpiredBatches: %v", err)
	}
	var status string
	err := db.QueryRow("SELECT status FROM runiq_batches WHERE batch_id = ?", batch.ID).Scan(&status)
	if err != nil {
		t.Fatalf("failed to query batch status: %v", err)
	}
	if status != "failed" {
		t.Errorf("expected batch status 'failed', got '%s'", status)
	}
	if err := batch.Submit(ctx); err != nil {
		t.Fatalf("failed to submit batch: %v", err)
	}
	assertNoCallbackJob(t, db)
}

func assertNoCallbackJob(t *testing.T, db *sql.DB) {
	var count int
	err := db.QueryRow("SELECT COUNT(*) FROM runiq_jobs WHERE name = 'CallbackJob'").Scan(&count)
	if err != nil {
		t.Fatalf("failed to query callback count: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 callback jobs, got %d", count)
	}
}

func TestCronTimezoneSqlite(t *testing.T) {
	db, store := setupSqlite(t)
	defer db.Close()
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skip("skipping, unable to load America/New_York timezone")
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pool := queue.NewWorkerPool(store, 1)
	pool.RegisterCron("* * * * *", "cron-tz-sqlite-queue", "CronNY", []byte("{}"), queue.WithCronLocation(loc))
	go func() { _ = pool.Start(ctx, "cron-tz-sqlite-queue") }()
	time.Sleep(100 * time.Millisecond)
	verifyCronTimezoneInStats(t, ctx, store, "CronNY", "America/New_York")
}

func verifyCronTimezoneInStats(t *testing.T, ctx context.Context, store queue.JobStats, name, expectedTz string) {
	stats, err := store.GetStats(ctx)
	if err != nil {
		t.Fatalf("failed to get stats: %v", err)
	}
	found := false
	for _, cron := range stats.CronJobs {
		if cron.Name == name {
			found = true
			if cron.Timezone != expectedTz {
				t.Errorf("expected timezone '%s', got '%s'", expectedTz, cron.Timezone)
			}
		}
	}
	if !found {
		t.Errorf("expected cron job '%s' to be registered", name)
	}
}

func TestBatchTimeoutRedis(t *testing.T) {
	client, store := setupRedis(t)
	defer client.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	qClient := queue.NewClient(store)
	callback := &queue.JobEnvelope{Queue: "batch-timeout-redis-queue", Name: "CallbackJob", Args: []byte("{}")}
	batch, err := qClient.NewBatch(ctx, callback, queue.WithBatchTimeout(50*time.Millisecond))
	if err != nil {
		t.Fatalf("failed to create batch: %v", err)
	}
	if err := batch.Enqueue(ctx, "batch-timeout-redis-queue", "Job1", []byte("{}")); err != nil {
		t.Fatalf("failed to enqueue: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	verifyRedisBatchFails(t, ctx, store, client, batch)
}

func setupRedis(t *testing.T) (*redis.Client, *queue.RedisStorage) {
	client := redis.NewClient(&redis.Options{Addr: redisAddress})
	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skip("skipping redis tests, service unreachable")
	}
	store, err := queue.NewRedisStorage(client)
	if err != nil {
		t.Fatalf("failed to create redis storage: %v", err)
	}
	client.FlushAll(ctx)
	return client, store
}

func verifyRedisBatchFails(t *testing.T, ctx context.Context, store *queue.RedisStorage, client *redis.Client, batch *queue.Batch) {
	if err := store.FailExpiredBatches(ctx); err != nil {
		t.Fatalf("failed to run FailExpiredBatches: %v", err)
	}
	status, err := client.HGet(ctx, "runiq:batch:"+batch.ID, "status").Result()
	if err != nil {
		t.Fatalf("failed to get status: %v", err)
	}
	if status != "failed" {
		t.Errorf("expected batch status 'failed', got '%s'", status)
	}
	if err := batch.Submit(ctx); err != nil {
		t.Fatalf("failed to submit: %v", err)
	}
	assertRedisQueueLength(t, ctx, client, 1)
}

func assertRedisQueueLength(t *testing.T, ctx context.Context, client *redis.Client, expected int) {
	length, err := client.LLen(ctx, "runiq:queue:batch-timeout-redis-queue").Result()
	if err != nil {
		t.Fatalf("failed to get queue length: %v", err)
	}
	if length != int64(expected) {
		t.Errorf("expected queue length %d, got %d", expected, length)
	}
}

func TestCronTimezoneRedis(t *testing.T) {
	client, store := setupRedis(t)
	defer client.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	loc, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skip("skipping, timezone America/New_York not loaded")
		return
	}
	pool := queue.NewWorkerPool(store, 1)
	pool.RegisterCron("* * * * *", "cron-tz-redis-queue", "CronNY", []byte("{}"), queue.WithCronLocation(loc))
	go func() { _ = pool.Start(ctx, "cron-tz-redis-queue") }()
	time.Sleep(100 * time.Millisecond)
	verifyCronTimezoneInStats(t, ctx, store, "CronNY", "America/New_York")
}
