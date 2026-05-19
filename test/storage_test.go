package test

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
	"github.com/wesleyskap/orkai-runiq/queue"
)

const (
	postgresConnStr = "postgres://admin:adminpassword@localhost:5433/condolivre_db?sslmode=disable"
	redisAddress     = "localhost:6379"
)

// TestPostgresStorageFlow runs a sequence of tests asserting correct PostgreSQL storage driver behavior.
// Usage example:
//
//	go test -v ./test/...
func TestPostgresStorageFlow(t *testing.T) {
	db, err := sql.Open("postgres", postgresConnStr)
	if err != nil {
		t.Skipf("skipping postgres storage tests, connection failed: %v", err)
		return
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		t.Skip("skipping postgres storage tests, service unreachable")
		return
	}

	storage, err := queue.NewPostgresStorage(db)
	if err != nil {
		t.Fatalf("failed to initialize postgres storage: %v", err)
	}

	ctx := context.Background()
	clearPostgresJobsTable(t, db)

	t.Run("EnqueueAndDequeue", func(t *testing.T) {
		assertPostgresEnqueueDequeue(t, ctx, storage)
	})

	t.Run("SkipLockedConcurrency", func(t *testing.T) {
		assertPostgresSkipLockedConcurrency(t, ctx, storage, db)
	})

	t.Run("RetryFlowAndBackoff", func(t *testing.T) {
		assertPostgresRetryFlow(t, ctx, storage, db)
	})
}

// TestRedisStorageFlow runs a sequence of tests asserting correct Redis storage driver behavior.
// Usage example:
//
//	go test -v ./test/...
func TestRedisStorageFlow(t *testing.T) {
	client := redis.NewClient(&redis.Options{
		Addr: redisAddress,
	})
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		t.Skip("skipping redis storage tests, service unreachable")
		return
	}

	storage, err := queue.NewRedisStorage(client)
	if err != nil {
		t.Fatalf("failed to initialize redis storage: %v", err)
	}

	client.FlushAll(ctx)

	t.Run("EnqueueAndDequeue", func(t *testing.T) {
		assertRedisEnqueueDequeue(t, ctx, storage)
	})

	t.Run("RetryFlowAndBackoff", func(t *testing.T) {
		assertRedisRetryFlow(t, ctx, storage, client)
	})
}

func clearPostgresJobsTable(t *testing.T, db *sql.DB) {
	_, err := db.Exec("DELETE FROM runiq_jobs")
	if err != nil {
		t.Logf("cleanup error: %v", err)
	}
}

func assertPostgresEnqueueDequeue(t *testing.T, ctx context.Context, s queue.Storage) {
	env := &queue.JobEnvelope{
		JobID: "job-postgres-flow-1",
		Queue: "postgres-test-queue",
		Name:  "test-job",
		Args:  []byte(`{"val":1}`),
	}
	if err := s.Enqueue(ctx, env); err != nil {
		t.Fatalf("failed to enqueue to postgres: %v", err)
	}

	deq, err := s.Dequeue(ctx, "postgres-test-queue")
	if err != nil {
		t.Fatalf("failed to dequeue from postgres: %v", err)
	}
	if deq == nil || deq.JobID != env.JobID {
		t.Errorf("mismatched dequeued job ID, expected %s, got %v", env.JobID, deq)
	}
}

func assertPostgresSkipLockedConcurrency(t *testing.T, ctx context.Context, s queue.Storage, db *sql.DB) {
	clearPostgresJobsTable(t, db)
	for i := 0; i < 5; i++ {
		_ = s.Enqueue(ctx, &queue.JobEnvelope{
			JobID: fmt.Sprintf("concurrent-job-%d", i),
			Queue: "concurrent",
			Name:  "test-job",
			Args:  []byte(`{}`),
		})
	}

	var wg sync.WaitGroup
	results := make(chan string, 5)

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Simulate concurrency competition
			deq, err := s.Dequeue(ctx, "concurrent")
			if err == nil && deq != nil {
				results <- deq.JobID
			}
		}()
	}

	wg.Wait()
	close(results)

	unique := make(map[string]bool)
	for id := range results {
		unique[id] = true
	}

	if len(unique) != 5 {
		t.Errorf("expected 5 unique jobs dequeued concurrently, got %d", len(unique))
	}
}

func assertRedisEnqueueDequeue(t *testing.T, ctx context.Context, s queue.Storage) {
	env := &queue.JobEnvelope{
		JobID: "job-redis-flow-1",
		Queue: "redis-test-queue",
		Name:  "test-job",
		Args:  []byte(`{"val":2}`),
	}
	if err := s.Enqueue(ctx, env); err != nil {
		t.Fatalf("failed to enqueue to redis: %v", err)
	}

	stats, err := s.GetStats(ctx)
	if err != nil {
		t.Fatalf("failed to get stats: %v", err)
	}
	if stats.Pending != 1 {
		t.Errorf("expected 1 pending job, got %d", stats.Pending)
	}

	deq, err := s.Dequeue(ctx, "redis-test-queue")
	if err != nil {
		t.Fatalf("failed to dequeue from redis: %v", err)
	}
	if deq == nil || deq.JobID != env.JobID {
		t.Errorf("mismatched dequeued job ID, expected %s, got %v", env.JobID, deq)
	}

	stats, _ = s.GetStats(ctx)
	if stats.Running != 1 {
		t.Errorf("expected 1 active job, got %d", stats.Running)
	}

	if err := s.Ack(ctx, env.JobID); err != nil {
		t.Fatalf("failed to ack job: %v", err)
	}

	stats, _ = s.GetStats(ctx)
	if stats.Processed != 1 {
		t.Errorf("expected 1 processed job, got %d", stats.Processed)
	}
	if len(stats.Jobs) != 1 || stats.Jobs[0].Status != "processed" {
		t.Errorf("expected 1 job listing in stats with 'processed' status, got %v", stats.Jobs)
	}
}

func assertRedisRetryFlow(t *testing.T, ctx context.Context, s queue.Storage, client *redis.Client) {
	env := &queue.JobEnvelope{
		JobID:       "job-redis-retry-2",
		Queue:       "redis-retry-queue",
		Name:        "test-retry-job",
		Args:        []byte(`{}`),
		MaxAttempts: 3,
	}
	if err := s.Enqueue(ctx, env); err != nil {
		t.Fatalf("failed to enqueue: %v", err)
	}

	deq, err := s.Dequeue(ctx, "redis-retry-queue")
	if err != nil || deq == nil {
		t.Fatalf("failed to dequeue: %v", err)
	}

	// Fail the job
	if err := s.Fail(ctx, deq.JobID, fmt.Errorf("temporary failure")); err != nil {
		t.Fatalf("failed to record failure: %v", err)
	}

	// Check that it's in the scheduled ZSet
	score, err := client.ZScore(ctx, "runiq:scheduled:redis-retry-queue", "job-redis-retry-2").Result()
	if err != nil {
		t.Fatalf("expected job to be scheduled in ZSet: %v", err)
	}
	if score <= 0 {
		t.Errorf("expected positive schedule score, got %f", score)
	}

	// Force schedule score to past to simulate time elapsed
	client.ZAdd(ctx, "runiq:scheduled:redis-retry-queue", redis.Z{Score: 0, Member: "job-redis-retry-2"})

	// Poll scheduled
	if err := s.PollScheduled(ctx, "redis-retry-queue"); err != nil {
		t.Fatalf("PollScheduled failed: %v", err)
	}

	// Dequeue again - should get the job back!
	deq2, err := s.Dequeue(ctx, "redis-retry-queue")
	if err != nil || deq2 == nil {
		t.Fatalf("failed to dequeue retried job: %v", err)
	}
	if deq2.JobID != "job-redis-retry-2" {
		t.Errorf("expected job-redis-retry-2, got %s", deq2.JobID)
	}
	if deq2.Attempts != 1 {
		t.Errorf("expected Attempts to be 1, got %d", deq2.Attempts)
	}

	// Fail it again
	if err := s.Fail(ctx, deq2.JobID, fmt.Errorf("another failure")); err != nil {
		t.Fatalf("failed to fail second time: %v", err)
	}

	// Force schedule score to past
	client.ZAdd(ctx, "runiq:scheduled:redis-retry-queue", redis.Z{Score: 0, Member: "job-redis-retry-2"})
	_ = s.PollScheduled(ctx, "redis-retry-queue")

	// Dequeue third time
	deq3, err := s.Dequeue(ctx, "redis-retry-queue")
	if err != nil || deq3 == nil {
		t.Fatalf("failed to dequeue third time: %v", err)
	}
	if deq3.Attempts != 2 {
		t.Errorf("expected Attempts to be 2, got %d", deq3.Attempts)
	}

	// Fail it third time - this is the 3rd fail (Attempts will become 3 == MaxAttempts), so it must fail permanently
	if err := s.Fail(ctx, deq3.JobID, fmt.Errorf("final failure")); err != nil {
		t.Fatalf("failed final fail: %v", err)
	}

	// ZSet should be empty for this job
	_, err = client.ZScore(ctx, "runiq:scheduled:redis-retry-queue", "job-redis-retry-2").Result()
	if err != redis.Nil {
		t.Errorf("expected job to be removed from scheduled ZSet, got error: %v", err)
	}

	// Stats should show 1 failed job
	stats, _ := s.GetStats(ctx)
	if stats.Failed != 1 {
		t.Errorf("expected 1 failed job in stats, got %d", stats.Failed)
	}
}

func assertPostgresRetryFlow(t *testing.T, ctx context.Context, s queue.Storage, db *sql.DB) {
	clearPostgresJobsTable(t, db)
	env := &queue.JobEnvelope{
		JobID:       "job-postgres-retry-2",
		Queue:       "postgres-retry-queue",
		Name:        "test-retry-job",
		Args:        []byte(`{}`),
		MaxAttempts: 3,
	}
	if err := s.Enqueue(ctx, env); err != nil {
		t.Fatalf("failed to enqueue: %v", err)
	}

	deq, err := s.Dequeue(ctx, "postgres-retry-queue")
	if err != nil || deq == nil {
		t.Fatalf("failed to dequeue: %v", err)
	}

	// Fail the job
	if err := s.Fail(ctx, deq.JobID, fmt.Errorf("temporary failure")); err != nil {
		t.Fatalf("failed to record failure: %v", err)
	}

	// In Postgres, after Fail, the job is still pending but run_at is in the future.
	// Let's verify that a subsequent Dequeue returns nil (not ready yet because run_at is future)
	deqFuture, err := s.Dequeue(ctx, "postgres-retry-queue")
	if err != nil {
		t.Fatalf("dequeue failed: %v", err)
	}
	if deqFuture != nil {
		t.Errorf("expected no job to be dequeued because run_at is in the future, got %v", deqFuture)
	}

	// Let's manually force run_at to the past to simulate time elapsed
	_, err = db.Exec("UPDATE runiq_jobs SET run_at = CURRENT_TIMESTAMP - INTERVAL '10 seconds' WHERE job_id = $1", "job-postgres-retry-2")
	if err != nil {
		t.Fatalf("failed to force run_at: %v", err)
	}

	// Dequeue again - should get the job back!
	deq2, err := s.Dequeue(ctx, "postgres-retry-queue")
	if err != nil || deq2 == nil {
		t.Fatalf("failed to dequeue retried job: %v", err)
	}
	if deq2.JobID != "job-postgres-retry-2" {
		t.Errorf("expected job-postgres-retry-2, got %s", deq2.JobID)
	}
	if deq2.Attempts != 1 {
		t.Errorf("expected Attempts to be 1, got %d", deq2.Attempts)
	}

	// Fail it again
	if err := s.Fail(ctx, deq2.JobID, fmt.Errorf("another failure")); err != nil {
		t.Fatalf("failed to fail second time: %v", err)
	}

	// Force run_at to past
	_, err = db.Exec("UPDATE runiq_jobs SET run_at = CURRENT_TIMESTAMP - INTERVAL '10 seconds' WHERE job_id = $1", "job-postgres-retry-2")
	if err != nil {
		t.Fatalf("failed to force run_at second time: %v", err)
	}

	// Dequeue third time
	deq3, err := s.Dequeue(ctx, "postgres-retry-queue")
	if err != nil || deq3 == nil {
		t.Fatalf("failed to dequeue third time: %v", err)
	}
	if deq3.Attempts != 2 {
		t.Errorf("expected Attempts to be 2, got %d", deq3.Attempts)
	}

	// Fail it third time - this is the 3rd fail (attempts will become 3 == maxAttempts), so it must fail permanently
	if err := s.Fail(ctx, deq3.JobID, fmt.Errorf("final failure")); err != nil {
		t.Fatalf("failed final fail: %v", err)
	}

	// Dequeue should return nil even if we force run_at (status should be failed)
	_, dbErr := db.Exec("UPDATE runiq_jobs SET run_at = CURRENT_TIMESTAMP - INTERVAL '10 seconds' WHERE job_id = $1", "job-postgres-retry-2")
	if dbErr != nil {
		t.Fatalf("failed to force run_at: %v", dbErr)
	}
	deq4, err := s.Dequeue(ctx, "postgres-retry-queue")
	if err != nil {
		t.Fatalf("dequeue failed: %v", err)
	}
	if deq4 != nil {
		t.Errorf("expected no job to be dequeued after permanent failure, got %v", deq4)
	}

	// Stats should show 1 failed job
	stats, _ := s.GetStats(ctx)
	if stats.Failed != 1 {
		t.Errorf("expected 1 failed job in stats, got %d", stats.Failed)
	}
}

