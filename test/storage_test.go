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

	t.Run("AdminActions", func(t *testing.T) {
		assertPostgresAdminActions(t, ctx, storage, db)
	})

	t.Run("UniqueJobs", func(t *testing.T) {
		assertPostgresUniqueJobs(t, ctx, storage, db)
	})

	t.Run("ActiveProcesses", func(t *testing.T) {
		assertPostgresActiveProcesses(t, ctx, storage, db)
	})

	t.Run("ThrottlingAndRateLimiting", func(t *testing.T) {
		assertPostgresThrottling(t, ctx, storage, db)
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

	t.Run("AdminActions", func(t *testing.T) {
		assertRedisAdminActions(t, ctx, storage, client)
	})

	t.Run("UniqueJobs", func(t *testing.T) {
		assertRedisUniqueJobs(t, ctx, storage, client)
	})

	t.Run("ActiveProcesses", func(t *testing.T) {
		assertRedisActiveProcesses(t, ctx, storage, client)
	})

	t.Run("ThrottlingAndRateLimiting", func(t *testing.T) {
		assertRedisThrottling(t, ctx, storage, client)
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

	// Verify it was pushed to runiq:dead list and NOT runiq:failed
	deadLen, err := client.LLen(ctx, "runiq:dead:redis-retry-queue").Result()
	if err != nil || deadLen != 1 {
		t.Errorf("expected 1 job in runiq:dead list, got %d (err: %v)", deadLen, err)
	}
	failedLen, err := client.LLen(ctx, "runiq:failed:redis-retry-queue").Result()
	if err != nil || failedLen != 0 {
		t.Errorf("expected 0 jobs in runiq:failed list, got %d (err: %v)", failedLen, err)
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

	// Verify it has status = 'dead' in the DB
	var status string
	err = db.QueryRowContext(ctx, "SELECT status FROM runiq_jobs WHERE job_id = $1", "job-postgres-retry-2").Scan(&status)
	if err != nil {
		t.Errorf("failed to query job status: %v", err)
	}
	if status != "dead" {
		t.Errorf("expected job status to be 'dead', got '%s'", status)
	}
}

func assertPostgresAdminActions(t *testing.T, ctx context.Context, s queue.Storage, db *sql.DB) {
	clearPostgresJobsTable(t, db)

	env1 := &queue.JobEnvelope{
		JobID: "job-p1",
		Queue: "queue-p",
		Name:  "TestJob",
		Args:  []byte("{}"),
	}
	env2 := &queue.JobEnvelope{
		JobID: "job-p2",
		Queue: "queue-p",
		Name:  "TestJob",
		Args:  []byte("{}"),
	}

	_ = s.Enqueue(ctx, env1)
	_ = s.Enqueue(ctx, env2)

	// Test Cancel/Delete of job-p1
	if err := s.Cancel(ctx, "job-p1"); err != nil {
		t.Fatalf("failed to cancel job: %v", err)
	}

	// Verify job-p1 is deleted
	var count int
	_ = db.QueryRow("SELECT COUNT(*) FROM runiq_jobs WHERE job_id = $1", "job-p1").Scan(&count)
	if count != 0 {
		t.Errorf("expected job-p1 to be deleted, found in db")
	}

	// Test Retry of job-p2 after failure
	deq, _ := s.Dequeue(ctx, "queue-p")
	_ = s.Fail(ctx, deq.JobID, fmt.Errorf("some failure"))

	// Force to failed status
	_, _ = db.Exec("UPDATE runiq_jobs SET status = 'failed' WHERE job_id = $1", "job-p2")

	if err := s.Retry(ctx, "job-p2"); err != nil {
		t.Fatalf("failed to retry job: %v", err)
	}

	var status string
	var errMsg sql.NullString
	_ = db.QueryRow("SELECT status, attempts, error_message FROM runiq_jobs WHERE job_id = $1", "job-p2").Scan(&status, &count, &errMsg)
	if status != "pending" || count != 0 || errMsg.String != "" {
		t.Errorf("expected status pending, attempts 0, empty error message; got status=%s, attempts=%d, err=%s", status, count, errMsg.String)
	}

	// Test ClearQueue
	if err := s.ClearQueue(ctx, "queue-p"); err != nil {
		t.Fatalf("failed to clear queue: %v", err)
	}
	_ = db.QueryRow("SELECT COUNT(*) FROM runiq_jobs WHERE queue = $1", "queue-p").Scan(&count)
	if count != 0 {
		t.Errorf("expected queue-p to be cleared, got count %d", count)
	}
}

func assertRedisAdminActions(t *testing.T, ctx context.Context, s queue.Storage, client *redis.Client) {
	client.FlushAll(ctx)

	env1 := &queue.JobEnvelope{
		JobID: "job-r1",
		Queue: "queue-r",
		Name:  "TestJob",
		Args:  []byte("{}"),
	}
	env2 := &queue.JobEnvelope{
		JobID: "job-r2",
		Queue: "queue-r",
		Name:  "TestJob",
		Args:  []byte("{}"),
	}

	_ = s.Enqueue(ctx, env1)
	_ = s.Enqueue(ctx, env2)

	// Test Cancel/Delete of job-r1
	if err := s.Cancel(ctx, "job-r1"); err != nil {
		t.Fatalf("failed to cancel job: %v", err)
	}

	// Verify job-r1 is deleted from jobs hash
	hexists, _ := client.HExists(ctx, "runiq:jobs", "job-r1").Result()
	if hexists {
		t.Error("expected job-r1 to be deleted from jobs hash")
	}

	// Test Retry of job-r2 after failure
	deq, _ := s.Dequeue(ctx, "queue-r")
	_ = s.Fail(ctx, deq.JobID, fmt.Errorf("some failure"))

	// Manually simulate job failed list
	client.LPush(ctx, "runiq:failed:queue-r", "job-r2")
	client.HSet(ctx, "runiq:errors", "job-r2", "some failure")

	if err := s.Retry(ctx, "job-r2"); err != nil {
		t.Fatalf("failed to retry job: %v", err)
	}

	// Verify it's removed from failed queue and errors, and added back to pending queue
	failedLenAfter, _ := client.LRem(ctx, "runiq:failed:queue-r", 0, "job-r2").Result()
	if failedLenAfter != 0 {
		t.Error("expected job-r2 to be removed from failed queue")
	}
	errExists, _ := client.HExists(ctx, "runiq:errors", "job-r2").Result()
	if errExists {
		t.Error("expected job-r2 error to be deleted")
	}
	pendingLen, _ := client.LLen(ctx, "runiq:queue:queue-r").Result()
	if pendingLen == 0 {
		t.Error("expected job-r2 to be pushed back to pending list")
	}

	// Test ClearQueue
	if err := s.ClearQueue(ctx, "queue-r"); err != nil {
		t.Fatalf("failed to clear queue: %v", err)
	}
	pendingLen, _ = client.LLen(ctx, "runiq:queue:queue-r").Result()
	if pendingLen != 0 {
		t.Errorf("expected pending list to be cleared, got len %d", pendingLen)
	}
}

func assertPostgresUniqueJobs(t *testing.T, ctx context.Context, s queue.Storage, db *sql.DB) {
	clearPostgresJobsTable(t, db)
	_, _ = db.Exec("DELETE FROM runiq_unique_locks")

	env1 := &queue.JobEnvelope{
		JobID:     "unique-job-p1",
		Queue:     "unique-queue-p",
		Name:      "TestJob",
		Args:      []byte("{}"),
		UniqueKey: "user-123",
	}
	env2 := &queue.JobEnvelope{
		JobID:     "unique-job-p2",
		Queue:     "unique-queue-p",
		Name:      "TestJob",
		Args:      []byte("{}"),
		UniqueKey: "user-123",
	}

	if err := s.Enqueue(ctx, env1); err != nil {
		t.Fatalf("first enqueue failed: %v", err)
	}

	err := s.Enqueue(ctx, env2)
	if err != queue.ErrDuplicateJob {
		t.Errorf("expected ErrDuplicateJob, got %v", err)
	}

	deq, err := s.Dequeue(ctx, "unique-queue-p")
	if err != nil || deq == nil {
		t.Fatalf("dequeue failed: %v", err)
	}
	if err := s.Ack(ctx, deq.JobID); err != nil {
		t.Fatalf("ack failed: %v", err)
	}

	if err := s.Enqueue(ctx, env2); err != nil {
		t.Errorf("second enqueue failed after ack: %v", err)
	}
}

func assertRedisUniqueJobs(t *testing.T, ctx context.Context, s queue.Storage, client *redis.Client) {
	client.FlushAll(ctx)

	env1 := &queue.JobEnvelope{
		JobID:     "unique-job-r1",
		Queue:     "unique-queue-r",
		Name:      "TestJob",
		Args:      []byte("{}"),
		UniqueKey: "user-123",
	}
	env2 := &queue.JobEnvelope{
		JobID:     "unique-job-r2",
		Queue:     "unique-queue-r",
		Name:      "TestJob",
		Args:      []byte("{}"),
		UniqueKey: "user-123",
	}

	if err := s.Enqueue(ctx, env1); err != nil {
		t.Fatalf("first enqueue failed: %v", err)
	}

	err := s.Enqueue(ctx, env2)
	if err != queue.ErrDuplicateJob {
		t.Errorf("expected ErrDuplicateJob, got %v", err)
	}

	deq, err := s.Dequeue(ctx, "unique-queue-r")
	if err != nil || deq == nil {
		t.Fatalf("dequeue failed: %v", err)
	}
	if err := s.Ack(ctx, deq.JobID); err != nil {
		t.Fatalf("ack failed: %v", err)
	}

	if err := s.Enqueue(ctx, env2); err != nil {
		t.Errorf("second enqueue failed after ack: %v", err)
	}
}

func assertPostgresActiveProcesses(t *testing.T, ctx context.Context, s queue.Storage, db *sql.DB) {
	_, _ = db.ExecContext(ctx, "DELETE FROM runiq_processes")

	proc := &queue.ProcessInfo{
		Queues:      []string{"default", "critical"},
		HeartbeatAt: time.Now(),
		ProcessID:   "proc-pg-1",
		Concurrency: 10,
	}

	if err := s.RegisterProcess(ctx, proc); err != nil {
		t.Fatalf("failed to register postgres process: %v", err)
	}

	active, err := s.GetActiveProcesses(ctx)
	if err != nil {
		t.Fatalf("failed to get active postgres processes: %v", err)
	}
	if len(active) != 1 || active[0].ProcessID != "proc-pg-1" {
		t.Fatalf("expected 1 process 'proc-pg-1', got %v", active)
	}

	if err := s.HeartbeatProcess(ctx, "proc-pg-1"); err != nil {
		t.Fatalf("failed to send heartbeat for postgres process: %v", err)
	}

	stats, err := s.GetStats(ctx)
	if err != nil {
		t.Fatalf("failed to get stats: %v", err)
	}
	if len(stats.Processes) != 1 || stats.Processes[0].ProcessID != "proc-pg-1" {
		t.Errorf("expected process in stats, got %v", stats.Processes)
	}
}

func assertRedisActiveProcesses(t *testing.T, ctx context.Context, s queue.Storage, client *redis.Client) {
	client.Del(ctx, "runiq:processes", "runiq:processes:heartbeat")

	proc := &queue.ProcessInfo{
		Queues:      []string{"default", "critical"},
		HeartbeatAt: time.Now(),
		ProcessID:   "proc-redis-1",
		Concurrency: 10,
	}

	if err := s.RegisterProcess(ctx, proc); err != nil {
		t.Fatalf("failed to register redis process: %v", err)
	}

	active, err := s.GetActiveProcesses(ctx)
	if err != nil {
		t.Fatalf("failed to get active redis processes: %v", err)
	}
	if len(active) != 1 || active[0].ProcessID != "proc-redis-1" {
		t.Fatalf("expected 1 process 'proc-redis-1', got %v", active)
	}

	if err := s.HeartbeatProcess(ctx, "proc-redis-1"); err != nil {
		t.Fatalf("failed to send heartbeat for redis process: %v", err)
	}

	stats, err := s.GetStats(ctx)
	if err != nil {
		t.Fatalf("failed to get stats: %v", err)
	}
	if len(stats.Processes) != 1 || stats.Processes[0].ProcessID != "proc-redis-1" {
		t.Errorf("expected process in stats, got %v", stats.Processes)
	}
}

func assertPostgresThrottling(t *testing.T, ctx context.Context, s queue.Storage, db *sql.DB) {
	clearPostgresJobsTable(t, db)
	_, _ = db.Exec("DELETE FROM runiq_rate_limits")

	// 1. Test GetRunningJobsCount
	// Enqueue a job and set it to status = 'running'
	env := &queue.JobEnvelope{
		JobID: "job-throttle-p1",
		Queue: "default",
		Name:  "TestThrottle",
		Args:  []byte("{}"),
	}
	if err := s.Enqueue(ctx, env); err != nil {
		t.Fatalf("failed to enqueue: %v", err)
	}
	_, err := db.Exec("UPDATE runiq_jobs SET status = 'running' WHERE job_id = $1", env.JobID)
	if err != nil {
		t.Fatalf("failed to update status: %v", err)
	}

	count, err := s.GetRunningJobsCount(ctx, "TestThrottle")
	if err != nil {
		t.Fatalf("GetRunningJobsCount failed: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 running job, got %d", count)
	}

	// 2. Test CheckRateLimit
	ok, err := s.CheckRateLimit(ctx, "TestThrottle", 2, 100*time.Millisecond)
	if err != nil || !ok {
		t.Errorf("expected rate limit check 1 to be true, got ok=%v, err=%v", ok, err)
	}
	ok, err = s.CheckRateLimit(ctx, "TestThrottle", 2, 100*time.Millisecond)
	if err != nil || !ok {
		t.Errorf("expected rate limit check 2 to be true, got ok=%v, err=%v", ok, err)
	}
	ok, err = s.CheckRateLimit(ctx, "TestThrottle", 2, 100*time.Millisecond)
	if err != nil || ok {
		t.Errorf("expected rate limit check 3 to be false (exceeded limit 2), got ok=%v, err=%v", ok, err)
	}

	// Wait for period to expire
	time.Sleep(150 * time.Millisecond)
	ok, err = s.CheckRateLimit(ctx, "TestThrottle", 2, 100*time.Millisecond)
	if err != nil || !ok {
		t.Errorf("expected rate limit check to reset after expiration, got ok=%v, err=%v", ok, err)
	}

	// 3. Test PostponeJob
	err = s.PostponeJob(ctx, env.JobID, env.Queue, 1*time.Second)
	if err != nil {
		t.Fatalf("PostponeJob failed: %v", err)
	}

	var status string
	var runAt *time.Time
	err = db.QueryRowContext(ctx, "SELECT status, run_at FROM runiq_jobs WHERE job_id = $1", env.JobID).Scan(&status, &runAt)
	if err != nil {
		t.Fatalf("failed to query job: %v", err)
	}
	if status != "pending" {
		t.Errorf("expected status to be 'pending', got '%s'", status)
	}
	if runAt == nil {
		t.Error("expected run_at to be set")
	}
}

func assertRedisThrottling(t *testing.T, ctx context.Context, s queue.Storage, client *redis.Client) {
	client.FlushAll(ctx)

	// 1. Test GetRunningJobsCount
	env := &queue.JobEnvelope{
		JobID: "job-throttle-r1",
		Queue: "default",
		Name:  "TestThrottle",
		Args:  []byte("{}"),
	}
	if err := s.Enqueue(ctx, env); err != nil {
		t.Fatalf("failed to enqueue: %v", err)
	}

	// Move job to active in Redis (simulate dequeue)
	_, err := s.Dequeue(ctx, env.Queue)
	if err != nil {
		t.Fatalf("dequeue failed: %v", err)
	}

	count, err := s.GetRunningJobsCount(ctx, "TestThrottle")
	if err != nil {
		t.Fatalf("GetRunningJobsCount failed: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 running job, got %d", count)
	}

	// 2. Test CheckRateLimit
	ok, err := s.CheckRateLimit(ctx, "TestThrottle", 2, 100*time.Millisecond)
	if err != nil || !ok {
		t.Errorf("expected rate limit check 1 to be true, got ok=%v, err=%v", ok, err)
	}
	ok, err = s.CheckRateLimit(ctx, "TestThrottle", 2, 100*time.Millisecond)
	if err != nil || !ok {
		t.Errorf("expected rate limit check 2 to be true, got ok=%v, err=%v", ok, err)
	}
	ok, err = s.CheckRateLimit(ctx, "TestThrottle", 2, 100*time.Millisecond)
	if err != nil || ok {
		t.Errorf("expected rate limit check 3 to be false (exceeded limit 2), got ok=%v, err=%v", ok, err)
	}

	// Wait for period to expire
	time.Sleep(150 * time.Millisecond)
	ok, err = s.CheckRateLimit(ctx, "TestThrottle", 2, 100*time.Millisecond)
	if err != nil || !ok {
		t.Errorf("expected rate limit check to reset after expiration, got ok=%v, err=%v", ok, err)
	}

	// 3. Test PostponeJob
	err = s.PostponeJob(ctx, env.JobID, env.Queue, 1*time.Second)
	if err != nil {
		t.Fatalf("PostponeJob failed: %v", err)
	}

	// Job should be removed from active
	isActive, err := client.SIsMember(ctx, "runiq:active:"+env.Queue, env.JobID).Result()
	if err != nil || isActive {
		t.Errorf("expected job to be removed from active, got isActive=%v, err=%v", isActive, err)
	}

	// Job should be in scheduled ZSet
	score, err := client.ZScore(ctx, "runiq:scheduled:"+env.Queue, env.JobID).Result()
	if err != nil || score <= 0 {
		t.Errorf("expected job to be in scheduled ZSet, got score=%v, err=%v", score, err)
	}
}



