package test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	_ "github.com/glebarez/go-sqlite"
	"github.com/wesleyskap/orkai-runiq/v2/queue"
)

func TestSqliteStorageFlow(t *testing.T) {
	// Open in-memory SQLite database
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("failed to open in-memory sqlite db: %v", err)
	}
	defer db.Close()

	storage, err := queue.NewSqliteStorage(db)
	if err != nil {
		t.Fatalf("failed to initialize sqlite storage: %v", err)
	}

	ctx := context.Background()

	t.Run("EnqueueAndDequeue", func(t *testing.T) {
		clearSqliteJobsTable(t, db)
		assertSqliteEnqueueDequeue(t, ctx, storage)
	})

	t.Run("RetryFlowAndBackoff", func(t *testing.T) {
		clearSqliteJobsTable(t, db)
		assertSqliteRetryFlow(t, ctx, storage, db)
	})

	t.Run("AdminActions", func(t *testing.T) {
		clearSqliteJobsTable(t, db)
		assertSqliteAdminActions(t, ctx, storage, db)
	})

	t.Run("UniqueJobs", func(t *testing.T) {
		clearSqliteJobsTable(t, db)
		assertSqliteUniqueJobs(t, ctx, storage, db)
	})

	t.Run("ActiveProcesses", func(t *testing.T) {
		clearSqliteJobsTable(t, db)
		assertSqliteActiveProcesses(t, ctx, storage, db)
	})

	t.Run("ThrottlingAndRateLimiting", func(t *testing.T) {
		clearSqliteJobsTable(t, db)
		assertSqliteThrottling(t, ctx, storage, db)
	})

	t.Run("Batches", func(t *testing.T) {
		clearSqliteJobsTable(t, db)
		assertSqliteBatches(t, ctx, storage, db)
	})

	t.Run("NewFeaturesV240", func(t *testing.T) {
		clearSqliteJobsTable(t, db)
		assertSqliteNewFeatures(t, ctx, storage, db)
	})
}

func clearSqliteJobsTable(t *testing.T, db *sql.DB) {
	tables := []string{
		"runiq_jobs",
		"runiq_unique_locks",
		"runiq_processes",
		"runiq_cron_locks",
		"runiq_rate_limits",
		"runiq_batches",
		"runiq_paused_queues",
	}
	for _, tbl := range tables {
		_, err := db.Exec(fmt.Sprintf("DELETE FROM %s", tbl))
		if err != nil {
			t.Logf("cleanup error for %s: %v", tbl, err)
		}
	}
}

func assertSqliteEnqueueDequeue(t *testing.T, ctx context.Context, s *queue.SqliteStorage) {
	env := &queue.JobEnvelope{
		JobID: "job-sqlite-flow-1",
		Queue: "sqlite-test-queue",
		Name:  "test-job",
		Args:  []byte(`{"val":1}`),
	}
	if err := s.Enqueue(ctx, env); err != nil {
		t.Fatalf("failed to enqueue to sqlite: %v", err)
	}

	deq, err := s.Dequeue(ctx, "sqlite-test-queue")
	if err != nil {
		t.Fatalf("failed to dequeue from sqlite: %v", err)
	}
	if deq == nil || deq.JobID != env.JobID {
		t.Errorf("mismatched dequeued job ID, expected %s, got %v", env.JobID, deq)
	}
}

func assertSqliteRetryFlow(t *testing.T, ctx context.Context, s *queue.SqliteStorage, db *sql.DB) {
	env := &queue.JobEnvelope{
		JobID:       "job-sqlite-retry-2",
		Queue:       "sqlite-retry-queue",
		Name:        "test-retry-job",
		Args:        []byte(`{}`),
		MaxAttempts: 3,
	}
	if err := s.Enqueue(ctx, env); err != nil {
		t.Fatalf("failed to enqueue: %v", err)
	}

	deq, err := s.Dequeue(ctx, "sqlite-retry-queue")
	if err != nil || deq == nil {
		t.Fatalf("failed to dequeue: %v", err)
	}

	// Fail the job
	if err := s.Fail(ctx, deq.JobID, fmt.Errorf("temporary failure")); err != nil {
		t.Fatalf("failed to record failure: %v", err)
	}

	// In SQLite, after Fail, the job is still pending but run_at is in the future.
	// Verify that a subsequent Dequeue returns nil (not ready yet because run_at is future)
	deqFuture, err := s.Dequeue(ctx, "sqlite-retry-queue")
	if err != nil {
		t.Fatalf("dequeue failed: %v", err)
	}
	if deqFuture != nil {
		t.Errorf("expected no job to be dequeued because run_at is in the future, got %v", deqFuture)
	}

	// Force run_at to the past to simulate time elapsed
	_, err = db.Exec("UPDATE runiq_jobs SET run_at = datetime('now', '-10 seconds') WHERE job_id = ?", "job-sqlite-retry-2")
	if err != nil {
		t.Fatalf("failed to force run_at: %v", err)
	}

	// Dequeue again - should get the job back!
	deq2, err := s.Dequeue(ctx, "sqlite-retry-queue")
	if err != nil || deq2 == nil {
		t.Fatalf("failed to dequeue retried job: %v", err)
	}
	if deq2.JobID != "job-sqlite-retry-2" {
		t.Errorf("expected job-sqlite-retry-2, got %s", deq2.JobID)
	}
	if deq2.Attempts != 1 {
		t.Errorf("expected Attempts to be 1, got %d", deq2.Attempts)
	}

	// Fail it again
	if err := s.Fail(ctx, deq2.JobID, fmt.Errorf("another failure")); err != nil {
		t.Fatalf("failed to fail second time: %v", err)
	}

	// Force run_at to past
	_, err = db.Exec("UPDATE runiq_jobs SET run_at = datetime('now', '-10 seconds') WHERE job_id = ?", "job-sqlite-retry-2")
	if err != nil {
		t.Fatalf("failed to force run_at second time: %v", err)
	}

	// Dequeue third time
	deq3, err := s.Dequeue(ctx, "sqlite-retry-queue")
	if err != nil || deq3 == nil {
		t.Fatalf("failed to dequeue third time: %v", err)
	}
	if deq3.Attempts != 2 {
		t.Errorf("expected Attempts to be 2, got %d", deq3.Attempts)
	}

	// Fail it third time (attempts becomes 3 == maxAttempts), so it must fail permanently
	if err := s.Fail(ctx, deq3.JobID, fmt.Errorf("final failure")); err != nil {
		t.Fatalf("failed final fail: %v", err)
	}

	// Dequeue should return nil even if we force run_at (status should be dead)
	_, _ = db.Exec("UPDATE runiq_jobs SET run_at = datetime('now', '-10 seconds') WHERE job_id = ?", "job-sqlite-retry-2")
	deq4, err := s.Dequeue(ctx, "sqlite-retry-queue")
	if err != nil {
		t.Fatalf("dequeue failed: %v", err)
	}
	if deq4 != nil {
		t.Errorf("expected no job to be dequeued after permanent failure, got %v", deq4)
	}

	// Stats should show 1 failed/dead job
	stats, _ := s.GetStats(ctx)
	if stats.Failed != 1 {
		t.Errorf("expected 1 failed job in stats, got %d", stats.Failed)
	}

	var status string
	err = db.QueryRowContext(ctx, "SELECT status FROM runiq_jobs WHERE job_id = ?", "job-sqlite-retry-2").Scan(&status)
	if err != nil || status != "dead" {
		t.Errorf("expected job status to be 'dead', got '%s' (err: %v)", status, err)
	}
}

func assertSqliteAdminActions(t *testing.T, ctx context.Context, s *queue.SqliteStorage, db *sql.DB) {
	env1 := &queue.JobEnvelope{JobID: "job-s1", Queue: "queue-s", Name: "TestJob", Args: []byte("{}")}
	env2 := &queue.JobEnvelope{JobID: "job-s2", Queue: "queue-s", Name: "TestJob", Args: []byte("{}")}

	_ = s.Enqueue(ctx, env1)
	_ = s.Enqueue(ctx, env2)

	// Test Cancel/Delete of job-s1
	if err := s.Cancel(ctx, "job-s1"); err != nil {
		t.Fatalf("failed to cancel job: %v", err)
	}

	var count int
	_ = db.QueryRow("SELECT COUNT(*) FROM runiq_jobs WHERE job_id = ?", "job-s1").Scan(&count)
	if count != 0 {
		t.Error("expected job-s1 to be deleted, found in db")
	}

	// Test Retry of job-s2 after failure
	deq, _ := s.Dequeue(ctx, "queue-s")
	_ = s.Fail(ctx, deq.JobID, fmt.Errorf("some failure"))

	// Force to failed status
	_, _ = db.Exec("UPDATE runiq_jobs SET status = 'failed' WHERE job_id = ?", "job-s2")

	if err := s.Retry(ctx, "job-s2"); err != nil {
		t.Fatalf("failed to retry job: %v", err)
	}

	var status string
	var errMsg sql.NullString
	_ = db.QueryRow("SELECT status, attempts, error_message FROM runiq_jobs WHERE job_id = ?", "job-s2").Scan(&status, &count, &errMsg)
	if status != "pending" || count != 0 || errMsg.String != "" {
		t.Errorf("expected status pending, attempts 0, empty error message; got status=%s, attempts=%d, err=%s", status, count, errMsg.String)
	}

	// Test ClearQueue
	if err := s.ClearQueue(ctx, "queue-s"); err != nil {
		t.Fatalf("failed to clear queue: %v", err)
	}
	_ = db.QueryRow("SELECT COUNT(*) FROM runiq_jobs WHERE queue = ?", "queue-s").Scan(&count)
	if count != 0 {
		t.Errorf("expected queue-s to be cleared, got count %d", count)
	}
}

func assertSqliteUniqueJobs(t *testing.T, ctx context.Context, s *queue.SqliteStorage, db *sql.DB) {
	env1 := &queue.JobEnvelope{
		JobID:     "unique-job-s1",
		Queue:     "unique-queue-s",
		Name:      "TestJob",
		Args:      []byte("{}"),
		UniqueKey: "user-123",
	}
	env2 := &queue.JobEnvelope{
		JobID:     "unique-job-s2",
		Queue:     "unique-queue-s",
		Name:      "TestJob",
		Args:      []byte("{}"),
		UniqueKey: "user-123",
	}

	if err := s.Enqueue(ctx, env1); err != nil {
		t.Fatalf("first enqueue failed: %v", err)
	}

	err := s.Enqueue(ctx, env2)
	if !errors.Is(err, queue.ErrDuplicateJob) {
		t.Errorf("expected ErrDuplicateJob, got %v", err)
	}

	deq, err := s.Dequeue(ctx, "unique-queue-s")
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

func assertSqliteActiveProcesses(t *testing.T, ctx context.Context, s *queue.SqliteStorage, db *sql.DB) {
	proc := &queue.ProcessInfo{
		Queues:      []string{"default", "critical"},
		HeartbeatAt: time.Now(),
		ProcessID:   "proc-sq-1",
		Concurrency: 10,
	}

	if err := s.RegisterProcess(ctx, proc); err != nil {
		t.Fatalf("failed to register sqlite process: %v", err)
	}

	active, err := s.GetActiveProcesses(ctx)
	if err != nil {
		t.Fatalf("failed to get active sqlite processes: %v", err)
	}
	if len(active) != 1 || active[0].ProcessID != "proc-sq-1" {
		t.Fatalf("expected 1 process 'proc-sq-1', got %v", active)
	}

	if err := s.HeartbeatProcess(ctx, "proc-sq-1"); err != nil {
		t.Fatalf("failed to send heartbeat for sqlite process: %v", err)
	}

	stats, err := s.GetStats(ctx)
	if err != nil {
		t.Fatalf("failed to get stats: %v", err)
	}
	if len(stats.Processes) != 1 || stats.Processes[0].ProcessID != "proc-sq-1" {
		t.Errorf("expected process in stats, got %v", stats.Processes)
	}
}

func assertSqliteThrottling(t *testing.T, ctx context.Context, s *queue.SqliteStorage, db *sql.DB) {
	env := &queue.JobEnvelope{
		JobID: "job-throttle-s1",
		Queue: "default",
		Name:  "TestThrottle",
		Args:  []byte("{}"),
	}
	if err := s.Enqueue(ctx, env); err != nil {
		t.Fatalf("failed to enqueue: %v", err)
	}
	_, err := db.Exec("UPDATE runiq_jobs SET status = 'running' WHERE job_id = ?", env.JobID)
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

	// Test CheckRateLimit
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

	// Wait for expiration
	time.Sleep(150 * time.Millisecond)
	ok, err = s.CheckRateLimit(ctx, "TestThrottle", 2, 100*time.Millisecond)
	if err != nil || !ok {
		t.Errorf("expected rate limit check to reset after expiration, got ok=%v, err=%v", ok, err)
	}

	// Test PostponeJob
	err = s.PostponeJob(ctx, env.JobID, env.Queue, 1*time.Second)
	if err != nil {
		t.Fatalf("PostponeJob failed: %v", err)
	}

	var status string
	var runAt *time.Time
	err = db.QueryRowContext(ctx, "SELECT status, run_at FROM runiq_jobs WHERE job_id = ?", env.JobID).Scan(&status, &runAt)
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

func assertSqliteBatches(t *testing.T, ctx context.Context, s *queue.SqliteStorage, db *sql.DB) {
	callback := &queue.JobEnvelope{
		JobID: "callback-job-1",
		Queue: "default",
		Name:  "CallbackJob",
		Args:  []byte(`{"val":"callback"}`),
	}

	// Create Batch
	if err := s.CreateBatch(ctx, "batch-1", callback, nil); err != nil {
		t.Fatalf("failed to create batch: %v", err)
	}

	// Enqueue in batch
	job1 := &queue.JobEnvelope{JobID: "batch-job-1", Queue: "default", Name: "Job1", Args: []byte("{}")}
	job2 := &queue.JobEnvelope{JobID: "batch-job-2", Queue: "default", Name: "Job2", Args: []byte("{}")}

	if err := s.EnqueueInBatch(ctx, "batch-1", job1); err != nil {
		t.Fatalf("failed to enqueue job 1 in batch: %v", err)
	}
	if err := s.EnqueueInBatch(ctx, "batch-1", job2); err != nil {
		t.Fatalf("failed to enqueue job 2 in batch: %v", err)
	}

	// Dequeue job 1 and Ack
	deq1, err := s.Dequeue(ctx, "default")
	if err != nil || deq1 == nil {
		t.Fatalf("failed to dequeue job 1: %v (deq=%v)", err, deq1)
	}
	if err := s.Ack(ctx, deq1.JobID); err != nil {
		t.Fatalf("failed to ack job 1: %v", err)
	}

	// Submit/Seal batch
	if err := s.SubmitBatch(ctx, "batch-1"); err != nil {
		t.Fatalf("failed to submit batch: %v", err)
	}

	// Dequeue job 2 and Ack
	deq2, err := s.Dequeue(ctx, "default")
	if err != nil || deq2 == nil {
		t.Fatalf("failed to dequeue job 2: %v", err)
	}
	if err := s.Ack(ctx, deq2.JobID); err != nil {
		t.Fatalf("failed to ack job 2: %v", err)
	}

	// After both jobs are acked and batch was sealed, callback should be enqueued
	deqCallback, err := s.Dequeue(ctx, "default")
	if err != nil || deqCallback == nil {
		t.Fatalf("failed to dequeue callback job: %v", err)
	}
	if deqCallback.Name != "CallbackJob" {
		t.Errorf("expected callback job name to be 'CallbackJob', got '%s'", deqCallback.Name)
	}
}

func assertSqliteNewFeatures(t *testing.T, ctx context.Context, s *queue.SqliteStorage, db *sql.DB) {
	t.Run("CronRegistration", func(t *testing.T) { assertSqliteCron(t, ctx, s) })
	t.Run("JobDetail", func(t *testing.T) { assertSqliteJobDetail(t, ctx, s) })
	t.Run("BulkRetryPurge", func(t *testing.T) { assertSqliteBulkActions(t, ctx, s, db) })
}

func assertSqliteCron(t *testing.T, ctx context.Context, s *queue.SqliteStorage) {
	crons := []queue.CronJob{
		{Name: "test-cron", Spec: "*/5 * * * *", Queue: "default", Payload: []byte(`{"hello":"world"}`)},
	}
	if err := s.RegisterCronJobs(ctx, crons); err != nil {
		t.Fatalf("failed to register cron: %v", err)
	}
	stats, err := s.GetStats(ctx)
	if err != nil || len(stats.CronJobs) != 1 {
		t.Fatalf("failed to get stats with cron: %v", err)
	}
	if stats.CronJobs[0].Name != "test-cron" || stats.CronJobs[0].Payload != `{"hello":"world"}` {
		t.Errorf("mismatched cron stats: %+v", stats.CronJobs[0])
	}
}

func assertSqliteJobDetail(t *testing.T, ctx context.Context, s *queue.SqliteStorage) {
	env := &queue.JobEnvelope{JobID: "detail-1", Queue: "default", Name: "Job", Args: []byte(`{"a":1}`)}
	if err := s.Enqueue(ctx, env); err != nil {
		t.Fatalf("failed to enqueue: %v", err)
	}
	detail, err := s.GetJobDetail(ctx, "detail-1")
	if err != nil || detail == nil {
		t.Fatalf("failed to get job detail: %v", err)
	}
	if string(detail.Args) != `{"a":1}` {
		t.Errorf("expected payload '{\"a\":1}', got %s", detail.Args)
	}
}

func assertSqliteBulkActions(t *testing.T, ctx context.Context, s *queue.SqliteStorage, db *sql.DB) {
	env1 := &queue.JobEnvelope{JobID: "b1", Queue: "q", Name: "Job", Args: []byte("{}")}
	env2 := &queue.JobEnvelope{JobID: "b2", Queue: "q", Name: "Job", Args: []byte("{}")}
	_ = s.Enqueue(ctx, env1)
	_ = s.Enqueue(ctx, env2)
	assertBulkRetry(t, ctx, s, db)
	assertBulkPurge(t, ctx, s, db)
}

func assertBulkRetry(t *testing.T, ctx context.Context, s *queue.SqliteStorage, db *sql.DB) {
	_, _ = db.Exec("UPDATE runiq_jobs SET status = 'failed' WHERE job_id IN ('b1', 'b2')")
	if err := s.RetryAllFailed(ctx); err != nil {
		t.Fatalf("RetryAllFailed failed: %v", err)
	}
	var count int
	_ = db.QueryRow("SELECT COUNT(*) FROM runiq_jobs WHERE status = 'pending' AND job_id IN ('b1', 'b2')").Scan(&count)
	if count != 2 {
		t.Errorf("expected 2 pending jobs, got %d", count)
	}
}

func assertBulkPurge(t *testing.T, ctx context.Context, s *queue.SqliteStorage, db *sql.DB) {
	_, _ = db.Exec("UPDATE runiq_jobs SET status = 'dead' WHERE job_id IN ('b1', 'b2')")
	if err := s.PurgeAllFailed(ctx); err != nil {
		t.Fatalf("PurgeAllFailed failed: %v", err)
	}
	var count int
	_ = db.QueryRow("SELECT COUNT(*) FROM runiq_jobs WHERE job_id IN ('b1', 'b2')").Scan(&count)
	if count != 0 {
		t.Errorf("expected 0 jobs, got %d", count)
	}
}
