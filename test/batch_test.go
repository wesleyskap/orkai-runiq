package test

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
	"github.com/wesleyskap/orkai-runiq/queue"
)

type callbackTrackerJob struct {
	mu           sync.Mutex
	executedJobs []string
	completed    bool
}

func (c *callbackTrackerJob) Perform(ctx context.Context, args []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.completed = true
	return nil
}

type segmentJob struct {
	mu      sync.Mutex
	tracker *callbackTrackerJob
	name    string
	fail    bool
}

func (s *segmentJob) Perform(ctx context.Context, args []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fail {
		return errors.New("segment processing failed permanently")
	}
	s.tracker.mu.Lock()
	s.tracker.executedJobs = append(s.tracker.executedJobs, s.name)
	s.tracker.mu.Unlock()
	return nil
}

func TestClientBatchCreation(t *testing.T) {
	fakeStore := &FakeStorage{}
	client := queue.NewClient(fakeStore)

	ctx := context.Background()
	callback := &queue.JobEnvelope{
		Queue: "default",
		Name:  "CallbackJob",
		Args:  []byte("{}"),
	}

	batch, err := client.NewBatch(ctx, callback)
	if err != nil {
		t.Fatalf("failed to create batch: %v", err)
	}

	if batch.ID == "" {
		t.Error("expected batch to have a unique ID")
	}

	err = batch.Enqueue(ctx, "default", "Job1", []byte("{}"))
	if err != nil {
		t.Fatalf("failed to enqueue: %v", err)
	}

	err = batch.Submit(ctx)
	if err != nil {
		t.Fatalf("failed to submit: %v", err)
	}
}

func TestPostgresBatchFlow(t *testing.T) {
	db, err := sql.Open("postgres", postgresConnStr)
	if err != nil {
		t.Skipf("skipping postgres batch tests, connection failed: %v", err)
		return
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		t.Skip("skipping postgres batch tests, service unreachable")
		return
	}

	storage, err := queue.NewPostgresStorage(db)
	if err != nil {
		t.Fatalf("failed to initialize postgres storage: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	clearPostgresJobsTable(t, db)

	// Set up tracking jobs
	tracker := &callbackTrackerJob{}
	seg1 := &segmentJob{tracker: tracker, name: "seg1"}
	seg2 := &segmentJob{tracker: tracker, name: "seg2"}

	pool := queue.NewWorkerPool(storage, 3)
	pool.Register("Segment1", seg1)
	pool.Register("Segment2", seg2)
	pool.Register("CallbackJob", tracker)

	go func() {
		_ = pool.Start(ctx, "batch-queue")
	}()

	client := queue.NewClient(storage)
	batch, err := client.NewBatch(ctx, &queue.JobEnvelope{
		Queue: "batch-queue",
		Name:  "CallbackJob",
		Args:  []byte("{}"),
	})
	if err != nil {
		t.Fatalf("failed to create batch: %v", err)
	}

	err = batch.Enqueue(ctx, "batch-queue", "Segment1", []byte("{}"))
	if err != nil {
		t.Fatalf("failed to enqueue segment 1: %v", err)
	}

	err = batch.Enqueue(ctx, "batch-queue", "Segment2", []byte("{}"))
	if err != nil {
		t.Fatalf("failed to enqueue segment 2: %v", err)
	}

	// Verify that the callback is NOT enqueued before Submit is called
	time.Sleep(100 * time.Millisecond)
	tracker.mu.Lock()
	if tracker.completed {
		tracker.mu.Unlock()
		t.Fatal("callback should not execute before batch submission")
	}
	tracker.mu.Unlock()

	// Submit the batch
	err = batch.Submit(ctx)
	if err != nil {
		t.Fatalf("failed to submit batch: %v", err)
	}

	// Wait for jobs to execute and callback to complete
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		tracker.mu.Lock()
		done := tracker.completed
		tracker.mu.Unlock()
		if done {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	if !tracker.completed {
		t.Fatal("expected callback to have been executed successfully")
	}

	if len(tracker.executedJobs) != 2 {
		t.Errorf("expected 2 batch jobs to have completed, got %d", len(tracker.executedJobs))
	}
}

func TestRedisBatchFlow(t *testing.T) {
	client := redis.NewClient(&redis.Options{
		Addr: redisAddress,
	})
	defer client.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		t.Skip("skipping redis batch tests, service unreachable")
		return
	}

	storage, err := queue.NewRedisStorage(client)
	if err != nil {
		t.Fatalf("failed to initialize redis storage: %v", err)
	}

	client.FlushAll(ctx)

	tracker := &callbackTrackerJob{}
	seg1 := &segmentJob{tracker: tracker, name: "seg1"}
	seg2 := &segmentJob{tracker: tracker, name: "seg2"}

	pool := queue.NewWorkerPool(storage, 3)
	pool.Register("Segment1", seg1)
	pool.Register("Segment2", seg2)
	pool.Register("CallbackJob", tracker)

	go func() {
		_ = pool.Start(ctx, "batch-queue")
	}()

	qClient := queue.NewClient(storage)
	batch, err := qClient.NewBatch(ctx, &queue.JobEnvelope{
		Queue: "batch-queue",
		Name:  "CallbackJob",
		Args:  []byte("{}"),
	})
	if err != nil {
		t.Fatalf("failed to create batch: %v", err)
	}

	err = batch.Enqueue(ctx, "batch-queue", "Segment1", []byte("{}"))
	if err != nil {
		t.Fatalf("failed to enqueue segment 1: %v", err)
	}

	err = batch.Enqueue(ctx, "batch-queue", "Segment2", []byte("{}"))
	if err != nil {
		t.Fatalf("failed to enqueue segment 2: %v", err)
	}

	// Verify callback is not executed prematurely
	time.Sleep(100 * time.Millisecond)
	tracker.mu.Lock()
	if tracker.completed {
		tracker.mu.Unlock()
		t.Fatal("callback should not execute before batch submission")
	}
	tracker.mu.Unlock()

	// Submit batch
	err = batch.Submit(ctx)
	if err != nil {
		t.Fatalf("failed to submit batch: %v", err)
	}

	// Wait for jobs to execute
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		tracker.mu.Lock()
		done := tracker.completed
		tracker.mu.Unlock()
		if done {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	if !tracker.completed {
		t.Fatal("expected callback to have been executed successfully")
	}
}

func TestBatchFailureDoesNotTriggerCallback(t *testing.T) {
	client := redis.NewClient(&redis.Options{
		Addr: redisAddress,
	})
	defer client.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		t.Skip("skipping redis batch failure tests, service unreachable")
		return
	}

	storage, err := queue.NewRedisStorage(client)
	if err != nil {
		t.Fatalf("failed to initialize redis storage: %v", err)
	}

	client.FlushAll(ctx)

	tracker := &callbackTrackerJob{}
	seg1 := &segmentJob{tracker: tracker, name: "seg1"}
	seg2 := &segmentJob{tracker: tracker, name: "seg2", fail: true} // Will fail permanently

	pool := queue.NewWorkerPool(storage, 3)
	pool.Register("Segment1", seg1)
	pool.Register("Segment2", seg2)
	pool.Register("CallbackJob", tracker)

	go func() {
		_ = pool.Start(ctx, "batch-queue")
	}()

	qClient := queue.NewClient(storage)
	batch, err := qClient.NewBatch(ctx, &queue.JobEnvelope{
		Queue:       "batch-queue",
		Name:        "CallbackJob",
		Args:        []byte("{}"),
		MaxAttempts: 1,
	})
	if err != nil {
		t.Fatalf("failed to create batch: %v", err)
	}

	// Enqueue segment 1 (succeeds)
	err = batch.Enqueue(ctx, "batch-queue", "Segment1", []byte("{}"))
	if err != nil {
		t.Fatalf("failed to enqueue: %v", err)
	}

	// Enqueue segment 2 (fails permanently)
	err = batch.Enqueue(ctx, "batch-queue", "Segment2", []byte("{}"))
	if err != nil {
		t.Fatalf("failed to enqueue: %v", err)
	}

	err = batch.Submit(ctx)
	if err != nil {
		t.Fatalf("failed to submit: %v", err)
	}

	// Wait for jobs to execute
	time.Sleep(500 * time.Millisecond)

	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	if tracker.completed {
		t.Fatal("expected callback to NOT have been executed because segment 2 failed permanently")
	}
}
