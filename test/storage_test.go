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
}

func clearPostgresJobsTable(t *testing.T, db *sql.DB) {
	_, err := db.Exec("DELETE FROM runiq_jobs")
	if err != nil {
		t.Logf("cleanup error: %v", err)
	}
}

func assertPostgresEnqueueDequeue(t *testing.T, ctx context.Context, s queue.Storage) {
	env := &queue.JobEnvelope{
		JobID: "job-p1",
		Queue: "default",
		Name:  "test-job",
		Args:  []byte(`{"val":1}`),
	}
	if err := s.Enqueue(ctx, env); err != nil {
		t.Fatalf("failed to enqueue to postgres: %v", err)
	}

	deq, err := s.Dequeue(ctx, "default")
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
		JobID: "job-r1",
		Queue: "default",
		Name:  "test-job",
		Args:  []byte(`{"val":2}`),
	}
	if err := s.Enqueue(ctx, env); err != nil {
		t.Fatalf("failed to enqueue to redis: %v", err)
	}

	deq, err := s.Dequeue(ctx, "default")
	if err != nil {
		t.Fatalf("failed to dequeue from redis: %v", err)
	}
	if deq == nil || deq.JobID != env.JobID {
		t.Errorf("mismatched dequeued job ID, expected %s, got %v", env.JobID, deq)
	}
}
