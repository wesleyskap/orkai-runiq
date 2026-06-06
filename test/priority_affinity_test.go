package test

import (
	"context"
	"database/sql"
	"testing"

	_ "github.com/glebarez/go-sqlite"
	"github.com/wesleyskap/orkai-runiq/v3/queue"
)

func TestPriorityOrdering(t *testing.T) {
	s, db := setupTestStorage(t)
	defer db.Close()
	ctx := context.Background()

	j1 := queue.NewJob("default", "JobLow", []byte("low")).WithPriority(0)
	j2 := queue.NewJob("default", "JobHigh", []byte("high")).WithPriority(10)
	j3 := queue.NewJob("default", "JobMid", []byte("mid")).WithPriority(5)

	_ = s.Enqueue(ctx, j1)
	_ = s.Enqueue(ctx, j2)
	_ = s.Enqueue(ctx, j3)

	assertDequeueSequence(t, ctx, s, j2.JobID, j3.JobID, j1.JobID)
}

func TestWorkerAffinity(t *testing.T) {
	s, db := setupTestStorage(t)
	defer db.Close()
	ctx := context.Background()

	j := queue.NewJob("default", "HeavyJob", []byte("data")).RequireTags("gpu", "high-mem")
	_ = s.Enqueue(ctx, j)

	assertDequeueFails(t, ctx, s, nil)
	assertDequeueFails(t, ctx, s, []string{"gpu"})
	assertDequeueSucceeds(t, ctx, s, []string{"gpu", "high-mem", "cpu"}, j.JobID)
}

func setupTestStorage(t *testing.T) (*queue.SqliteStorage, *sql.DB) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}
	s, err := queue.NewSqliteStorage(db)
	if err != nil {
		db.Close()
		t.Fatalf("failed to create storage: %v", err)
	}
	return s, db
}

func assertDequeueSequence(t *testing.T, ctx context.Context, s *queue.SqliteStorage, ids ...string) {
	for i, id := range ids {
		deq, _ := s.Dequeue(ctx, "default", nil)
		if deq == nil || deq.JobID != id {
			t.Errorf("at index %d: expected %s, got %v", i, id, deq)
		}
	}
}

func assertDequeueFails(t *testing.T, ctx context.Context, s *queue.SqliteStorage, tags []string) {
	deq, _ := s.Dequeue(ctx, "default", tags)
	if deq != nil {
		t.Errorf("expected no job dequeued for tags %v, got %v", tags, deq)
	}
}

func assertDequeueSucceeds(t *testing.T, ctx context.Context, s *queue.SqliteStorage, tags []string, expectedID string) {
	deq, _ := s.Dequeue(ctx, "default", tags)
	if deq == nil || deq.JobID != expectedID {
		t.Errorf("expected job %s to be dequeued with tags %v, got %v", expectedID, tags, deq)
	}
}
