package test

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	_ "github.com/glebarez/go-sqlite"
	"github.com/wesleyskap/orkai-runiq/v3/queue"
)

func newTestStorage(t *testing.T) (*queue.SqliteStorage, *sql.DB) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("failed to open sqlite: %v", err)
	}
	storage, err := queue.NewSqliteStorage(db)
	if err != nil {
		db.Close()
		t.Fatalf("failed to create sqlite storage: %v", err)
	}
	return storage, db
}

func TestWorkflowSequentialExecution(t *testing.T) {
	storage, db := newTestStorage(t)
	defer db.Close()
	client := queue.NewClient(storage)
	ctx := context.Background()

	jobA := queue.NewJob("default", "JobA", []byte("a"))
	jobB := queue.NewJob("default", "JobB", []byte("b"))
	jobC := queue.NewJob("default", "JobC", []byte("c"))

	jobB.DependsOn(jobA)
	jobC.DependsOn(jobB)

	if err := client.EnqueueWorkflow(ctx, jobA, jobB, jobC); err != nil {
		t.Fatalf("failed to enqueue: %v", err)
	}
	assertSequentialLifecycle(t, ctx, storage, jobA, jobB, jobC)
}

func assertSequentialLifecycle(t *testing.T, ctx context.Context, s *queue.SqliteStorage, jobA, jobB, jobC *queue.JobEnvelope) {
	deq, _ := s.Dequeue(ctx, "default", nil)
	if deq == nil || deq.JobID != jobA.JobID {
		t.Fatalf("expected to dequeue jobA, got %v", deq)
	}
	if deq2, _ := s.Dequeue(ctx, "default", nil); deq2 != nil {
		t.Fatalf("expected no other jobs available, got %v", deq2)
	}
	if err := s.Ack(ctx, jobA.JobID); err != nil {
		t.Fatalf("failed to ack jobA: %v", err)
	}
	assertSecondStep(t, ctx, s, jobB, jobC)
}

func assertSecondStep(t *testing.T, ctx context.Context, s *queue.SqliteStorage, jobB, jobC *queue.JobEnvelope) {
	deq, _ := s.Dequeue(ctx, "default", nil)
	if deq == nil || deq.JobID != jobB.JobID {
		t.Fatalf("expected to dequeue jobB, got %v", deq)
	}
	if err := s.Ack(ctx, jobB.JobID); err != nil {
		t.Fatalf("failed to ack jobB: %v", err)
	}
	deq, _ = s.Dequeue(ctx, "default", nil)
	if deq == nil || deq.JobID != jobC.JobID {
		t.Fatalf("expected to dequeue jobC, got %v", deq)
	}
}

func TestWorkflowComplexDAG(t *testing.T) {
	storage, db := newTestStorage(t)
	defer db.Close()
	client := queue.NewClient(storage)
	ctx := context.Background()

	jobA := queue.NewJob("default", "JobA", []byte("a"))
	jobB := queue.NewJob("default", "JobB", []byte("b"))
	jobC := queue.NewJob("default", "JobC", []byte("c"))

	jobC.DependsOn(jobA)
	jobC.DependsOn(jobB)

	if err := client.EnqueueWorkflow(ctx, jobA, jobB, jobC); err != nil {
		t.Fatalf("failed to enqueue: %v", err)
	}
	assertComplexDAGLifecycle(t, ctx, storage, jobA, jobB, jobC)
}

func assertComplexDAGLifecycle(t *testing.T, ctx context.Context, s *queue.SqliteStorage, jobA, jobB, jobC *queue.JobEnvelope) {
	deq1, _ := s.Dequeue(ctx, "default", nil)
	deq2, _ := s.Dequeue(ctx, "default", nil)
	if deq1 == nil || deq2 == nil {
		t.Fatalf("expected to dequeue both A and B, got %v and %v", deq1, deq2)
	}
	if deq3, _ := s.Dequeue(ctx, "default", nil); deq3 != nil {
		t.Fatalf("expected jobC to be blocked, got %v", deq3)
	}
	if err := s.Ack(ctx, deq1.JobID); err != nil {
		t.Fatalf("failed to ack: %v", err)
	}
	if deq3, _ := s.Dequeue(ctx, "default", nil); deq3 != nil {
		t.Fatalf("expected jobC to be blocked, got %v", deq3)
	}
	assertComplexDAGSecondStep(t, ctx, s, deq2.JobID, jobC.JobID)
}

func assertComplexDAGSecondStep(t *testing.T, ctx context.Context, s *queue.SqliteStorage, deq2ID, jobCID string) {
	if err := s.Ack(ctx, deq2ID); err != nil {
		t.Fatalf("failed to ack: %v", err)
	}
	deq3, _ := s.Dequeue(ctx, "default", nil)
	if deq3 == nil || deq3.JobID != jobCID {
		t.Fatalf("expected to dequeue jobC, got %v", deq3)
	}
}

func TestWorkflowCascadeFailure(t *testing.T) {
	storage, db := newTestStorage(t)
	defer db.Close()
	client := queue.NewClient(storage)
	ctx := context.Background()

	jobA, jobB, jobC := createTestJobsChain()
	jobA.MaxAttempts = 1
	if err := client.EnqueueWorkflow(ctx, jobA, jobB, jobC); err != nil {
		t.Fatalf("failed to enqueue: %v", err)
	}
	failJobOnce(t, ctx, storage, jobA.JobID)
	assertCascadeDeadJobs(t, ctx, storage, jobB.JobID, jobC.JobID)
}

func createTestJobsChain() (*queue.JobEnvelope, *queue.JobEnvelope, *queue.JobEnvelope) {
	jobA := queue.NewJob("default", "JobA", []byte("a"))
	jobB := queue.NewJob("default", "JobB", []byte("b"))
	jobC := queue.NewJob("default", "JobC", []byte("c"))
	jobB.DependsOn(jobA)
	jobC.DependsOn(jobB)
	return jobA, jobB, jobC
}

func failJobOnce(t *testing.T, ctx context.Context, s *queue.SqliteStorage, jobID string) {
	deq, _ := s.Dequeue(ctx, "default", nil)
	if deq == nil || deq.JobID != jobID {
		t.Fatalf("expected to dequeue %s, got %v", jobID, deq)
	}
	if err := s.Fail(ctx, jobID, errors.New("fail")); err != nil {
		t.Fatalf("fail failed: %v", err)
	}
}

func assertCascadeDeadJobs(t *testing.T, ctx context.Context, s *queue.SqliteStorage, jobBID, jobCID string) {
	jobs, _, err := s.GetJobs(ctx, "", "dead", 1, 10)
	if err != nil {
		t.Fatalf("GetJobs failed: %v", err)
	}
	bDead, cDead := false, false
	for _, job := range jobs {
		if job.JobID == jobBID {
			bDead = true
		}
		if job.JobID == jobCID {
			cDead = true
		}
	}
	if !bDead || !cDead {
		t.Fatalf("expected jobB and jobC to be dead, got %v", jobs)
	}
}

func TestWorkflowCascadeCancellation(t *testing.T) {
	storage, db := newTestStorage(t)
	defer db.Close()
	client := queue.NewClient(storage)
	ctx := context.Background()

	jobA, jobB, jobC := createTestJobsChain()
	if err := client.EnqueueWorkflow(ctx, jobA, jobB, jobC); err != nil {
		t.Fatalf("failed to enqueue: %v", err)
	}
	if err := storage.Cancel(ctx, jobA.JobID); err != nil {
		t.Fatalf("failed to cancel: %v", err)
	}
	assertCascadeDeadJobs(t, ctx, storage, jobB.JobID, jobC.JobID)
}
