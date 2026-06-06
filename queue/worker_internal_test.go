package queue

import (
	"context"
	"testing"
)

type mockStorageForWeights struct {
	WorkerPoolStorage
	queries []string
}

func (m *mockStorageForWeights) Dequeue(ctx context.Context, queueName string, workerTags []string) (*JobEnvelope, error) {
	m.queries = append(m.queries, queueName)
	return nil, nil // return nil to force trying other queues
}

func (m *mockStorageForWeights) RegisterProcess(ctx context.Context, info *ProcessInfo) error {
	return nil
}

func (m *mockStorageForWeights) HeartbeatProcess(ctx context.Context, processID string) error {
	return nil
}

func (m *mockStorageForWeights) IsQueuePaused(ctx context.Context, queue string) (bool, error) {
	res := false
	return res, nil
}

func TestWorkerPoolWeightedRotation(t *testing.T) {
	store := &mockStorageForWeights{}
	pool := NewWorkerPool(store, 1, WithQueueWeights(map[string]int{
		"critical": 3,
		"default":  1,
	}))

	// Mimic Start building firstQueues
	pool.firstQueues = []string{"critical", "critical", "critical", "default"}

	ctx := context.Background()

	// Let's call fetchNext 4 times
	for i := 0; i < 4; i++ {
		_, _ = pool.fetchNext(ctx, []string{"critical", "default"})
	}

	// We expect Dequeue to be called with "critical" as primary 3 times, and "default" as primary 1 time.
	// Since Dequeue returns nil (no job), the worker checks both queues in order.
	// For idx=0 (primary=critical): critical, default
	// For idx=1 (primary=critical): critical, default
	// For idx=2 (primary=critical): critical, default
	// For idx=3 (primary=default): default, critical
	expectedQueries := []string{
		"critical", "default",
		"critical", "default",
		"critical", "default",
		"default", "critical",
	}

	if len(store.queries) != len(expectedQueries) {
		t.Fatalf("expected %d queries, got %d", len(expectedQueries), len(store.queries))
	}

	for i, q := range expectedQueries {
		if store.queries[i] != q {
			t.Errorf("at query index %d: expected %q, got %q", i, q, store.queries[i])
		}
	}
}

func TestWorkerPoolStrictPriorityFallback(t *testing.T) {
	store := &mockStorageForWeights{}
	pool := NewWorkerPool(store, 1) // No weights

	ctx := context.Background()
	for i := 0; i < 2; i++ {
		_, _ = pool.fetchNext(ctx, []string{"critical", "default"})
	}

	expectedQueries := []string{
		"critical", "default",
		"critical", "default",
	}

	if len(store.queries) != len(expectedQueries) {
		t.Fatalf("expected %d queries, got %d", len(expectedQueries), len(store.queries))
	}

	for i, q := range expectedQueries {
		if store.queries[i] != q {
			t.Errorf("at query index %d: expected %q, got %q", i, q, store.queries[i])
		}
	}
}
