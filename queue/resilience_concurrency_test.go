package queue

import (
	"context"
	"errors"
	"testing"
	"time"
)

type mockClientStorage struct {
	ClientStorage
	enqueueErr   error
	enqueueDelay time.Duration
}

func (m *mockClientStorage) Enqueue(ctx context.Context, env *JobEnvelope) error {
	if m.enqueueDelay > 0 {
		time.Sleep(m.enqueueDelay)
	}
	return m.enqueueErr
}

func TestCB_Trips(t *testing.T) {
	fs := &mockClientStorage{enqueueErr: errors.New("db error")}
	cfg := CircuitBreakerConfig{
		Cooldown:         1 * time.Second,
		FailureThreshold: 2,
	}
	c := NewClient(fs, WithCircuitBreaker(cfg))
	ctx := context.Background()

	_ = c.Enqueue(ctx, "q", "j", []byte("{}"))
	_ = c.Enqueue(ctx, "q", "j", []byte("{}"))
	_ = c.Enqueue(ctx, "q", "j", []byte("{}"))

	err := c.Enqueue(ctx, "q", "j", []byte("{}"))
	if !errors.Is(err, ErrCircuitBreakerOpen) {
		t.Errorf("expected ErrCircuitBreakerOpen, got %v", err)
	}
}

func TestCB_Recovery(t *testing.T) {
	fs := &mockClientStorage{enqueueErr: errors.New("db error")}
	cfg := CircuitBreakerConfig{
		Cooldown:         50 * time.Millisecond,
		FailureThreshold: 1,
	}
	c := NewClient(fs, WithCircuitBreaker(cfg))
	ctx := context.Background()

	_ = c.Enqueue(ctx, "q", "j", []byte("{}"))
	_ = c.Enqueue(ctx, "q", "j", []byte("{}"))

	time.Sleep(100 * time.Millisecond)
	fs.enqueueErr = nil

	if err := c.Enqueue(ctx, "q", "j", []byte("{}")); err != nil {
		t.Fatalf("expected trial call to succeed, got %v", err)
	}
	if err := c.Enqueue(ctx, "q", "j", []byte("{}")); err != nil {
		t.Errorf("expected subsequent calls to succeed, got %v", err)
	}
}

func TestCB_Latency(t *testing.T) {
	fs := &mockClientStorage{enqueueDelay: 20 * time.Millisecond}
	cfg := CircuitBreakerConfig{
		Cooldown:         1 * time.Second,
		LatencyThreshold: 10 * time.Millisecond,
		FailureThreshold: 2,
	}
	c := NewClient(fs, WithCircuitBreaker(cfg))
	ctx := context.Background()

	_ = c.Enqueue(ctx, "q", "j", []byte("{}"))
	_ = c.Enqueue(ctx, "q", "j", []byte("{}"))
	_ = c.Enqueue(ctx, "q", "j", []byte("{}"))

	err := c.Enqueue(ctx, "q", "j", []byte("{}"))
	if !errors.Is(err, ErrCircuitBreakerOpen) {
		t.Errorf("expected ErrCircuitBreakerOpen from latency, got %v", err)
	}
}

type mockWorkerPoolStorage struct {
	WorkerPoolStorage
	stats *Stats
}

func (m *mockWorkerPoolStorage) GetStats(ctx context.Context) (*Stats, error) {
	return m.stats, nil
}

func TestAutoscale_ScaleUp(t *testing.T) {
	fs := &mockWorkerPoolStorage{
		stats: &Stats{
			Queues: []QueueStats{{Name: "default", Pending: 15}},
		},
	}
	cfg := DynamicConcurrencyConfig{
		MinConcurrency:  2,
		MaxConcurrency:  5,
		QueueDepthLimit: 10,
		ScaleUpStep:     2,
	}
	w := NewWorkerPool(fs, 2, WithDynamicConcurrency(cfg))
	w.monitoredQueues = []string{"default"}
	w.setupSemaphore()
	ctx := context.Background()

	w.runAutoscaleIteration(ctx)
	if conc := w.getCurrentConcurrency(); conc != 4 {
		t.Errorf("expected scaled up concurrency 4, got %d", conc)
	}
}

func TestAutoscale_ScaleDown(t *testing.T) {
	fs := &mockWorkerPoolStorage{
		stats: &Stats{
			Queues: []QueueStats{{Name: "default", Pending: 0}},
		},
	}
	cfg := DynamicConcurrencyConfig{
		MinConcurrency:  2,
		MaxConcurrency:  5,
		QueueDepthLimit: 10,
		ScaleDownStep:   1,
	}
	w := NewWorkerPool(fs, 2, WithDynamicConcurrency(cfg))
	w.monitoredQueues = []string{"default"}
	w.setupSemaphore()
	w.adjustConcurrency(5)
	ctx := context.Background()

	w.runAutoscaleIteration(ctx)
	if conc := w.getCurrentConcurrency(); conc != 4 {
		t.Errorf("expected scaled down concurrency 4, got %d", conc)
	}
}
