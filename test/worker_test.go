package test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/wesleyskap/orkai-runiq/queue"
)

type mockTracer struct {
	mu        sync.Mutex
	extracted bool
	injected  bool
	latencies []string
	counters  []string
}

func (m *mockTracer) ExtractTrace(ctx context.Context) (string, string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.extracted = true
	return "mock-trace-id", "mock-span-id"
}

func (m *mockTracer) InjectTrace(ctx context.Context, traceID, spanID string) context.Context {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.injected = true
	return context.WithValue(ctx, "trace-id-key", traceID)
}

func (m *mockTracer) RecordLatency(ctx context.Context, name string, dur time.Duration, tags map[string]string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.latencies = append(m.latencies, name)
}

func (m *mockTracer) IncrementCounter(ctx context.Context, name string, tags map[string]string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.counters = append(m.counters, name)
}

type trackableJob struct {
	mu       sync.Mutex
	executed bool
	traceID  string
}

func (t *trackableJob) Perform(ctx context.Context, args []byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.executed = true
	if id, ok := ctx.Value("trace-id-key").(string); ok {
		t.traceID = id
	}
	return nil
}

type panicJob struct{}

func (p *panicJob) Perform(ctx context.Context, args []byte) error {
	panic("something went terribly wrong")
}

// TestClientTraceExtraction asserts that Client enqueues jobs with propagated trace details.
// Usage example:
//	go test -v ./test/...
func TestClientTraceExtraction(t *testing.T) {
	fakeStore := &FakeStorage{}
	mt := &mockTracer{}
	client := queue.NewClient(fakeStore, queue.WithClientTracer(mt))

	ctx := context.Background()
	err := client.Enqueue(ctx, "default", "TrackableJob", []byte("{}"))
	if err != nil {
		t.Fatalf("failed to enqueue: %v", err)
	}

	if len(fakeStore.Enqueued) == 0 {
		t.Fatal("expected job to be enqueued")
	}

	env := fakeStore.Enqueued[0]
	if env.TraceContext.TraceID != "mock-trace-id" {
		t.Errorf("expected TraceID 'mock-trace-id', got %q", env.TraceContext.TraceID)
	}
}

// TestWorkerPoolExecution validates worker startup, context injection, and success telemetry.
// Usage example:
//	go test -v ./test/...
func TestWorkerPoolExecution(t *testing.T) {
	fakeStore := &FakeStorage{}
	mt := &mockTracer{}
	job := &trackableJob{}

	pool := queue.NewWorkerPool(fakeStore, 2, queue.WithWorkerTracer(mt))
	pool.Register("TrackableJob", job)

	// Prepare a job envelope with pre-existing trace data
	env := &queue.JobEnvelope{
		JobID: "job-1",
		Queue: "default",
		Name:  "TrackableJob",
		Args:  []byte("{}"),
		TraceContext: queue.TraceContext{
			TraceID: "incoming-trace",
			SpanID:  "incoming-span",
		},
	}
	_ = fakeStore.Enqueue(context.Background(), env)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = pool.Start(ctx, "default")
	}()

	// Wait for processing
	time.Sleep(100 * time.Millisecond)

	job.mu.Lock()
	defer job.mu.Unlock()
	if !job.executed {
		t.Error("expected job to run")
	}
	if job.traceID != "incoming-trace" {
		t.Errorf("expected context trace ID 'incoming-trace', got %q", job.traceID)
	}
}

// TestWorkerPoolPanicRecovery validates that worker handles panics without crashing and marks storage failures.
// Usage example:
//	go test -v ./test/...
func TestWorkerPoolPanicRecovery(t *testing.T) {
	fakeStore := &FakeStorage{}
	pool := queue.NewWorkerPool(fakeStore, 1)
	pool.Register("PanicJob", &panicJob{})

	env := &queue.JobEnvelope{
		JobID: "job-panic",
		Queue: "default",
		Name:  "PanicJob",
		Args:  []byte("{}"),
	}
	_ = fakeStore.Enqueue(context.Background(), env)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = pool.Start(ctx, "default")
	}()

	time.Sleep(100 * time.Millisecond)
	if fakeStore.Failed == nil || fakeStore.Failed["job-panic"] == nil {
		t.Error("expected job-panic to be marked as failed in storage")
	}
}

// TestClientScheduling verifies Client scheduling (EnqueueIn and EnqueueAt) behavior.
func TestClientScheduling(t *testing.T) {
	fakeStore := &FakeStorage{}
	client := queue.NewClient(fakeStore)
	ctx := context.Background()

	// 1. Test EnqueueIn
	delay := 10 * time.Minute
	nowBefore := time.Now()
	err := client.EnqueueIn(ctx, "default", "ScheduledJob", []byte("{}"), delay)
	if err != nil {
		t.Fatalf("failed to EnqueueIn: %v", err)
	}

	if len(fakeStore.Enqueued) != 1 {
		t.Fatalf("expected 1 job enqueued, got %d", len(fakeStore.Enqueued))
	}

	env := fakeStore.Enqueued[0]
	if env.RunAt == nil {
		t.Fatal("expected RunAt to be set")
	}

	expectedTime := nowBefore.Add(delay)
	diff := env.RunAt.Sub(expectedTime)
	if diff < -5*time.Second || diff > 5*time.Second {
		t.Errorf("expected RunAt to be around %v, got %v", expectedTime, *env.RunAt)
	}

	// 2. Test EnqueueAt
	targetTime := time.Now().Add(1 * time.Hour)
	err = client.EnqueueAt(ctx, "default", "ScheduledJobAt", []byte("{}"), targetTime)
	if err != nil {
		t.Fatalf("failed to EnqueueAt: %v", err)
	}

	if len(fakeStore.Enqueued) != 2 {
		t.Fatalf("expected 2 jobs enqueued, got %d", len(fakeStore.Enqueued))
	}

	envAt := fakeStore.Enqueued[1]
	if envAt.RunAt == nil {
		t.Fatal("expected RunAt to be set")
	}
	if !envAt.RunAt.Equal(targetTime) {
		t.Errorf("expected RunAt to be %v, got %v", targetTime, *envAt.RunAt)
	}
}

