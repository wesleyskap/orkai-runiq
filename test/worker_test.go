package test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/wesleyskap/orkai-runiq/v2/queue"
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

// TestClientEnqueueUnique verifies that EnqueueUnique properly propagates UniqueKey and UniqueTTL.
func TestClientEnqueueUnique(t *testing.T) {
	fakeStore := &FakeStorage{}
	client := queue.NewClient(fakeStore)

	ctx := context.Background()
	err := client.EnqueueUnique(ctx, "default", "UniqueJob", []byte("{}"), "test-lock-key", 5*time.Minute)
	if err != nil {
		t.Fatalf("failed to EnqueueUnique: %v", err)
	}

	if len(fakeStore.Enqueued) != 1 {
		t.Fatal("expected 1 job to be enqueued")
	}

	env := fakeStore.Enqueued[0]
	if env.UniqueKey != "test-lock-key" {
		t.Errorf("expected UniqueKey 'test-lock-key', got %q", env.UniqueKey)
	}
	if env.UniqueTTL != 5*time.Minute {
		t.Errorf("expected UniqueTTL 5m, got %v", env.UniqueTTL)
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

// TestWorkerProcessRegistration verifies that starting the worker pool registers the process and starts sending heartbeats.
func TestWorkerProcessRegistration(t *testing.T) {
	fakeStore := &FakeStorage{}
	pool := queue.NewWorkerPool(fakeStore, 4)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = pool.Start(ctx, "critical", "default")
	}()

	// Wait for worker pool to execute startup registration
	time.Sleep(50 * time.Millisecond)

	if len(fakeStore.Processes) != 1 {
		t.Fatalf("expected 1 process registered, got %d", len(fakeStore.Processes))
	}

	p := fakeStore.Processes[0]
	if p.Concurrency != 4 {
		t.Errorf("expected concurrency 4, got %d", p.Concurrency)
	}
	if len(p.Queues) != 2 || p.Queues[0] != "critical" || p.Queues[1] != "default" {
		t.Errorf("expected queues ['critical', 'default'], got %v", p.Queues)
	}
}

// TestWorkerPoolMaxConcurrency validates that when a job name exceeds its configured max concurrency, it is postponed and not executed.
func TestWorkerPoolMaxConcurrency(t *testing.T) {
	fakeStore := &FakeStorage{}
	fakeStore.RunningCountToReturn = map[string]int{
		"LimitJob": 6, // exceeds maxConcurrency = 5
	}

	job := &trackableJob{}
	pool := queue.NewWorkerPool(fakeStore, 1)
	// Register with Max Concurrency option
	pool.Register("LimitJob", job, queue.WithMaxConcurrency(5))

	env := &queue.JobEnvelope{
		JobID: "job-limit-1",
		Queue: "default",
		Name:  "LimitJob",
		Args:  []byte("{}"),
	}
	_ = fakeStore.Enqueue(context.Background(), env)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = pool.Start(ctx, "default")
	}()

	time.Sleep(50 * time.Millisecond)

	job.mu.Lock()
	executed := job.executed
	job.mu.Unlock()

	if executed {
		t.Error("expected job to be throttled (postponed) due to concurrency limit, but it executed")
	}

	if len(fakeStore.Postponed) != 1 || fakeStore.Postponed[0] != "job-limit-1" {
		t.Errorf("expected job-limit-1 to be postponed, got %v", fakeStore.Postponed)
	}
}

// TestWorkerPoolRateLimit validates that when a job name exceeds its configured rate limit, it is postponed and not executed.
func TestWorkerPoolRateLimit(t *testing.T) {
	fakeStore := &FakeStorage{}
	fakeStore.RateLimitToReturn = map[string]bool{
		"RateJob": false, // Rate limit exceeded
	}

	job := &trackableJob{}
	pool := queue.NewWorkerPool(fakeStore, 1)
	// Register with Rate Limit option
	pool.Register("RateJob", job, queue.WithRateLimit(10, time.Second))

	env := &queue.JobEnvelope{
		JobID: "job-rate-1",
		Queue: "default",
		Name:  "RateJob",
		Args:  []byte("{}"),
	}
	_ = fakeStore.Enqueue(context.Background(), env)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = pool.Start(ctx, "default")
	}()

	time.Sleep(50 * time.Millisecond)

	job.mu.Lock()
	executed := job.executed
	job.mu.Unlock()

	if executed {
		t.Error("expected job to be throttled (postponed) due to rate limit, but it executed")
	}

	if len(fakeStore.Postponed) != 1 || fakeStore.Postponed[0] != "job-rate-1" {
		t.Errorf("expected job-rate-1 to be postponed, got %v", fakeStore.Postponed)
	}
}


