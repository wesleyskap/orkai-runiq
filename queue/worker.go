package queue

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// WorkerPool processes jobs concurrently from the storage backend.
type WorkerPool struct {
	storage     Storage
	concurrency int
	logger      Logger
	tracer      Tracer
	registry    map[string]Job
}

// NewWorkerPool instantiates a new WorkerPool.
// Usage example:
//	pool := queue.NewWorkerPool(storage, 5, queue.WithWorkerLogger(logger))
func NewWorkerPool(storage Storage, concurrency int, opts ...WorkerOption) *WorkerPool {
	w := &WorkerPool{
		storage:     storage,
		concurrency: concurrency,
		logger:      &defaultLogger{},
		tracer:      &defaultTracer{},
		registry:    make(map[string]Job),
	}
	for _, opt := range opts {
		opt(w)
	}
	return w
}

// WorkerOption defines functional configuration options for WorkerPool.
type WorkerOption func(*WorkerPool)

// WithWorkerTracer sets a custom tracer.
func WithWorkerTracer(t Tracer) WorkerOption {
	return func(w *WorkerPool) {
		w.tracer = t
	}
}

// WithWorkerLogger sets a custom logger.
func WithWorkerLogger(l Logger) WorkerOption {
	return func(w *WorkerPool) {
		w.logger = l
	}
}

// Register maps a job name to a Job implementation.
func (w *WorkerPool) Register(name string, job Job) {
	w.registry[name] = job
}

// Start spawns workers and begins consuming from specified queues.
func (w *WorkerPool) Start(ctx context.Context, queues ...string) error {
	sem := make(chan struct{}, w.concurrency)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			w.acquireAndProcess(ctx, sem, queues)
		}
	}
}

func (w *WorkerPool) acquireAndProcess(ctx context.Context, sem chan struct{}, queues []string) {
	select {
	case sem <- struct{}{}:
		job, err := w.fetchNext(ctx, queues)
		if err != nil || job == nil {
			<-sem
			time.Sleep(100 * time.Millisecond)
			return
		}
		go w.runJobWithSemaphore(ctx, sem, job)
	case <-ctx.Done():
	}
}

func (w *WorkerPool) runJobWithSemaphore(ctx context.Context, sem chan struct{}, env *JobEnvelope) {
	defer func() { <-sem }()
	w.executeJob(ctx, env)
}

func (w *WorkerPool) fetchNext(ctx context.Context, queues []string) (*JobEnvelope, error) {
	for _, q := range queues {
		env, err := w.storage.Dequeue(ctx, q)
		if err == nil && env != nil {
			return env, nil
		}
	}
	return nil, nil
}

func (w *WorkerPool) executeJob(ctx context.Context, env *JobEnvelope) {
	job, ok := w.registry[env.Name]
	if !ok {
		_ = w.storage.Fail(ctx, env.JobID, errors.New("job type not registered"))
		return
	}
	w.runJobPerform(ctx, env, job)
}

func (w *WorkerPool) runJobPerform(ctx context.Context, env *JobEnvelope, job Job) {
	jobCtx := w.tracer.InjectTrace(ctx, env.TraceContext.TraceID, env.TraceContext.SpanID)
	start := time.Now()
	w.tracer.IncrementCounter(jobCtx, "runiq_job_started", map[string]string{"name": env.Name})

	var runErr error
	defer func() {
		if r := recover(); r != nil {
			runErr = fmt.Errorf("panic: %v", r)
		}
		w.finalizeJob(jobCtx, env, start, runErr)
	}()
	runErr = job.Perform(jobCtx, env.Args)
}

func (w *WorkerPool) finalizeJob(ctx context.Context, env *JobEnvelope, start time.Time, err error) {
	duration := time.Since(start)
	w.tracer.RecordLatency(ctx, "runiq_job_duration", duration, map[string]string{"name": env.Name})
	if err != nil {
		_ = w.storage.Fail(ctx, env.JobID, err)
		w.tracer.IncrementCounter(ctx, "runiq_job_failed", map[string]string{"name": env.Name})
		w.logger.Error(ctx, "job failed", err, "job_id", env.JobID)
		return
	}
	_ = w.storage.Ack(ctx, env.JobID)
	w.tracer.IncrementCounter(ctx, "runiq_job_success", map[string]string{"name": env.Name})
	w.logger.Info(ctx, "job completed", "job_id", env.JobID)
}
