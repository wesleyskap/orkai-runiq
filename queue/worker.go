package queue

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"time"
)

// WorkerPool processes jobs concurrently from the storage backend.
type WorkerPool struct {
	firstQueues  []string
	cronJobs     []CronJob
	storage      Storage
	logger       Logger
	tracer       Tracer
	processID    string
	registry     map[string]Job
	weights      map[string]int
	fetchCounter uint64
	concurrency  int
}

// NewWorkerPool instantiates a new WorkerPool.
// Usage example:
//	pool := queue.NewWorkerPool(storage, 5, queue.WithWorkerLogger(logger))
func NewWorkerPool(storage Storage, concurrency int, opts ...WorkerOption) *WorkerPool {
	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = "unknown"
	}
	pid := os.Getpid()
	processID := fmt.Sprintf("%s:%d:%d", hostname, pid, time.Now().UnixNano()%100000)

	w := &WorkerPool{
		storage:     storage,
		logger:      &defaultLogger{},
		tracer:      &defaultTracer{},
		processID:   processID,
		registry:    make(map[string]Job),
		weights:     make(map[string]int),
		concurrency: concurrency,
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

// WithQueueWeights configures the relative execution weight for each queue.
func WithQueueWeights(weights map[string]int) WorkerOption {
	return func(w *WorkerPool) {
		w.weights = weights
	}
}

// Register maps a job name to a Job implementation.
func (w *WorkerPool) Register(name string, job Job) {
	w.registry[name] = job
}

// RegisterCron registers a recurring task under the specified cron spec.
func (w *WorkerPool) RegisterCron(spec, queue, name string, payload []byte) {
	w.cronJobs = append(w.cronJobs, CronJob{
		Payload: payload,
		Spec:    spec,
		Name:    name,
		Queue:   queue,
	})
}

// Start spawns workers and begins consuming from specified queues.
func (w *WorkerPool) Start(ctx context.Context, queues ...string) error {
	if len(w.weights) > 0 {
		var fqs []string
		for _, q := range queues {
			wt := 1
			if val, ok := w.weights[q]; ok && val > 0 {
				wt = val
			}
			for i := 0; i < wt; i++ {
				fqs = append(fqs, q)
			}
		}
		w.firstQueues = fqs
	}

	info := &ProcessInfo{
		Queues:      queues,
		HeartbeatAt: time.Now(),
		ProcessID:   w.processID,
		Concurrency: w.concurrency,
	}
	if err := w.storage.RegisterProcess(ctx, info); err != nil {
		w.logger.Error(ctx, "failed to register worker process", err)
	}

	// Start process heartbeat goroutine
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_ = w.storage.HeartbeatProcess(ctx, w.processID)
			}
		}
	}()

	// Start cron scheduler goroutine
	if len(w.cronJobs) > 0 {
		go w.startCronScheduler(ctx)
	}

	sem := make(chan struct{}, w.concurrency)

	// Start scheduled jobs poller goroutine
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				for _, q := range queues {
					_ = w.storage.PollScheduled(ctx, q)
				}
			}
		}
	}()

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
	if len(w.firstQueues) > 0 {
		searchOrder := w.buildSearchOrder(queues)
		for _, q := range searchOrder {
			env, err := w.storage.Dequeue(ctx, q)
			if err == nil && env != nil {
				return env, nil
			}
		}
		return nil, nil
	}
	return w.fetchStrict(ctx, queues)
}

func (w *WorkerPool) buildSearchOrder(queues []string) []string {
	idx := int((atomic.AddUint64(&w.fetchCounter, 1) - 1) % uint64(len(w.firstQueues)))
	primary := w.firstQueues[idx]
	order := make([]string, 0, len(queues))
	order = append(order, primary)
	for _, q := range queues {
		if q != primary {
			order = append(order, q)
		}
	}
	return order
}

func (w *WorkerPool) fetchStrict(ctx context.Context, queues []string) (*JobEnvelope, error) {
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

func (w *WorkerPool) startCronScheduler(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	var lastMinute int = -1
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := time.Now().UTC()
			if now.Minute() != lastMinute {
				lastMinute = now.Minute()
				w.processCronJobs(ctx, now)
			}
		}
	}
}

func (w *WorkerPool) processCronJobs(ctx context.Context, now time.Time) {
	for _, cron := range w.cronJobs {
		if MatchCron(cron.Spec, now) {
			go w.enqueueCronJob(ctx, cron, now)
		}
	}
}

func (w *WorkerPool) enqueueCronJob(ctx context.Context, cron CronJob, now time.Time) {
	ok, err := w.storage.LockCronExecution(ctx, cron.Name, now)
	if err != nil {
		w.logger.Error(ctx, "failed to check cron lock", err, "cron_name", cron.Name)
		return
	}
	if !ok {
		return
	}
	env := &JobEnvelope{
		JobID:       fmt.Sprintf("cron-%s-%d", cron.Name, now.Unix()),
		Queue:       cron.Queue,
		Name:        cron.Name,
		Args:        cron.Payload,
		MaxAttempts: 3,
	}
	if err := w.storage.Enqueue(ctx, env); err != nil {
		w.logger.Error(ctx, "failed to enqueue cron job", err, "cron_name", cron.Name)
	}
}
