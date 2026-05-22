package queue

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type handlerConfig struct {
	maxConcurrency int
	rateLimit      int
	ratePeriod     time.Duration
}

// HandlerOption configures throttling parameters for registered job handlers.
type HandlerOption func(*handlerConfig)

// WithMaxConcurrency configures the maximum global concurrency limit for a job.
// Usage example:
//
//	pool.Register("Payment", job, queue.WithMaxConcurrency(5))
func WithMaxConcurrency(max int) HandlerOption {
	return func(cfg *handlerConfig) {
		cfg.maxConcurrency = max
	}
}

// WithRateLimit configures the maximum execution rate limit for a job.
// Usage example:
//
//	pool.Register("SMS", job, queue.WithRateLimit(100, time.Minute))
func WithRateLimit(limit int, period time.Duration) HandlerOption {
	return func(cfg *handlerConfig) {
		cfg.rateLimit = limit
		cfg.ratePeriod = period
	}
}

// WorkerPool processes jobs concurrently from the storage backend.
type WorkerPool struct {
	firstQueues     []string
	cronJobs        []CronJob
	storage         WorkerPoolStorage
	logger          Logger
	tracer          Tracer
	processID       string
	registry        map[string]Job
	configs         map[string]handlerConfig
	weights         map[string]int
	middlewares     []func(JobHandler) JobHandler
	eventHandlers   map[EventType][]EventHandler
	fetchCounter    uint64
	concurrency     int
	shutdownTimeout time.Duration
	dlqTTL          time.Duration
	activeWorkers   sync.WaitGroup
}

func getWorkerProcessID() string {
	hostname, _ := os.Hostname()
	if hostname == "" {
		hostname = "unknown"
	}
	return fmt.Sprintf("%s:%d:%d", hostname, os.Getpid(), time.Now().UnixNano()%100000)
}

// NewWorkerPool instantiates a new WorkerPool.
func NewWorkerPool(storage WorkerPoolStorage, concurrency int, opts ...WorkerOption) *WorkerPool {
	w := &WorkerPool{
		storage:         storage,
		logger:          &defaultLogger{},
		tracer:          &defaultTracer{},
		processID:       getWorkerProcessID(),
		registry:        make(map[string]Job),
		configs:         make(map[string]handlerConfig),
		weights:         make(map[string]int),
		concurrency:     concurrency,
		shutdownTimeout: 10 * time.Second,
		eventHandlers:   make(map[EventType][]EventHandler),
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

// WithShutdownTimeout sets the maximum duration to wait for active jobs to finish during shutdown.
func WithShutdownTimeout(timeout time.Duration) WorkerOption {
	return func(w *WorkerPool) {
		w.shutdownTimeout = timeout
	}
}

// WithDLQTTL sets the TTL for dead letter queue (DLQ) auto-purge.
func WithDLQTTL(ttl time.Duration) WorkerOption {
	return func(w *WorkerPool) {
		w.dlqTTL = ttl
	}
}

// Register maps a job name to a Job implementation with optional rate limiting/concurrency.
func (w *WorkerPool) Register(name string, job Job, opts ...HandlerOption) {
	w.registry[name] = job
	var cfg handlerConfig
	for _, opt := range opts {
		opt(&cfg)
	}
	w.configs[name] = cfg
}

// RegisterCron registers a recurring task under the specified cron spec.
func (w *WorkerPool) RegisterCron(spec, queue, name string, payload []byte, opts ...CronOption) {
	cfg := cronOptions{
		location: time.UTC,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	w.cronJobs = append(w.cronJobs, CronJob{
		Payload:  payload,
		Spec:     spec,
		Name:     name,
		Queue:    queue,
		Timezone: cfg.location.String(),
		Location: cfg.location,
	})
}

// Use registers job execution middlewares.
func (w *WorkerPool) Use(mws ...func(JobHandler) JobHandler) {
	w.middlewares = append(w.middlewares, mws...)
}

// OnEvent registers an event handler for a specific lifecycle event.
func (w *WorkerPool) OnEvent(eventType EventType, h EventHandler) {
	w.eventHandlers[eventType] = append(w.eventHandlers[eventType], h)
}

// Start spawns workers and begins consuming from specified queues.
func (w *WorkerPool) Start(ctx context.Context, queues ...string) error {
	w.setupWeightedQueues(queues)
	w.registerProcess(ctx, queues)
	if len(w.cronJobs) > 0 {
		if err := w.storage.RegisterCronJobs(ctx, w.cronJobs); err != nil {
			w.logger.Error(ctx, "failed to register cron jobs", err)
		}
	}
	w.startBackgroundLoops(ctx, queues)
	w.runProcessingLoop(ctx, queues)
	return ctx.Err()
}


func (w *WorkerPool) setupWeightedQueues(queues []string) {
	if len(w.weights) == 0 {
		return
	}
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

func (w *WorkerPool) registerProcess(ctx context.Context, queues []string) {
	info := &ProcessInfo{
		Queues:      queues,
		HeartbeatAt: time.Now(),
		ProcessID:   w.processID,
		Concurrency: w.concurrency,
	}
	if err := w.storage.RegisterProcess(ctx, info); err != nil {
		w.logger.Error(ctx, "failed to register worker process", err, "process_id", w.processID)
	}
}

func (w *WorkerPool) startBackgroundLoops(ctx context.Context, queues []string) {
	go w.startHeartbeat(ctx)
	if len(w.cronJobs) > 0 {
		go w.startCronScheduler(ctx)
	}
	go w.startScheduledPoller(ctx, queues)
	go w.startDLQAutopurge(ctx)
	go w.startBatchTimeoutWatcher(ctx)
}

func (w *WorkerPool) startDLQAutopurge(ctx context.Context) {
	if w.dlqTTL <= 0 {
		return
	}
	ticker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()
	_ = w.storage.PurgeExpiredDLQ(ctx, w.dlqTTL)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = w.storage.PurgeExpiredDLQ(ctx, w.dlqTTL)
		}
	}
}

func (w *WorkerPool) triggerEvent(eventType EventType, env *JobEnvelope, err error) {
	handlers := w.eventHandlers[eventType]
	if len(handlers) == 0 {
		return
	}
	event := Event{
		Type: eventType,
		Job:  env,
		Err:  err,
	}
	for _, h := range handlers {
		h(event)
	}
}

func (w *WorkerPool) runProcessingLoop(ctx context.Context, queues []string) {
	sem := make(chan struct{}, w.concurrency)
	for {
		if err := ctx.Err(); err != nil {
			w.gracefulWait()
			return
		}
		w.acquireAndProcess(ctx, sem, queues)
	}
}

func (w *WorkerPool) gracefulWait() {
	done := make(chan struct{})
	go func() {
		w.activeWorkers.Wait()
		close(done)
	}()
	select {
	case <-done:
		w.logger.Info(context.Background(), "all workers finished")
	case <-time.After(w.shutdownTimeout):
		w.logger.Error(context.Background(), "shutdown timeout reached", nil)
	}
}

func (w *WorkerPool) acquireAndProcess(ctx context.Context, sem chan struct{}, queues []string) {
	select {
	case sem <- struct{}{}:
		w.fetchAndSpawn(ctx, sem, queues)
	case <-ctx.Done():
	}
}

func (w *WorkerPool) fetchAndSpawn(ctx context.Context, sem chan struct{}, queues []string) {
	job, err := w.fetchNext(ctx, queues)
	if err != nil || job == nil {
		<-sem
		time.Sleep(100 * time.Millisecond)
		return
	}
	w.activeWorkers.Add(1)
	go w.runJobWithSemaphore(ctx, sem, job)
}

func (w *WorkerPool) runJobWithSemaphore(ctx context.Context, sem chan struct{}, env *JobEnvelope) {
	defer func() {
		<-sem
		w.activeWorkers.Done()
	}()
	w.executeJob(ctx, env)
}

func (w *WorkerPool) dequeueFromQueue(ctx context.Context, q string) (*JobEnvelope, error) {
	paused, err := w.storage.IsQueuePaused(ctx, q)
	if err == nil && paused {
		return nil, nil
	}
	return w.storage.Dequeue(ctx, q)
}

func (w *WorkerPool) checkAndDequeue(ctx context.Context, q string) (*JobEnvelope, error) {
	env, err := w.dequeueFromQueue(ctx, q)
	if err != nil {
		return nil, err
	}
	if env == nil {
		return nil, nil
	}
	if allowed := w.checkLimitsAndPostpone(ctx, env); allowed {
		return env, nil
	}
	return nil, nil
}

func (w *WorkerPool) fetchNext(ctx context.Context, queues []string) (*JobEnvelope, error) {
	if len(w.firstQueues) == 0 {
		return w.fetchStrict(ctx, queues)
	}
	searchOrder := w.buildSearchOrder(queues)
	return w.fetchFromOrder(ctx, searchOrder)
}

func (w *WorkerPool) fetchFromOrder(ctx context.Context, searchOrder []string) (*JobEnvelope, error) {
	var env *JobEnvelope
	var err error
	i := 0
	for env == nil && err == nil && i < len(searchOrder) {
		env, err = w.checkAndDequeue(ctx, searchOrder[i])
		i++
	}
	return env, err
}

func (w *WorkerPool) buildSearchOrder(queues []string) []string {
	idx := int((atomic.AddUint64(&w.fetchCounter, 1) - 1) % uint64(len(w.firstQueues)))
	primary := w.firstQueues[idx]
	order := make([]string, 0, len(queues))
	order = append(order, primary)
	for _, q := range queues {
		w.appendIfNotPrimary(&order, q, primary)
	}
	return order
}

func (w *WorkerPool) appendIfNotPrimary(order *[]string, q, primary string) {
	if q != primary {
		*order = append(*order, q)
	}
}

func (w *WorkerPool) fetchStrict(ctx context.Context, queues []string) (*JobEnvelope, error) {
	var env *JobEnvelope
	var err error
	i := 0
	for env == nil && err == nil && i < len(queues) {
		env, err = w.checkAndDequeue(ctx, queues[i])
		i++
	}
	return env, err
}

func (w *WorkerPool) checkLimitsAndPostpone(ctx context.Context, env *JobEnvelope) bool {
	cfg, exists := w.configs[env.Name]
	if !exists {
		return true
	}

	// 1. Max Concurrency check
	if cfg.maxConcurrency > 0 {
		count, err := w.storage.GetRunningJobsCount(ctx, env.Name)
		if err == nil && count > cfg.maxConcurrency {
			_ = w.storage.PostponeJob(ctx, env.JobID, env.Queue, 1*time.Second)
			return false
		}
	}

	// 2. Rate Limit check
	if cfg.rateLimit > 0 && cfg.ratePeriod > 0 {
		ok, err := w.storage.CheckRateLimit(ctx, env.Name, cfg.rateLimit, cfg.ratePeriod)
		if err == nil && !ok {
			_ = w.storage.PostponeJob(ctx, env.JobID, env.Queue, 1*time.Second)
			return false
		}
	}

	return true
}

func (w *WorkerPool) executeJob(ctx context.Context, env *JobEnvelope) {
	job, ok := w.registry[env.Name]
	if !ok {
		_ = w.storage.Fail(ctx, env.JobID, fmt.Errorf("job type not registered: name=%q", env.Name))
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

	handler := func(c context.Context, je *JobEnvelope) error {
		return job.Perform(c, je.Args)
	}
	for i := len(w.middlewares) - 1; i >= 0; i-- {
		handler = w.middlewares[i](handler)
	}
	runErr = handler(jobCtx, env)
}

func (w *WorkerPool) finalizeJob(ctx context.Context, env *JobEnvelope, start time.Time, err error) {
	duration := time.Since(start)
	w.tracer.RecordLatency(ctx, "runiq_job_duration", duration, map[string]string{"name": env.Name})
	if err != nil {
		w.finalizeJobFailure(ctx, env, err)
		return
	}
	_ = w.storage.Ack(ctx, env.JobID)
	w.tracer.IncrementCounter(ctx, "runiq_job_success", map[string]string{"name": env.Name})
	w.logger.Info(ctx, "job completed", "job_id", env.JobID)
	w.triggerEvent(EventJobCompleted, env, nil)
}

func (w *WorkerPool) finalizeJobFailure(ctx context.Context, env *JobEnvelope, err error) {
	_ = w.storage.Fail(ctx, env.JobID, err)
	w.tracer.IncrementCounter(ctx, "runiq_job_failed", map[string]string{"name": env.Name})
	w.logger.Error(ctx, "job failed", err, "job_id", env.JobID)
	maxAttempts := env.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 3
	}
	if env.Attempts+1 < maxAttempts {
		w.triggerEvent(EventJobFailed, env, err)
	} else {
		w.triggerEvent(EventJobDead, env, err)
	}
}

func createCronEnvelope(cron CronJob, now time.Time) *JobEnvelope {
	return &JobEnvelope{
		JobID:       fmt.Sprintf("cron-%s-%d", cron.Name, now.Unix()),
		Queue:       cron.Queue,
		Name:        cron.Name,
		Args:        cron.Payload,
		MaxAttempts: 3,
	}
}

func (w *WorkerPool) enqueueCronJob(ctx context.Context, cron CronJob, now time.Time) {
	ok, err := w.storage.LockCronExecution(ctx, cron.Name, now)
	if err != nil || !ok {
		if err != nil {
			w.logger.Error(ctx, "failed to check cron lock", err, "cron_name", cron.Name)
		}
		return
	}
	env := createCronEnvelope(cron, now)
	if err := w.storage.Enqueue(ctx, env); err != nil {
		w.logger.Error(ctx, "failed to enqueue cron job", err, "cron_name", cron.Name)
	} else {
		w.triggerEvent(EventJobEnqueued, env, nil)
	}
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
	m := w.mergeCronJobs(ctx)
	for _, cron := range m {
		if cron.Paused {
			continue
		}
		locNow := now
		if cron.Location != nil {
			locNow = now.In(cron.Location)
		}
		if MatchCron(cron.Spec, locNow) {
			go w.enqueueCronJob(ctx, cron, now)
		}
	}
}

func (w *WorkerPool) mergeCronJobs(ctx context.Context) map[string]CronJob {
	m := make(map[string]CronJob)
	for _, c := range w.cronJobs {
		m[c.Name] = c
	}
	dyn, err := w.storage.GetCronSchedules(ctx)
	if err != nil {
		return m
	}
	for _, c := range dyn {
		if c.Timezone != "" {
			c.Location = getCronLocation(c.Timezone)
		}
		m[c.Name] = c
	}
	return m
}

func getCronLocation(tz string) *time.Location {
	if tz == "" {
		return time.UTC
	}
	if loc, err := time.LoadLocation(tz); err == nil {
		return loc
	}
	return time.UTC
}

func (w *WorkerPool) startHeartbeat(ctx context.Context) {
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
}

func (w *WorkerPool) startScheduledPoller(ctx context.Context, queues []string) {
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
}

func (w *WorkerPool) startBatchTimeoutWatcher(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	_ = w.storage.FailExpiredBatches(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = w.storage.FailExpiredBatches(ctx)
		}
	}
}

