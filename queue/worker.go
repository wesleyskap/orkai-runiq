package queue

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// IntervalJob represents a background task that repeats after a simple duration.
type IntervalJob struct {
	Payload  []byte
	Interval time.Duration
	Name     string
	Queue    string
}

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
	firstQueues        []string
	monitoredQueues    []string
	cronJobs           []CronJob
	middlewares        []func(JobHandler) JobHandler
	activeWorkers      sync.WaitGroup
	storage            WorkerPoolStorage
	logger             Logger
	tracer             Tracer
	processID          string
	concurrencyMutex   sync.Mutex
	autoscale          *DynamicConcurrencyConfig
	sem                chan struct{}
	registry           map[string]Job
	configs            map[string]handlerConfig
	weights            map[string]int
	eventHandlers      map[EventType][]EventHandler
	fetchCounter       uint64
	concurrency        int
	currentConcurrency int
	shutdownTimeout    time.Duration
	dlqTTL             time.Duration
	leaderTTL          time.Duration
	archivalAge        time.Duration
	archivalInterval   time.Duration
	isLeader           int32
	encryptionKey      []byte
	intervalJobs       []IntervalJob
	lastIntervalTicks  map[string]int64
	intervalMutex      sync.Mutex
	tags               []string
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
		storage:           storage,
		logger:            &defaultLogger{},
		tracer:            &defaultTracer{},
		processID:         getWorkerProcessID(),
		registry:          make(map[string]Job),
		configs:           make(map[string]handlerConfig),
		weights:           make(map[string]int),
		concurrency:       concurrency,
		shutdownTimeout:   10 * time.Second,
		eventHandlers:     make(map[EventType][]EventHandler),
		lastIntervalTicks: make(map[string]int64),
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

// WithWorkerTags configures matching capability tags for the worker pool.
func WithWorkerTags(tags ...string) WorkerOption {
	return func(w *WorkerPool) {
		w.tags = tags
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

// WithLeaderElection enables native leader election for background loops.
func WithLeaderElection(ttl time.Duration) WorkerOption {
	return func(w *WorkerPool) {
		w.leaderTTL = ttl
	}
}

// WithJobArchival enables periodic archival of old processed/dead jobs.
func WithJobArchival(age, interval time.Duration) WorkerOption {
	return func(w *WorkerPool) {
		w.archivalAge = age
		w.archivalInterval = interval
	}
}

// WithWorkerPayloadEncryption configures a custom key for decrypting job arguments.
func WithWorkerPayloadEncryption(key []byte) WorkerOption {
	return func(w *WorkerPool) {
		w.encryptionKey = key
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

// RegisterInterval registers a recurring task under the specified duration interval.
func (w *WorkerPool) RegisterInterval(interval time.Duration, queueName, name string, payload []byte) {
	w.intervalJobs = append(w.intervalJobs, IntervalJob{
		Payload:  payload,
		Interval: interval,
		Name:     name,
		Queue:    queueName,
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
	if w.leaderTTL > 0 {
		defer func() {
			ctxShut, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_ = w.storage.ReleaseLeader(ctxShut, w.processID)
		}()
	}
	w.monitoredQueues = queues
	w.setupSemaphore()
	w.setupWeightedQueues(queues)
	w.registerProcess(ctx, queues)
	if len(w.cronJobs) > 0 {
		_ = w.storage.RegisterCronJobs(ctx, w.cronJobs)
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
	conc := w.concurrency
	var minC, maxC int
	if w.autoscale != nil {
		conc = w.getCurrentConcurrency()
		minC = w.autoscale.MinConcurrency
		maxC = w.autoscale.MaxConcurrency
	}
	info := &ProcessInfo{
		Queues:         queues,
		HeartbeatAt:    time.Now(),
		ProcessID:      w.processID,
		Concurrency:    conc,
		MinConcurrency: minC,
		MaxConcurrency: maxC,
	}
	if err := w.storage.RegisterProcess(ctx, info); err != nil {
		w.logger.Error(ctx, "failed to register worker process", err, "process_id", w.processID)
	}
}

func (w *WorkerPool) startBackgroundLoops(ctx context.Context, queues []string) {
	go w.startHeartbeat(ctx)
	if w.leaderTTL > 0 {
		go w.startLeaderElector(ctx)
	}
	if len(w.cronJobs) > 0 {
		go w.startCronScheduler(ctx)
	}
	go w.startScheduledPoller(ctx, queues)
	go w.startDLQAutopurge(ctx)
	go w.startBatchTimeoutWatcher(ctx)
	if w.archivalAge > 0 && w.archivalInterval > 0 {
		go w.startJobArchiver(ctx)
	}
	if len(w.intervalJobs) > 0 {
		go w.startIntervalScheduler(ctx)
	}
	if w.autoscale != nil {
		go w.startAutoscaler(ctx)
	}
}

func (w *WorkerPool) startDLQAutopurge(ctx context.Context) {
	if w.dlqTTL <= 0 {
		return
	}
	ticker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()
	w.purgeDLQIfLeader(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.purgeDLQIfLeader(ctx)
		}
	}
}

func (w *WorkerPool) purgeDLQIfLeader(ctx context.Context) {
	if w.checkLeader() {
		_ = w.storage.PurgeExpiredDLQ(ctx, w.dlqTTL)
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
	for {
		if err := ctx.Err(); err != nil {
			w.gracefulWait()
			return
		}
		w.acquireAndProcess(ctx, queues)
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

func (w *WorkerPool) acquireAndProcess(ctx context.Context, queues []string) {
	select {
	case w.sem <- struct{}{}:
		w.fetchAndSpawn(ctx, queues)
	case <-ctx.Done():
	}
}

func (w *WorkerPool) fetchAndSpawn(ctx context.Context, queues []string) {
	job, err := w.fetchNext(ctx, queues)
	if err != nil || job == nil {
		<-w.sem
		time.Sleep(100 * time.Millisecond)
		return
	}
	w.activeWorkers.Add(1)
	go w.runJobWithSemaphore(ctx, job)
}

func (w *WorkerPool) runJobWithSemaphore(ctx context.Context, env *JobEnvelope) {
	defer func() {
		<-w.sem
		w.activeWorkers.Done()
	}()
	w.executeJob(ctx, env)
}

func (w *WorkerPool) dequeueFromQueue(ctx context.Context, q string) (*JobEnvelope, error) {
	paused, err := w.storage.IsQueuePaused(ctx, q)
	if err == nil && paused {
		return nil, nil
	}
	return w.storage.Dequeue(ctx, q, w.tags)
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
	if IsEncrypted(env.Args) {
		if len(w.encryptionKey) == 0 {
			_ = w.storage.Fail(ctx, env.JobID, fmt.Errorf("payload is encrypted but worker has no key configured"))
			return
		}
		dec, err := DecryptPayload(env.Args, w.encryptionKey)
		if err != nil {
			_ = w.storage.Fail(ctx, env.JobID, fmt.Errorf("failed to decrypt payload: %w", err))
			return
		}
		env.Args = dec
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

func (w *WorkerPool) EnqueueCronJob(ctx context.Context, cron CronJob, now time.Time) {
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
			if w.checkLeader() {
				now := time.Now().UTC()
				if now.Minute() != lastMinute {
					lastMinute = now.Minute()
					w.ProcessCronJobs(ctx, now)
				}
			}
		}
	}
}

func (w *WorkerPool) ProcessCronJobs(ctx context.Context, now time.Time) {
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
			go w.EnqueueCronJob(ctx, cron, now)
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
			w.registerProcess(ctx, w.monitoredQueues)
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
			w.pollQueuesScheduled(ctx, queues)
		}
	}
}

func (w *WorkerPool) pollQueuesScheduled(ctx context.Context, queues []string) {
	if !w.checkLeader() {
		return
	}
	for _, q := range queues {
		_ = w.storage.PollScheduled(ctx, q)
	}
}

func (w *WorkerPool) startBatchTimeoutWatcher(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	w.failExpiredBatchesIfLeader(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.failExpiredBatchesIfLeader(ctx)
		}
	}
}

func (w *WorkerPool) failExpiredBatchesIfLeader(ctx context.Context) {
	if w.checkLeader() {
		_ = w.storage.FailExpiredBatches(ctx)
	}
}

func (w *WorkerPool) checkLeader() bool {
	if w.leaderTTL <= 0 {
		return true
	}
	return atomic.LoadInt32(&w.isLeader) == 1
}

func (w *WorkerPool) startLeaderElector(ctx context.Context) {
	ticker := time.NewTicker(w.leaderTTL / 2)
	defer ticker.Stop()
	w.electLeader(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.electLeader(ctx)
		}
	}
}

func (w *WorkerPool) electLeader(ctx context.Context) {
	ok, err := w.storage.AcquireLeader(ctx, w.processID, w.leaderTTL)
	if err != nil {
		w.logger.Error(ctx, "leader elector: failed to acquire/renew lease", err)
		atomic.StoreInt32(&w.isLeader, 0)
		return
	}
	if ok {
		if atomic.CompareAndSwapInt32(&w.isLeader, 0, 1) {
			w.logger.Info(ctx, "leader elector: successfully elected as leader", "process_id", w.processID)
		}
	} else {
		if atomic.CompareAndSwapInt32(&w.isLeader, 1, 0) {
			w.logger.Info(ctx, "leader elector: lost leader status", "process_id", w.processID)
		}
	}
}

func (w *WorkerPool) startJobArchiver(ctx context.Context) {
	ticker := time.NewTicker(w.archivalInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.runArchival(ctx)
		}
	}
}

func (w *WorkerPool) runArchival(ctx context.Context) {
	if !w.checkLeader() {
		return
	}
	count, err := w.storage.ArchiveJobs(ctx, w.archivalAge)
	if err != nil {
		w.logger.Error(ctx, "job archiver failed", err)
	} else if count > 0 {
		w.logger.Info(ctx, "job archiver completed", "count", count)
	}
}

func (w *WorkerPool) startIntervalScheduler(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if w.checkLeader() {
				w.ProcessIntervalJobs(ctx, time.Now().UTC())
			}
		}
	}
}

// ProcessIntervalJobs processes all registered interval jobs.
func (w *WorkerPool) ProcessIntervalJobs(ctx context.Context, now time.Time) {
	for _, job := range w.intervalJobs {
		intervalSec := int64(job.Interval.Seconds())
		if intervalSec <= 0 {
			continue
		}
		blockIdx := now.Unix() / intervalSec
		w.intervalMutex.Lock()
		lastIdx := w.lastIntervalTicks[job.Name]
		if blockIdx > lastIdx {
			w.lastIntervalTicks[job.Name] = blockIdx
			w.intervalMutex.Unlock()
			go w.EnqueueIntervalJob(ctx, job, time.Unix(blockIdx*intervalSec, 0).UTC())
		} else {
			w.intervalMutex.Unlock()
		}
	}
}

// EnqueueIntervalJob enqueues a single interval job.
func (w *WorkerPool) EnqueueIntervalJob(ctx context.Context, job IntervalJob, blockTime time.Time) {
	ok, err := w.storage.LockCronExecution(ctx, job.Name, blockTime)
	if err != nil || !ok {
		return
	}
	env := &JobEnvelope{
		JobID:       fmt.Sprintf("interval-%s-%d", job.Name, blockTime.Unix()),
		Queue:       job.Queue,
		Name:        job.Name,
		Args:        job.Payload,
		MaxAttempts: 3,
	}
	if err := w.storage.Enqueue(ctx, env); err == nil {
		w.triggerEvent(EventJobEnqueued, env, nil)
	}
}
