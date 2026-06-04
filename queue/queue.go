package queue

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// ErrDuplicateJob is returned when a job with a unique key is already queued or running.
var ErrDuplicateJob = errors.New("job with this unique key is already enqueued or running")

// TraceContext encapsulates tracing correlation metadata from orkai-observability.
type TraceContext struct {
	TraceID string `json:"trace_id"`
	SpanID  string `json:"span_id"`
}

// JobEnvelope represents the raw job payload wrapper stored in PostgreSQL/Redis.
// Fields ordered largest to smallest to minimize struct padding.
type JobEnvelope struct {
	TraceContext TraceContext  `json:"trace_context"`
	Args         []byte        `json:"args"`
	Dependencies []string      `json:"dependencies,omitempty"`
	JobID        string        `json:"job_id"`
	Queue        string        `json:"queue"`
	Name         string        `json:"name"`
	UniqueKey    string        `json:"unique_key,omitempty"`
	BatchID      string        `json:"batch_id,omitempty"`
	RunAt        *time.Time    `json:"run_at,omitempty"`
	UniqueTTL    time.Duration `json:"unique_ttl,omitempty"`
	Attempts     int           `json:"attempts"`
	MaxAttempts  int           `json:"max_attempts"`
}

// JobDetail represents the serialized metadata for listing jobs in the dashboard.
type JobDetail struct {
	JobID        string `json:"job_id"`
	Queue        string `json:"queue"`
	Name         string `json:"name"`
	Status       string `json:"status"`
	TraceID      string `json:"trace_id"`
	ErrorMessage string `json:"error_message,omitempty"`
	CreatedAt    string `json:"created_at,omitempty"`
}

// ProcessInfo represents metadata of an active worker pool process.
type ProcessInfo struct {
	HeartbeatAt    time.Time `json:"heartbeat_at"`
	Queues         []string  `json:"queues"`
	ProcessID      string    `json:"process_id"`
	Concurrency    int       `json:"concurrency"`
	MinConcurrency int       `json:"min_concurrency"`
	MaxConcurrency int       `json:"max_concurrency"`
	IsLeader       bool      `json:"is_leader"`
}

// QueueStats holds job counts grouped by queue.
type QueueStats struct {
	Name      string `json:"name"`
	Pending   int64  `json:"pending"`
	Running   int64  `json:"running"`
	Failed    int64  `json:"failed"`
	Processed int64  `json:"processed"`
	Paused    bool   `json:"paused"`
}

// CronJobDetail represents the serialized metadata for active cron jobs in the dashboard.
type CronJobDetail struct {
	Name       string `json:"name"`
	Expression string `json:"expression"`
	Queue      string `json:"queue"`
	Payload    string `json:"payload"`
	Timezone   string `json:"timezone,omitempty"`
	Source     string `json:"source,omitempty"`
	Paused     bool   `json:"paused,omitempty"`
}

// Stats holds aggregate and queue-specific job counts.
type Stats struct {
	Queues    []QueueStats    `json:"queues"`
	Jobs      []JobDetail     `json:"jobs,omitempty"`
	Processes []ProcessInfo   `json:"processes,omitempty"`
	CronJobs  []CronJobDetail `json:"cron_jobs,omitempty"`
	Pending   int64           `json:"pending"`
	Running   int64           `json:"running"`
	Failed    int64           `json:"failed"`
	Processed int64           `json:"processed"`
}

// JobQueue defines the core job lifecycle operations.
type JobQueue interface {
	// Enqueue persists a job envelope into the designated storage backend.
	// Usage example:
	//	err := storage.Enqueue(ctx, envelope)
	Enqueue(ctx context.Context, env *JobEnvelope) error

	// Dequeue fetches the next pending job from the storage backend.
	// Usage example:
	//	env, err := storage.Dequeue(ctx, "default")
	Dequeue(ctx context.Context, queue string) (*JobEnvelope, error)

	// Ack marks a job as successfully completed in storage.
	// Usage example:
	//	err := storage.Ack(ctx, "job-123")
	Ack(ctx context.Context, jobID string) error

	// Fail records a job execution failure in storage.
	// Usage example:
	//	err := storage.Fail(ctx, "job-123", err)
	Fail(ctx context.Context, jobID string, err error) error
}

// ScheduledJobQueue defines operations for scheduled and postponed jobs.
type ScheduledJobQueue interface {
	// PollScheduled moves scheduled jobs that are due into the active queue list.
	// Usage example:
	//	err := storage.PollScheduled(ctx, "default")
	PollScheduled(ctx context.Context, queue string) error

	// PostponeJob postpones a job to be executed in the future without failing it.
	PostponeJob(ctx context.Context, jobID string, queueName string, delay time.Duration) error
}

// JobStats defines statistics retrieval.
type JobStats interface {
	// GetStats retrieves the current statistics of jobs in storage.
	// Usage example:
	//	stats, err := storage.GetStats(ctx)
	GetStats(ctx context.Context) (*Stats, error)
}

// JobAdmin defines administrative operations on jobs.
type JobAdmin interface {
	// Retry resets a failed job back to pending state for re-execution.
	// Usage example:
	//	err := storage.Retry(ctx, "job-123")
	Retry(ctx context.Context, jobID string) error

	// Cancel deletes a pending, scheduled, or failed job from storage.
	// Usage example:
	//	err := storage.Cancel(ctx, "job-123")
	Cancel(ctx context.Context, jobID string) error

	// ClearQueue removes all jobs belonging to the specified queue.
	// Usage example:
	//	err := storage.ClearQueue(ctx, "default")
	ClearQueue(ctx context.Context, queue string) error

	// PauseQueue pauses processing of a specific queue.
	PauseQueue(ctx context.Context, queue string) error

	// ResumeQueue resumes processing of a specific queue.
	ResumeQueue(ctx context.Context, queue string) error

	// RetryAllFailed resets all failed (dead/failed) jobs back to pending state.
	RetryAllFailed(ctx context.Context) error

	// PurgeAllFailed permanently deletes all failed (dead/failed) jobs.
	PurgeAllFailed(ctx context.Context) error
}

// ProcessRegistry defines worker process lifecycle operations.
type ProcessRegistry interface {
	// RegisterProcess registers a worker process with its monitored queues and concurrency limit.
	RegisterProcess(ctx context.Context, info *ProcessInfo) error

	// HeartbeatProcess updates the heartbeat timestamp of a worker process.
	HeartbeatProcess(ctx context.Context, processID string) error

	// GetActiveProcesses returns all active worker processes that have recently reported heartbeats.
	GetActiveProcesses(ctx context.Context) ([]ProcessInfo, error)
}

// CronLocker defines distributed cron execution locking.
type CronLocker interface {
	// LockCronExecution attempts to acquire a unique execution lock for a cron job at a specific minute.
	LockCronExecution(ctx context.Context, cronName string, executionMinute time.Time) (bool, error)
}

// JobThrottler defines concurrency and rate limiting checks.
type JobThrottler interface {
	// GetRunningJobsCount returns the number of currently running jobs with the specified name.
	GetRunningJobsCount(ctx context.Context, jobName string) (int, error)

	// CheckRateLimit checks and increments/updates the rate limit window for a job name.
	CheckRateLimit(ctx context.Context, jobName string, limit int, period time.Duration) (bool, error)
}

// BatchStorage defines batch/workflow operations.
type BatchStorage interface {
	// CreateBatch registers a new batch record with open status and callback details.
	CreateBatch(ctx context.Context, batchID string, callback *JobEnvelope, expiresAt *time.Time) error

	// EnqueueInBatch associates a job envelope with a batch and enqueues it, incrementing batch job counts.
	EnqueueInBatch(ctx context.Context, batchID string, env *JobEnvelope) error

	// SubmitBatch seals the batch enqueuing phase and triggers callback if all jobs have already completed.
	SubmitBatch(ctx context.Context, batchID string) error
}

// WorkflowStorage defines workflow/DAG operations.
type WorkflowStorage interface {
	// EnqueueWorkflow enqueues a group of dependent jobs transactionally.
	EnqueueWorkflow(ctx context.Context, jobs ...*JobEnvelope) error
}

// Pinger defines the health check operation.
type Pinger interface {
	Ping(ctx context.Context) error
}

// WorkerPoolStorage is the storage interface required by WorkerPool.
type WorkerPoolStorage interface {
	JobQueue
	ScheduledJobQueue
	ProcessRegistry
	CronLocker
	JobThrottler
	Pinger
	JobStats
	IsQueuePaused(ctx context.Context, queue string) (bool, error)
	RegisterCronJobs(ctx context.Context, crons []CronJob) error
	PurgeExpiredDLQ(ctx context.Context, ttl time.Duration) error
	FailExpiredBatches(ctx context.Context) error
	GetCronSchedules(ctx context.Context) ([]CronJob, error)
	AcquireLeader(ctx context.Context, clientID string, ttl time.Duration) (bool, error)
	ReleaseLeader(ctx context.Context, clientID string) error
	ArchiveJobs(ctx context.Context, age time.Duration) (int64, error)
}

// ClientStorage is the storage interface required by Client.
type ClientStorage interface {
	JobQueue
	BatchStorage
	WorkflowStorage
	Pinger
}

// ServerStorage is the storage interface required by Server.
type ServerStorage interface {
	JobStats
	JobAdmin
	Pinger
	GetJobDetail(ctx context.Context, jobID string) (*JobEnvelope, error)
	GetJobs(ctx context.Context, q, status string, page, limit int) ([]JobDetail, int64, error)
	BulkRetry(ctx context.Context, jobIDs []string) error
	BulkCancel(ctx context.Context, jobIDs []string) error
	BulkPurge(ctx context.Context, jobIDs []string) error
	RetryModified(ctx context.Context, jobID string, args []byte) error
	GetCronSchedules(ctx context.Context) ([]CronJob, error)
	SaveCronSchedule(ctx context.Context, cron CronJob) error
	DeleteCronSchedule(ctx context.Context, name string) error
}

// CronJob represents a scheduled recurring task definition.
type CronJob struct {
	Payload  []byte         `json:"payload"`
	Spec     string         `json:"spec"`
	Name     string         `json:"name"`
	Queue    string         `json:"queue"`
	Timezone string         `json:"timezone,omitempty"`
	Location *time.Location `json:"-"`
	Paused   bool           `json:"paused,omitempty"`
}

// JobHandler defines the function signature for executing a job through middlewares.
type JobHandler func(ctx context.Context, env *JobEnvelope) error

// EventType represents the type of a background job event.
type EventType string

const (
	EventJobEnqueued  EventType = "JobEnqueued"
	EventJobCompleted EventType = "JobCompleted"
	EventJobFailed    EventType = "JobFailed"
	EventJobDead      EventType = "JobDead"
)

// Event contains metadata for background job lifecycles.
type Event struct {
	Type EventType
	Job  *JobEnvelope
	Err  error
}

// EventHandler is a callback function for listening to job events.
type EventHandler func(event Event)

// Job defines the contract that every background task must implement.
type Job interface {
	// Perform executes the background task logic with the provided arguments and context.
	// Usage example:
	//	err := emailJob.Perform(ctx, args)
	Perform(ctx context.Context, args []byte) error
}

// Logger defines pluggable logging interfaces for Runiq.
type Logger interface {
	Info(ctx context.Context, msg string, keysAndValues ...interface{})
	Error(ctx context.Context, msg string, err error, keysAndValues ...interface{})
}

// Tracer defines pluggable telemetry and trace-context propagation boundaries.
type Tracer interface {
	ExtractTrace(ctx context.Context) (traceID, spanID string)
	InjectTrace(ctx context.Context, traceID, spanID string) context.Context
	RecordLatency(ctx context.Context, name string, duration time.Duration, tags map[string]string)
	IncrementCounter(ctx context.Context, name string, tags map[string]string)
}

type defaultLogger struct{}

func (d *defaultLogger) Info(ctx context.Context, msg string, keysAndValues ...interface{}) {}
func (d *defaultLogger) Error(ctx context.Context, msg string, err error, keysAndValues ...interface{}) {
}

type defaultTracer struct{}

func (d *defaultTracer) ExtractTrace(ctx context.Context) (string, string) { return "", "" }
func (d *defaultTracer) InjectTrace(ctx context.Context, traceID, spanID string) context.Context {
	return ctx
}
func (d *defaultTracer) RecordLatency(ctx context.Context, name string, duration time.Duration, tags map[string]string) {
}
func (d *defaultTracer) IncrementCounter(ctx context.Context, name string, tags map[string]string) {}

// Namespacer defines the interface for storage backends that support namespaces.
type Namespacer interface {
	SetNamespace(ns string)
}

// StorageFactory defines a function that takes a connection/configuration and returns a storage driver.
type StorageFactory func(conn interface{}) (interface{}, error)

var (
	driversMu sync.RWMutex
	drivers   = make(map[string]StorageFactory)
)

// RegisterStorageDriver registers a storage factory under a specific name.
func RegisterStorageDriver(name string, factory StorageFactory) {
	driversMu.Lock()
	defer driversMu.Unlock()
	if factory == nil {
		panic("queue: RegisterStorageDriver factory is nil")
	}
	if _, dup := drivers[name]; dup {
		panic("queue: RegisterStorageDriver called twice for driver " + name)
	}
	drivers[name] = factory
}

// OpenStorage creates a new storage instance using the registered factory.
func OpenStorage(name string, conn interface{}) (interface{}, error) {
	driversMu.RLock()
	factory, ok := drivers[name]
	driversMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("queue: unknown storage driver %q (forgotten import?)", name)
	}
	return factory(conn)
}
