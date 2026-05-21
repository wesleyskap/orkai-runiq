package queue

import (
	"context"
	"errors"
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
	Queues      []string  `json:"queues"`
	HeartbeatAt time.Time `json:"heartbeat_at"`
	ProcessID   string    `json:"process_id"`
	Concurrency int       `json:"concurrency"`
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
	CreateBatch(ctx context.Context, batchID string, callback *JobEnvelope) error

	// EnqueueInBatch associates a job envelope with a batch and enqueues it, incrementing batch job counts.
	EnqueueInBatch(ctx context.Context, batchID string, env *JobEnvelope) error

	// SubmitBatch seals the batch enqueuing phase and triggers callback if all jobs have already completed.
	SubmitBatch(ctx context.Context, batchID string) error
}

// WorkerPoolStorage is the storage interface required by WorkerPool.
type WorkerPoolStorage interface {
	JobQueue
	ScheduledJobQueue
	ProcessRegistry
	CronLocker
	JobThrottler
	IsQueuePaused(ctx context.Context, queue string) (bool, error)
	RegisterCronJobs(ctx context.Context, crons []CronJob) error
}

// ClientStorage is the storage interface required by Client.
type ClientStorage interface {
	JobQueue
	BatchStorage
}

// ServerStorage is the storage interface required by Server.
type ServerStorage interface {
	JobStats
	JobAdmin
	GetJobDetail(ctx context.Context, jobID string) (*JobEnvelope, error)
}

// CronJob represents a scheduled recurring task definition.
type CronJob struct {
	Payload []byte `json:"payload"`
	Spec    string `json:"spec"`
	Name    string `json:"name"`
	Queue   string `json:"queue"`
}

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
func (d *defaultLogger) Error(ctx context.Context, msg string, err error, keysAndValues ...interface{}) {}

type defaultTracer struct{}

func (d *defaultTracer) ExtractTrace(ctx context.Context) (string, string) { return "", "" }
func (d *defaultTracer) InjectTrace(ctx context.Context, traceID, spanID string) context.Context { return ctx }
func (d *defaultTracer) RecordLatency(ctx context.Context, name string, duration time.Duration, tags map[string]string) {}
func (d *defaultTracer) IncrementCounter(ctx context.Context, name string, tags map[string]string) {}

