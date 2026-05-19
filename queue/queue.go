package queue

import "context"

// TraceContext encapsulates tracing correlation metadata from orkai-observability.
type TraceContext struct {
	TraceID string `json:"trace_id"`
	SpanID  string `json:"span_id"`
}

// JobEnvelope represents the raw job payload wrapper stored in PostgreSQL/Redis.
type JobEnvelope struct {
	JobID        string       `json:"job_id"`
	Queue        string       `json:"queue"`
	Name         string       `json:"name"`
	Args         []byte       `json:"args"`
	TraceContext TraceContext `json:"trace_context"`
}

// Storage defines the persistence engine interface for enqueuing and processing tasks.
type Storage interface {
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

// Job defines the contract that every background task must implement.
type Job interface {
	// Perform executes the background task logic with the provided arguments and context.
	// Usage example:
	//	err := emailJob.Perform(ctx, args)
	Perform(ctx context.Context, args []byte) error
}
