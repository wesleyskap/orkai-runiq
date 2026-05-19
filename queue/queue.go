package queue

import (
	"context"
	"time"
)

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

