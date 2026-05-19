package queue

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"time"
)

// Client enqueues background jobs.
type Client struct {
	storage Storage
	tracer  Tracer
}

// NewClient creates a new Client instance.
// Usage example:
//	client := queue.NewClient(storage, queue.WithClientTracer(tracer))
func NewClient(storage Storage, opts ...ClientOption) *Client {
	c := &Client{
		storage: storage,
		tracer:  &defaultTracer{},
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// ClientOption defines functional configuration options for Client.
type ClientOption func(*Client)

// WithClientTracer configures a custom Tracer for the Client.
func WithClientTracer(t Tracer) ClientOption {
	return func(c *Client) {
		c.tracer = t
	}
}

// Enqueue serializes and schedules a background job.
func (c *Client) Enqueue(ctx context.Context, queueName, name string, args []byte) error {
	traceID, spanID := c.tracer.ExtractTrace(ctx)
	env := &JobEnvelope{
		JobID:       generateJobID(),
		Queue:       queueName,
		Name:        name,
		Args:        args,
		MaxAttempts: 3,
		TraceContext: TraceContext{
			TraceID: traceID,
			SpanID:  spanID,
		},
	}
	return c.storage.Enqueue(ctx, env)
}

// EnqueueIn schedules a job to be executed after a duration delay.
func (c *Client) EnqueueIn(ctx context.Context, queueName, name string, args []byte, delay time.Duration) error {
	runAt := time.Now().Add(delay)
	return c.EnqueueAt(ctx, queueName, name, args, runAt)
}

// EnqueueAt schedules a job to be executed at a specific time.
func (c *Client) EnqueueAt(ctx context.Context, queueName, name string, args []byte, runAt time.Time) error {
	traceID, spanID := c.tracer.ExtractTrace(ctx)
	env := &JobEnvelope{
		JobID:       generateJobID(),
		Queue:       queueName,
		Name:        name,
		Args:        args,
		RunAt:       &runAt,
		MaxAttempts: 3,
		TraceContext: TraceContext{
			TraceID: traceID,
			SpanID:  spanID,
		},
	}
	return c.storage.Enqueue(ctx, env)
}

func generateJobID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
