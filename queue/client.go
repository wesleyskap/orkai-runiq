package queue

import (
	"context"
	"time"
)

// Client enqueues background jobs.
type Client struct {
	storage ClientStorage
	tracer  Tracer
	cb      *circuitBreaker
}

// NewClient creates a new Client instance.
// Usage example:
//	client := queue.NewClient(storage, queue.WithClientTracer(tracer))
func NewClient(storage ClientStorage, opts ...ClientOption) *Client {
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

// WithCircuitBreaker configures a Client-Side Circuit Breaker for the Client.
func WithCircuitBreaker(cfg CircuitBreakerConfig) ClientOption {
	return func(c *Client) {
		c.cb = newCircuitBreaker(cfg)
	}
}

// WithNamespace configures a custom namespace prefix for the Client's storage.
func WithNamespace(ns string) ClientOption {
	return func(c *Client) {
		if nsStore, ok := c.storage.(Namespacer); ok {
			nsStore.SetNamespace(ns)
		}
	}
}

func (c *Client) execute(fn func() error) error {
	if c.cb == nil {
		return fn()
	}
	if err := c.cb.beforeCall(); err != nil {
		return err
	}
	start := time.Now()
	err := fn()
	c.cb.afterCall(err, time.Since(start))
	return err
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
	return c.execute(func() error {
		return c.storage.Enqueue(ctx, env)
	})
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
	return c.execute(func() error {
		return c.storage.Enqueue(ctx, env)
	})
}

// EnqueueUnique schedules a unique job that is protected by a uniqueness lock.
func (c *Client) EnqueueUnique(ctx context.Context, queueName, name string, args []byte, uniqueKey string, uniqueTTL time.Duration) error {
	traceID, spanID := c.tracer.ExtractTrace(ctx)
	env := &JobEnvelope{
		JobID:       generateJobID(),
		Queue:       queueName,
		Name:        name,
		Args:        args,
		MaxAttempts: 3,
		UniqueKey:   uniqueKey,
		UniqueTTL:   uniqueTTL,
		TraceContext: TraceContext{
			TraceID: traceID,
			SpanID:  spanID,
		},
	}
	return c.execute(func() error {
		return c.storage.Enqueue(ctx, env)
	})
}

// NewJob instantiates a JobEnvelope with a pre-generated ID.
// Usage example:
//	job := queue.NewJob("default", "UploadData", payload)
func NewJob(queueName, name string, args []byte) *JobEnvelope {
	return &JobEnvelope{
		JobID:       generateJobID(),
		Queue:       queueName,
		Name:        name,
		Args:        args,
		MaxAttempts: 3,
	}
}

// DependsOn adds parentJobID dependencies to the job.
// Usage example:
//	child.DependsOn(parent)
func (env *JobEnvelope) DependsOn(parent *JobEnvelope) {
	if parent != nil && parent.JobID != "" {
		env.Dependencies = append(env.Dependencies, parent.JobID)
	}
}

// EnqueueWorkflow enqueues multiple dependent jobs transactionally.
// Usage example:
//	err := client.EnqueueWorkflow(ctx, jobA, jobB)
func (c *Client) EnqueueWorkflow(ctx context.Context, jobs ...*JobEnvelope) error {
	for _, job := range jobs {
		traceID, spanID := c.tracer.ExtractTrace(ctx)
		if job.TraceContext.TraceID == "" {
			job.TraceContext.TraceID = traceID
			job.TraceContext.SpanID = spanID
		}
	}
	return c.execute(func() error {
		return c.storage.EnqueueWorkflow(ctx, jobs...)
	})
}

// EnqueueWithDelay schedules a job to be executed after a relative duration delay.
// Usage example:
//	err := client.EnqueueWithDelay(ctx, "default", "SendReminder", payload, 30*time.Minute)
func (c *Client) EnqueueWithDelay(ctx context.Context, queueName, name string, args []byte, delay time.Duration) error {
	return c.EnqueueIn(ctx, queueName, name, args, delay)
}



