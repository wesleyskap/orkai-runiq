package queue

import (
	"context"
	"errors"
	"time"
)

// BatchOption configures optional parameters for a job batch.
type BatchOption func(*batchOptions)

type batchOptions struct {
	timeout time.Duration
}

// WithBatchTimeout sets a maximum execution duration for the batch.
func WithBatchTimeout(timeout time.Duration) BatchOption {
	return func(o *batchOptions) {
		o.timeout = timeout
	}
}

// Batch represents a workflow group of jobs that triggers a callback on completion.
type Batch struct {
	ID       string
	client   *Client
	callback *JobEnvelope
}

func validateCallback(cb *JobEnvelope) error {
	if cb == nil {
		return errors.New("callback job envelope cannot be nil")
	}
	if cb.Queue == "" || cb.Name == "" {
		return errors.New("callback must specify queue and name")
	}
	return nil
}

// NewBatch initializes a new batch of jobs with a callback job.
// Usage example:
//	batch, err := client.NewBatch(ctx, &queue.JobEnvelope{
//		Queue: "default",
//		Name:  "OnSuccessCallback",
//		Args:  []byte(`{}`),
//	})
func (c *Client) NewBatch(ctx context.Context, callback *JobEnvelope, opts ...BatchOption) (*Batch, error) {
	if err := validateCallback(callback); err != nil {
		return nil, err
	}
	var cfg batchOptions
	for _, opt := range opts {
		opt(&cfg)
	}
	var expiresAt *time.Time
	if cfg.timeout > 0 {
		t := time.Now().Add(cfg.timeout)
		expiresAt = &t
	}
	batchID := generateJobID()
	if err := c.storage.CreateBatch(ctx, batchID, callback, expiresAt); err != nil {
		return nil, err
	}
	return &Batch{
		ID:       batchID,
		client:   c,
		callback: callback,
	}, nil
}

// Enqueue adds a job to the batch and enqueues it immediately in the storage backend.
// Usage example:
//	err := batch.Enqueue(ctx, "default", "ProcessSegment", segment1)
func (b *Batch) Enqueue(ctx context.Context, queueName, name string, args []byte) error {
	traceID, spanID := b.client.tracer.ExtractTrace(ctx)
	env := &JobEnvelope{
		JobID:       generateJobID(),
		Queue:       queueName,
		Name:        name,
		Args:        args,
		MaxAttempts: 3,
		BatchID:     b.ID,
		TraceContext: TraceContext{
			TraceID: traceID,
			SpanID:  spanID,
		},
	}
	return b.client.storage.EnqueueInBatch(ctx, b.ID, env)
}

// Submit seals the batch enqueuing phase, marking it ready to trigger the callback when done.
// Usage example:
//	err := batch.Submit(ctx)
func (b *Batch) Submit(ctx context.Context) error {
	return b.client.storage.SubmitBatch(ctx, b.ID)
}
