package queue

import (
	"context"
	"errors"
)

// Batch represents a workflow group of jobs that triggers a callback on completion.
type Batch struct {
	ID       string
	client   *Client
	callback *JobEnvelope
}

// NewBatch initializes a new batch of jobs with a callback job.
// Usage example:
//	batch, err := client.NewBatch(ctx, &queue.JobEnvelope{
//		Queue: "default",
//		Name:  "OnSuccessCallback",
//		Args:  []byte(`{}`),
//	})
func (c *Client) NewBatch(ctx context.Context, callback *JobEnvelope) (*Batch, error) {
	if callback == nil {
		return nil, errors.New("callback job envelope cannot be nil")
	}
	if callback.Queue == "" || callback.Name == "" {
		return nil, errors.New("callback must specify queue and name")
	}

	batchID := generateJobID()
	if err := c.storage.CreateBatch(ctx, batchID, callback); err != nil {
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
