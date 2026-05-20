package test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/wesleyskap/orkai-runiq/queue"
)

// FakeStorage implements queue.Storage interface for testing purposes.
type FakeStorage struct {
	Enqueued      []*queue.JobEnvelope
	Acked         []string
	Failed        map[string]error
	StatsToReturn *queue.Stats
	Retried       []string
	Cancelled     []string
	Cleared       []string
	Processes     []queue.ProcessInfo
	Heartbeats    []string
}

func (f *FakeStorage) Enqueue(ctx context.Context, env *queue.JobEnvelope) error {
	f.Enqueued = append(f.Enqueued, env)
	return nil
}

func (f *FakeStorage) Dequeue(ctx context.Context, queueName string) (*queue.JobEnvelope, error) {
	if len(f.Enqueued) == 0 {
		return nil, errors.New("queue empty")
	}
	env := f.Enqueued[0]
	f.Enqueued = f.Enqueued[1:]
	return env, nil
}

func (f *FakeStorage) Ack(ctx context.Context, jobID string) error {
	f.Acked = append(f.Acked, jobID)
	return nil
}

func (f *FakeStorage) Fail(ctx context.Context, jobID string, err error) error {
	if f.Failed == nil {
		f.Failed = make(map[string]error)
	}
	f.Failed[jobID] = err
	return nil
}

func (f *FakeStorage) GetStats(ctx context.Context) (*queue.Stats, error) {
	if f.StatsToReturn != nil {
		return f.StatsToReturn, nil
	}
	return &queue.Stats{}, nil
}

func (f *FakeStorage) PollScheduled(ctx context.Context, queueName string) error {
	return nil
}

func (f *FakeStorage) Retry(ctx context.Context, jobID string) error {
	f.Retried = append(f.Retried, jobID)
	return nil
}

func (f *FakeStorage) Cancel(ctx context.Context, jobID string) error {
	f.Cancelled = append(f.Cancelled, jobID)
	return nil
}

func (f *FakeStorage) ClearQueue(ctx context.Context, queue string) error {
	f.Cleared = append(f.Cleared, queue)
	return nil
}

func (f *FakeStorage) RegisterProcess(ctx context.Context, info *queue.ProcessInfo) error {
	f.Processes = append(f.Processes, *info)
	return nil
}

func (f *FakeStorage) HeartbeatProcess(ctx context.Context, processID string) error {
	f.Heartbeats = append(f.Heartbeats, processID)
	return nil
}

func (f *FakeStorage) GetActiveProcesses(ctx context.Context) ([]queue.ProcessInfo, error) {
	return f.Processes, nil
}

func (f *FakeStorage) LockCronExecution(ctx context.Context, cronName string, executionMinute time.Time) (bool, error) {
	return true, nil
}

// DummyJob implements queue.Job for verification.
type DummyJob struct {
	Executed bool
}

func (d *DummyJob) Perform(ctx context.Context, args []byte) error {
	d.Executed = true
	return nil
}

// TestJobEnvelopeSerialization validates JSON marshalling/unmarshalling of Runiq envelopes.
// Usage example:
//
//	go test -v ./...
func TestJobEnvelopeSerialization(t *testing.T) {
	env := &queue.JobEnvelope{
		JobID: "job-123",
		Name:  "dummy",
		Args:  []byte(`{"email":"test@orkai.com"}`),
		TraceContext: queue.TraceContext{
			TraceID: "4bf92f3577b34da6a3ce929d0e0e4736",
			SpanID:  "00f067aa0ba902b7",
		},
	}

	data, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("failed to marshal job envelope: %v", err)
	}

	var decoded queue.JobEnvelope
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("failed to unmarshal job envelope: %v", err)
	}

	if decoded.JobID != env.JobID {
		t.Errorf("expected JobID %q, got %q", env.JobID, decoded.JobID)
	}
	if decoded.TraceContext.TraceID != env.TraceContext.TraceID {
		t.Errorf("expected TraceID %q, got %q", env.TraceContext.TraceID, decoded.TraceContext.TraceID)
	}
}

// TestJobInterfaceConformance checks interface binding of registered tasks.
// Usage example:
//
//	go test -v ./...
func TestJobInterfaceConformance(t *testing.T) {
	var job queue.Job = &DummyJob{}
	ctx := context.Background()
	err := job.Perform(ctx, []byte(`{}`))
	if err != nil {
		t.Fatalf("perform failed: %v", err)
	}

	dummy, ok := job.(*DummyJob)
	if !ok || !dummy.Executed {
		t.Errorf("expected job to be executed")
	}
}
