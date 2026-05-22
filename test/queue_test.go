package test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/wesleyskap/orkai-runiq/v2/queue"
)

// FakeStorage implements queue.WorkerPoolStorage and queue.ClientStorage for testing purposes.
type FakeStorage struct {
	Enqueued             []*queue.JobEnvelope
	Acked                []string
	Failed               map[string]error
	StatsToReturn        *queue.Stats
	Retried              []string
	Cancelled            []string
	Cleared              []string
	Processes            []queue.ProcessInfo
	Heartbeats           []string
	RunningCountToReturn map[string]int
	RateLimitToReturn    map[string]bool
	Postponed            []string
	PausedQueues         map[string]bool
	ModifiedRetries      map[string][]byte
	CronSchedules        []queue.CronJob
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

func (f *FakeStorage) GetRunningJobsCount(ctx context.Context, jobName string) (int, error) {
	if f.RunningCountToReturn != nil {
		return f.RunningCountToReturn[jobName], nil
	}
	return 0, nil
}

func (f *FakeStorage) CheckRateLimit(ctx context.Context, jobName string, limit int, period time.Duration) (bool, error) {
	if f.RateLimitToReturn != nil {
		if val, ok := f.RateLimitToReturn[jobName]; ok {
			return val, nil
		}
	}
	return true, nil
}

func (f *FakeStorage) PostponeJob(ctx context.Context, jobID string, queueName string, delay time.Duration) error {
	f.Postponed = append(f.Postponed, jobID)
	return nil
}

func (f *FakeStorage) CreateBatch(ctx context.Context, batchID string, callback *queue.JobEnvelope, expiresAt *time.Time) error {
	return nil
}

func (f *FakeStorage) EnqueueInBatch(ctx context.Context, batchID string, env *queue.JobEnvelope) error {
	f.Enqueued = append(f.Enqueued, env)
	return nil
}

func (f *FakeStorage) SubmitBatch(ctx context.Context, batchID string) error {
	return nil
}

func (f *FakeStorage) EnqueueWorkflow(ctx context.Context, jobs ...*queue.JobEnvelope) error {
	f.Enqueued = append(f.Enqueued, jobs...)
	return nil
}

func (f *FakeStorage) IsQueuePaused(ctx context.Context, queue string) (bool, error) {
	if f.PausedQueues == nil {
		return false, nil
	}
	return f.PausedQueues[queue], nil
}

func (f *FakeStorage) PauseQueue(ctx context.Context, queue string) error {
	if f.PausedQueues == nil {
		f.PausedQueues = make(map[string]bool)
	}
	f.PausedQueues[queue] = true
	return nil
}

func (f *FakeStorage) ResumeQueue(ctx context.Context, queue string) error {
	if f.PausedQueues == nil {
		return nil
	}
	f.PausedQueues[queue] = false
	return nil
}

func (f *FakeStorage) GetJobDetail(ctx context.Context, jobID string) (*queue.JobEnvelope, error) {
	for _, env := range f.Enqueued {
		if env.JobID == jobID {
			return env, nil
		}
	}
	return nil, nil
}

func (f *FakeStorage) RetryAllFailed(ctx context.Context) error {
	// RetryAllFailed is a mock implementation for tests.
	_ = ctx
	return nil
}

func (f *FakeStorage) PurgeAllFailed(ctx context.Context) error {
	// PurgeAllFailed is a mock implementation for tests.
	_ = ctx
	return nil
}

func (f *FakeStorage) RegisterCronJobs(ctx context.Context, crons []queue.CronJob) error {
	// RegisterCronJobs is a mock implementation for tests.
	_ = crons
	return nil
}

func (f *FakeStorage) Ping(ctx context.Context) error {
	return nil
}

func (f *FakeStorage) PurgeExpiredDLQ(ctx context.Context, ttl time.Duration) error {
	return nil
}

func (f *FakeStorage) FailExpiredBatches(ctx context.Context) error {
	return nil
}

func (f *FakeStorage) GetJobs(ctx context.Context, q, status string, page, limit int) ([]queue.JobDetail, int64, error) {
	_ = ctx
	_ = q
	return nil, 0, nil
}

func (f *FakeStorage) BulkRetry(ctx context.Context, jobIDs []string) error {
	_ = ctx
	f.Retried = append(f.Retried, jobIDs...)
	return nil
}

func (f *FakeStorage) BulkCancel(ctx context.Context, jobIDs []string) error {
	_ = ctx
	f.Cancelled = append(f.Cancelled, jobIDs...)
	return nil
}

func (f *FakeStorage) BulkPurge(ctx context.Context, jobIDs []string) error {
	_ = ctx
	_ = jobIDs
	return nil
}

func (f *FakeStorage) RetryModified(ctx context.Context, jobID string, args []byte) error {
	if f.ModifiedRetries == nil {
		f.ModifiedRetries = make(map[string][]byte)
	}
	f.ModifiedRetries[jobID] = args
	return nil
}

func (f *FakeStorage) GetCronSchedules(ctx context.Context) ([]queue.CronJob, error) {
	return f.CronSchedules, nil
}

func (f *FakeStorage) SaveCronSchedule(ctx context.Context, cron queue.CronJob) error {
	for i, existing := range f.CronSchedules {
		if existing.Name == cron.Name {
			f.CronSchedules[i] = cron
			return nil
		}
	}
	f.CronSchedules = append(f.CronSchedules, cron)
	return nil
}

func (f *FakeStorage) DeleteCronSchedule(ctx context.Context, name string) error {
	var kept []queue.CronJob
	for _, cron := range f.CronSchedules {
		if cron.Name != name {
			kept = append(kept, cron)
		}
	}
	f.CronSchedules = kept
	return nil
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
