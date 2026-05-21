package queuetest

import (
	"context"
	"time"

	"github.com/wesleyskap/orkai-runiq/v2/queue"
)

// FakeClientStorage implements queue.ClientStorage.
type FakeClientStorage struct {
	EnqueueFunc        func(ctx context.Context, env *queue.JobEnvelope) error
	DequeueFunc        func(ctx context.Context, queueName string) (*queue.JobEnvelope, error)
	AckFunc            func(ctx context.Context, jobID string) error
	FailFunc           func(ctx context.Context, jobID string, err error) error
	CreateBatchFunc    func(ctx context.Context, batchID string, callback *queue.JobEnvelope) error
	EnqueueInBatchFunc func(ctx context.Context, batchID string, env *queue.JobEnvelope) error
	SubmitBatchFunc    func(ctx context.Context, batchID string) error
	PingFunc           func(ctx context.Context) error
}

func (f *FakeClientStorage) Enqueue(ctx context.Context, env *queue.JobEnvelope) error {
	if f.EnqueueFunc != nil {
		return f.EnqueueFunc(ctx, env)
	}
	return nil
}

func (f *FakeClientStorage) Dequeue(ctx context.Context, queueName string) (*queue.JobEnvelope, error) {
	if f.DequeueFunc != nil {
		return f.DequeueFunc(ctx, queueName)
	}
	return nil, nil
}

func (f *FakeClientStorage) Ack(ctx context.Context, jobID string) error {
	if f.AckFunc != nil {
		return f.AckFunc(ctx, jobID)
	}
	return nil
}

func (f *FakeClientStorage) Fail(ctx context.Context, jobID string, err error) error {
	if f.FailFunc != nil {
		return f.FailFunc(ctx, jobID, err)
	}
	return nil
}

func (f *FakeClientStorage) CreateBatch(ctx context.Context, batchID string, callback *queue.JobEnvelope) error {
	if f.CreateBatchFunc != nil {
		return f.CreateBatchFunc(ctx, batchID, callback)
	}
	return nil
}

func (f *FakeClientStorage) EnqueueInBatch(ctx context.Context, batchID string, env *queue.JobEnvelope) error {
	if f.EnqueueInBatchFunc != nil {
		return f.EnqueueInBatchFunc(ctx, batchID, env)
	}
	return nil
}

func (f *FakeClientStorage) SubmitBatch(ctx context.Context, batchID string) error {
	if f.SubmitBatchFunc != nil {
		return f.SubmitBatchFunc(ctx, batchID)
	}
	return nil
}

func (f *FakeClientStorage) Ping(ctx context.Context) error {
	if f.PingFunc != nil {
		return f.PingFunc(ctx)
	}
	return nil
}

// FakeServerStorage implements queue.ServerStorage.
type FakeServerStorage struct {
	GetStatsFunc       func(ctx context.Context) (*queue.Stats, error)
	RetryFunc          func(ctx context.Context, jobID string) error
	CancelFunc         func(ctx context.Context, jobID string) error
	ClearQueueFunc     func(ctx context.Context, queueName string) error
	PauseQueueFunc     func(ctx context.Context, queueName string) error
	ResumeQueueFunc    func(ctx context.Context, queueName string) error
	RetryAllFailedFunc func(ctx context.Context) error
	PurgeAllFailedFunc func(ctx context.Context) error
	PingFunc           func(ctx context.Context) error
	GetJobDetailFunc   func(ctx context.Context, jobID string) (*queue.JobEnvelope, error)
}

func (f *FakeServerStorage) GetStats(ctx context.Context) (*queue.Stats, error) {
	if f.GetStatsFunc != nil {
		return f.GetStatsFunc(ctx)
	}
	return &queue.Stats{}, nil
}

func (f *FakeServerStorage) Retry(ctx context.Context, jobID string) error {
	if f.RetryFunc != nil {
		return f.RetryFunc(ctx, jobID)
	}
	return nil
}

func (f *FakeServerStorage) Cancel(ctx context.Context, jobID string) error {
	if f.CancelFunc != nil {
		return f.CancelFunc(ctx, jobID)
	}
	return nil
}

func (f *FakeServerStorage) ClearQueue(ctx context.Context, queueName string) error {
	if f.ClearQueueFunc != nil {
		return f.ClearQueueFunc(ctx, queueName)
	}
	return nil
}

func (f *FakeServerStorage) PauseQueue(ctx context.Context, queueName string) error {
	if f.PauseQueueFunc != nil {
		return f.PauseQueueFunc(ctx, queueName)
	}
	return nil
}

func (f *FakeServerStorage) ResumeQueue(ctx context.Context, queueName string) error {
	if f.ResumeQueueFunc != nil {
		return f.ResumeQueueFunc(ctx, queueName)
	}
	return nil
}

func (f *FakeServerStorage) RetryAllFailed(ctx context.Context) error {
	if f.RetryAllFailedFunc != nil {
		return f.RetryAllFailedFunc(ctx)
	}
	return nil
}

func (f *FakeServerStorage) PurgeAllFailed(ctx context.Context) error {
	if f.PurgeAllFailedFunc != nil {
		return f.PurgeAllFailedFunc(ctx)
	}
	return nil
}

func (f *FakeServerStorage) Ping(ctx context.Context) error {
	if f.PingFunc != nil {
		return f.PingFunc(ctx)
	}
	return nil
}

func (f *FakeServerStorage) GetJobDetail(ctx context.Context, jobID string) (*queue.JobEnvelope, error) {
	if f.GetJobDetailFunc != nil {
		return f.GetJobDetailFunc(ctx, jobID)
	}
	return nil, nil
}

// FakeWorkerPoolStorage implements queue.WorkerPoolStorage.
type FakeWorkerPoolStorage struct {
	EnqueueFunc             func(ctx context.Context, env *queue.JobEnvelope) error
	DequeueFunc             func(ctx context.Context, queueName string) (*queue.JobEnvelope, error)
	AckFunc                 func(ctx context.Context, jobID string) error
	FailFunc                func(ctx context.Context, jobID string, err error) error
	PollScheduledFunc       func(ctx context.Context, queueName string) error
	PostponeJobFunc         func(ctx context.Context, jobID string, queueName string, delay time.Duration) error
	RegisterProcessFunc     func(ctx context.Context, info *queue.ProcessInfo) error
	HeartbeatProcessFunc    func(ctx context.Context, processID string) error
	GetActiveProcessesFunc  func(ctx context.Context) ([]queue.ProcessInfo, error)
	LockCronExecutionFunc   func(ctx context.Context, cronName string, executionMinute time.Time) (bool, error)
	GetRunningJobsCountFunc func(ctx context.Context, jobName string) (int, error)
	CheckRateLimitFunc      func(ctx context.Context, jobName string, limit int, period time.Duration) (bool, error)
	PingFunc                func(ctx context.Context) error
	IsQueuePausedFunc       func(ctx context.Context, queueName string) (bool, error)
	RegisterCronJobsFunc    func(ctx context.Context, crons []queue.CronJob) error
	PurgeExpiredDLQFunc     func(ctx context.Context, ttl time.Duration) error
}

func (f *FakeWorkerPoolStorage) Enqueue(ctx context.Context, env *queue.JobEnvelope) error {
	if f.EnqueueFunc != nil {
		return f.EnqueueFunc(ctx, env)
	}
	return nil
}

func (f *FakeWorkerPoolStorage) Dequeue(ctx context.Context, queueName string) (*queue.JobEnvelope, error) {
	if f.DequeueFunc != nil {
		return f.DequeueFunc(ctx, queueName)
	}
	return nil, nil
}

func (f *FakeWorkerPoolStorage) Ack(ctx context.Context, jobID string) error {
	if f.AckFunc != nil {
		return f.AckFunc(ctx, jobID)
	}
	return nil
}

func (f *FakeWorkerPoolStorage) Fail(ctx context.Context, jobID string, err error) error {
	if f.FailFunc != nil {
		return f.FailFunc(ctx, jobID, err)
	}
	return nil
}

func (f *FakeWorkerPoolStorage) PollScheduled(ctx context.Context, queueName string) error {
	if f.PollScheduledFunc != nil {
		return f.PollScheduledFunc(ctx, queueName)
	}
	return nil
}

func (f *FakeWorkerPoolStorage) PostponeJob(ctx context.Context, jobID string, queueName string, delay time.Duration) error {
	if f.PostponeJobFunc != nil {
		return f.PostponeJobFunc(ctx, jobID, queueName, delay)
	}
	return nil
}

func (f *FakeWorkerPoolStorage) RegisterProcess(ctx context.Context, info *queue.ProcessInfo) error {
	if f.RegisterProcessFunc != nil {
		return f.RegisterProcessFunc(ctx, info)
	}
	return nil
}

func (f *FakeWorkerPoolStorage) HeartbeatProcess(ctx context.Context, processID string) error {
	if f.HeartbeatProcessFunc != nil {
		return f.HeartbeatProcessFunc(ctx, processID)
	}
	return nil
}

func (f *FakeWorkerPoolStorage) GetActiveProcesses(ctx context.Context) ([]queue.ProcessInfo, error) {
	if f.GetActiveProcessesFunc != nil {
		return f.GetActiveProcessesFunc(ctx)
	}
	return nil, nil
}

func (f *FakeWorkerPoolStorage) LockCronExecution(ctx context.Context, cronName string, executionMinute time.Time) (bool, error) {
	if f.LockCronExecutionFunc != nil {
		return f.LockCronExecutionFunc(ctx, cronName, executionMinute)
	}
	return false, nil
}

func (f *FakeWorkerPoolStorage) GetRunningJobsCount(ctx context.Context, jobName string) (int, error) {
	if f.GetRunningJobsCountFunc != nil {
		return f.GetRunningJobsCountFunc(ctx, jobName)
	}
	return 0, nil
}

func (f *FakeWorkerPoolStorage) CheckRateLimit(ctx context.Context, jobName string, limit int, period time.Duration) (bool, error) {
	if f.CheckRateLimitFunc != nil {
		return f.CheckRateLimitFunc(ctx, jobName, limit, period)
	}
	return false, nil
}

func (f *FakeWorkerPoolStorage) Ping(ctx context.Context) error {
	if f.PingFunc != nil {
		return f.PingFunc(ctx)
	}
	return nil
}

func (f *FakeWorkerPoolStorage) IsQueuePaused(ctx context.Context, queueName string) (bool, error) {
	if f.IsQueuePausedFunc != nil {
		return f.IsQueuePausedFunc(ctx, queueName)
	}
	return false, nil
}

func (f *FakeWorkerPoolStorage) RegisterCronJobs(ctx context.Context, crons []queue.CronJob) error {
	if f.RegisterCronJobsFunc != nil {
		return f.RegisterCronJobsFunc(ctx, crons)
	}
	return nil
}

func (f *FakeWorkerPoolStorage) PurgeExpiredDLQ(ctx context.Context, ttl time.Duration) error {
	if f.PurgeExpiredDLQFunc != nil {
		return f.PurgeExpiredDLQFunc(ctx, ttl)
	}
	return nil
}
