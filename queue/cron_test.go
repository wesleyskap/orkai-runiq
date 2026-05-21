package queue

import (
	"context"
	"testing"
	"time"
)

func TestMatchCron(t *testing.T) {
	baseTime := time.Date(2026, 5, 20, 8, 30, 0, 0, time.UTC)

	tests := []struct {
		spec  string
		time  time.Time
		match bool
	}{
		{"* * * * *", baseTime, true},
		{"30 8 20 5 3", baseTime, true},
		{"*/10 * * * *", baseTime, true},
		{"*/15 * * * *", baseTime, true},
		{"*/7 * * * *", baseTime, false},
		{"30,45 8,9 * * *", baseTime, true},
		{"20-35 8-10 * * *", baseTime, true},
		{"0-15 * * * *", baseTime, false},
		{"30 8 * * 1-5", baseTime, true},
		{"30 8 * * 0,6", baseTime, false},
	}

	for _, tc := range tests {
		got := MatchCron(tc.spec, tc.time)
		if got != tc.match {
			t.Errorf("MatchCron(%q, %v) = %v; want %v", tc.spec, tc.time, got, tc.match)
		}
	}
}

type mockCronStorage struct {
	WorkerPoolStorage
	enqueued []*JobEnvelope
	lockRes  bool
	lockErr  error
}

func (m *mockCronStorage) LockCronExecution(ctx context.Context, name string, t time.Time) (bool, error) {
	return m.lockRes, m.lockErr
}

func (m *mockCronStorage) Enqueue(ctx context.Context, env *JobEnvelope) error {
	m.enqueued = append(m.enqueued, env)
	return nil
}

func TestWorkerPoolCronScheduler_LockAcquired(t *testing.T) {
	mock := &mockCronStorage{lockRes: true}
	pool := NewWorkerPool(mock, 1)
	cron := CronJob{
		Payload: []byte("pay"),
		Spec:    "* * * * *",
		Name:    "job-a",
		Queue:   "q-a",
	}

	pool.enqueueCronJob(context.Background(), cron, time.Now())

	if len(mock.enqueued) != 1 {
		t.Fatalf("expected 1 enqueued job, got %d", len(mock.enqueued))
	}
	env := mock.enqueued[0]
	if env.Queue != "q-a" || env.Name != "job-a" {
		t.Errorf("incorrect job queue/name: %+v", env)
	}
}

func TestWorkerPoolCronScheduler_LockDenied(t *testing.T) {
	mock := &mockCronStorage{lockRes: false}
	pool := NewWorkerPool(mock, 1)
	cron := CronJob{
		Payload: []byte("pay"),
		Spec:    "* * * * *",
		Name:    "job-b",
		Queue:   "q-b",
	}

	pool.enqueueCronJob(context.Background(), cron, time.Now())

	if len(mock.enqueued) != 0 {
		t.Fatalf("expected 0 enqueued jobs due to lock denial, got %d", len(mock.enqueued))
	}
}

func TestWorkerPoolCronScheduler_ProcessMatching(t *testing.T) {
	mock := &mockCronStorage{lockRes: true}
	pool := NewWorkerPool(mock, 1)
	pool.RegisterCron("0 0 * * *", "default", "daily-job", []byte{})

	ctx := context.Background()
	pool.processCronJobs(ctx, time.Date(2026, 5, 20, 0, 0, 0, 0, time.UTC))

	for i := 0; i < 10; i++ {
		if len(mock.enqueued) > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if len(mock.enqueued) != 1 {
		t.Fatalf("expected 1 job, got %d", len(mock.enqueued))
	}
}
