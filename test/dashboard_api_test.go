package test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/wesleyskap/orkai-runiq/v3/queue"
)

func TestDashboardGetJobs(t *testing.T) {
	fakeStore := &FakeStorage{}
	srv := queue.NewServer(fakeStore, ":8989")
	req := httptest.NewRequest(http.MethodGet, "/api/jobs?q=foo&status=failed&page=2&limit=5", nil)
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	body := w.Body.String()
	if !strings.Contains(body, `"jobs"`) {
		t.Errorf("expected jobs json response, got %s", body)
	}
}

func TestDashboardBulkRetry(t *testing.T) {
	fakeStore := &FakeStorage{}
	srv := queue.NewServer(fakeStore, ":8989")
	body := `{"ids":["job1","job2"]}`
	req := httptest.NewRequest(http.MethodPost, "/api/jobs/bulk-retry", strings.NewReader(body))
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if len(fakeStore.Retried) != 2 {
		t.Errorf("expected 2 retried jobs, got %d", len(fakeStore.Retried))
	}
}

func TestDashboardBulkCancel(t *testing.T) {
	fakeStore := &FakeStorage{}
	srv := queue.NewServer(fakeStore, ":8989")
	body := `{"ids":["job1","job2"]}`
	req := httptest.NewRequest(http.MethodPost, "/api/jobs/bulk-cancel", strings.NewReader(body))
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if len(fakeStore.Cancelled) != 2 {
		t.Errorf("expected 2 cancelled jobs, got %d", len(fakeStore.Cancelled))
	}
}

func TestDashboardBulkPurge(t *testing.T) {
	fakeStore := &FakeStorage{}
	srv := queue.NewServer(fakeStore, ":8989")
	body := `{"ids":["job1","job2"]}`
	req := httptest.NewRequest(http.MethodPost, "/api/jobs/bulk-purge", strings.NewReader(body))
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestDashboardStatsStream(t *testing.T) {
	fakeStore := &FakeStorage{
		StatsToReturn: &queue.Stats{Pending: 5},
	}
	srv := queue.NewServer(fakeStore, ":8989")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req := httptest.NewRequest(http.MethodGet, "/api/stats/stream", nil).WithContext(ctx)
	w := httptest.NewRecorder()
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()
	srv.Handler().ServeHTTP(w, req)
	if !strings.Contains(w.Body.String(), "data: ") {
		t.Errorf("expected SSE event, got %q", w.Body.String())
	}
}

func TestDashboardMetrics(t *testing.T) {
	fakeStore := &FakeStorage{
		StatsToReturn: &queue.Stats{
			Queues: []queue.QueueStats{
				{Name: "default", Pending: 12, Running: 2, Failed: 1, Processed: 50, Paused: true},
			},
		},
	}
	srv := queue.NewServer(fakeStore, ":8989")
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	body := w.Body.String()
	if !strings.Contains(body, `runiq_jobs_count{queue="default", status="pending"} 12`) {
		t.Errorf("expected metric in response, got %s", body)
	}
}
