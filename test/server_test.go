package test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/wesleyskap/orkai-runiq/queue"
)

// TestDashboardStatsEndpoint validates the JSON stats output from /api/stats.
// Usage example:
//	go test -v ./test/...
func TestDashboardStatsEndpoint(t *testing.T) {
	fakeStore := &FakeStorage{
		StatsToReturn: &queue.Stats{
			Pending: 10,
			Running: 3,
			Failed:  1,
			Queues: []queue.QueueStats{
				{Name: "default", Pending: 10, Running: 3, Failed: 1},
			},
			Processes: []queue.ProcessInfo{
				{ProcessID: "proc-1", Concurrency: 5, Queues: []string{"default"}},
			},
		},
	}

	server := queue.NewServer(fakeStore, ":8989")
	handler := server.Handler() // Access the inner router or handler for testing

	req := httptest.NewRequest(http.MethodGet, "/api/stats", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", w.Code)
	}

	var res queue.Stats
	if err := json.Unmarshal(w.Body.Bytes(), &res); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if res.Pending != 10 || len(res.Queues) != 1 || res.Queues[0].Name != "default" {
		t.Errorf("mismatched stats in JSON response: %+v", res)
	}

	if len(res.Processes) != 1 || res.Processes[0].ProcessID != "proc-1" {
		t.Errorf("expected active process 'proc-1', got %+v", res.Processes)
	}
}

// TestDashboardUIEndpoint validates that the root path serves the embedded index.html UI.
// Usage example:
//	go test -v ./test/...
func TestDashboardUIEndpoint(t *testing.T) {
	fakeStore := &FakeStorage{}
	server := queue.NewServer(fakeStore, ":8989")
	handler := server.Handler()

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	// Since assets might be loaded or not, it should either serve 200 OK with html content
	// or redirect/serve index.html.
	if w.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", w.Code)
	}

	body := w.Body.String()
	if !strings.Contains(body, "<!DOCTYPE html>") && !strings.Contains(body, "<html") {
		t.Errorf("expected HTML body, got %q", body)
	}
}

// TestAdminEndpoints validates the dashboard administrative POST endpoints.
func TestAdminEndpoints(t *testing.T) {
	fakeStore := &FakeStorage{}
	server := queue.NewServer(fakeStore, ":8989")
	handler := server.Handler()

	// 1. Test /api/jobs/retry
	reqRetry := httptest.NewRequest(http.MethodPost, "/api/jobs/retry?id=job-retry-123", nil)
	wRetry := httptest.NewRecorder()
	handler.ServeHTTP(wRetry, reqRetry)
	if wRetry.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", wRetry.Code)
	}
	if len(fakeStore.Retried) != 1 || fakeStore.Retried[0] != "job-retry-123" {
		t.Errorf("expected job-retry-123 to be retried, got %+v", fakeStore.Retried)
	}

	// 2. Test /api/jobs/cancel
	reqCancel := httptest.NewRequest(http.MethodPost, "/api/jobs/cancel?id=job-cancel-456", nil)
	wCancel := httptest.NewRecorder()
	handler.ServeHTTP(wCancel, reqCancel)
	if wCancel.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", wCancel.Code)
	}
	if len(fakeStore.Cancelled) != 1 || fakeStore.Cancelled[0] != "job-cancel-456" {
		t.Errorf("expected job-cancel-456 to be cancelled, got %+v", fakeStore.Cancelled)
	}

	// 3. Test /api/queues/clear
	reqClear := httptest.NewRequest(http.MethodPost, "/api/queues/clear?name=test-queue", nil)
	wClear := httptest.NewRecorder()
	handler.ServeHTTP(wClear, reqClear)
	if wClear.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", wClear.Code)
	}
	if len(fakeStore.Cleared) != 1 || fakeStore.Cleared[0] != "test-queue" {
		t.Errorf("expected test-queue to be cleared, got %+v", fakeStore.Cleared)
	}
}

