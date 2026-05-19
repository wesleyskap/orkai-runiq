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
