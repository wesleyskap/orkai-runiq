package test

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/wesleyskap/orkai-runiq/v2/queue"
)

// TestDashboardRetryModified asserts that the server processes retry modified requests correctly.
func TestDashboardRetryModified(t *testing.T) {
	fake := &FakeStorage{}
	srv := queue.NewServer(fake, ":8990")
	payload := `{"count":42}`
	req := httptest.NewRequest(http.MethodPost, "/api/jobs/retry-modified?id=job-abc", strings.NewReader(payload))
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	assertStatusCode(t, w.Code, http.StatusOK)
	val, ok := fake.ModifiedRetries["job-abc"]
	if !ok || string(val) != payload {
		t.Errorf("expected payload %q, got %q", payload, string(val))
	}
}

// TestDashboardGetCronSchedules checks the dynamic cron list API.
func TestDashboardGetCronSchedules(t *testing.T) {
	fake := &FakeStorage{
		CronSchedules: []queue.CronJob{
			{Name: "dynamic-1", Spec: "* * * * *", Queue: "default"},
		},
	}
	srv := queue.NewServer(fake, ":8990")
	req := httptest.NewRequest(http.MethodGet, "/api/crons", nil)
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	assertStatusCode(t, w.Code, http.StatusOK)
	body := w.Body.String()
	if !strings.Contains(body, `"dynamic-1"`) {
		t.Errorf("expected dynamic cron in response, got %s", body)
	}
}

// TestDashboardSaveCronSchedule asserts saving a dynamic cron schedule via POST.
func TestDashboardSaveCronSchedule(t *testing.T) {
	fake := &FakeStorage{}
	srv := queue.NewServer(fake, ":8990")
	body := `{"name":"sync","expression":"*/5 * * * *","queue":"high","payload":"{}","timezone":"UTC","paused":false}`
	req := httptest.NewRequest(http.MethodPost, "/api/crons", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	assertStatusCode(t, w.Code, http.StatusOK)
	if len(fake.CronSchedules) != 1 || fake.CronSchedules[0].Name != "sync" {
		t.Errorf("expected cron to be saved, got %v", fake.CronSchedules)
	}
}

// TestDashboardSaveCronInvalidTimezone checks that invalid timezone names are rejected on save.
func TestDashboardSaveCronInvalidTimezone(t *testing.T) {
	fake := &FakeStorage{}
	srv := queue.NewServer(fake, ":8990")
	body := `{"name":"sync-bad","expression":"* * * * *","queue":"high","payload":"{}","timezone":"Invalid/Zone","paused":false}`
	req := httptest.NewRequest(http.MethodPost, "/api/crons", bytes.NewBufferString(body))
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	assertStatusCode(t, w.Code, http.StatusBadRequest)
	if len(fake.CronSchedules) != 0 {
		t.Errorf("expected invalid timezone cron to not be saved, but it was")
	}
}

// TestDashboardDeleteCronSchedule verifies that deleting a cron schedule works.
func TestDashboardDeleteCronSchedule(t *testing.T) {
	fake := &FakeStorage{
		CronSchedules: []queue.CronJob{
			{Name: "to-delete", Spec: "* * * * *", Queue: "default"},
		},
	}
	srv := queue.NewServer(fake, ":8990")
	req := httptest.NewRequest(http.MethodDelete, "/api/crons?name=to-delete", nil)
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, req)
	assertStatusCode(t, w.Code, http.StatusOK)
	if len(fake.CronSchedules) != 0 {
		t.Errorf("expected cron list to be empty, got %d", len(fake.CronSchedules))
	}
}

func assertStatusCode(t *testing.T, actual, expected int) {
	t.Helper()
	if actual != expected {
		t.Fatalf("expected status %d, got %d", expected, actual)
	}
}
