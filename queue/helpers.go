package queue

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"sort"
	"time"
)

func generateJobID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// computeBackoffDelay returns the delay before the next retry attempt
// using exponential backoff: 10s, 20s, 40s... capped at 1h, with sub-second jitter.
func computeBackoffDelay(attempts int) time.Duration {
	delaySec := (1 << uint(attempts)) * 10
	if delaySec > 3600 {
		delaySec = 3600
	}
	jitterSec := time.Now().Nanosecond() % 3
	return time.Duration(delaySec+jitterSec) * time.Second
}

func getRunAt(t *time.Time) time.Time {
	if t != nil {
		return *t
	}
	return time.Now()
}

func getMaxAttempts(max int) int {
	if max <= 0 {
		return 3
	}
	return max
}

func sortCronJobs(m map[string]CronJobDetail) []CronJobDetail {
	names := make([]string, 0, len(m))
	for name := range m {
		names = append(names, name)
	}
	sort.Strings(names)
	res := make([]CronJobDetail, 0, len(names))
	for _, name := range names {
		res = append(res, m[name])
	}
	return res
}

type sqlExecutor interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
}


