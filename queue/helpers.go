package queue

import (
	"crypto/rand"
	"encoding/hex"
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
