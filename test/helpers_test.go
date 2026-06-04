package test

import (
	"testing"
	"time"

	"github.com/wesleyskap/orkai-runiq/v3/queue"
)

func TestComputeBackoffDelay_CapsAtOneHour(t *testing.T) {
	delay := queue.ComputeBackoffDelay(100)
	if delay > time.Hour {
		t.Errorf("expected delay <= 1h, got %v", delay)
	}
}

func TestComputeBackoffDelay_Exponential(t *testing.T) {
	d0 := queue.ComputeBackoffDelay(0) // 10s
	d1 := queue.ComputeBackoffDelay(1) // 20s
	if d0 >= d1 {
		t.Errorf("expected delay(0) < delay(1), got %v >= %v", d0, d1)
	}
}

func TestComputeBackoffDelay_NonNegative(t *testing.T) {
	for i := 0; i <= 10; i++ {
		d := queue.ComputeBackoffDelay(i)
		if d < 0 {
			t.Errorf("expected non-negative delay for attempt %d, got %v", i, d)
		}
	}
}
