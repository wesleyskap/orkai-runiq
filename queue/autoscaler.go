package queue

import (
	"context"
	"time"
)

// DynamicConcurrencyConfig holds configuration for autoscaling worker pool concurrency.
type DynamicConcurrencyConfig struct {
	CheckInterval   time.Duration
	MinConcurrency  int
	MaxConcurrency  int
	QueueDepthLimit int
	ScaleUpStep     int
	ScaleDownStep   int
}

// WithDynamicConcurrency configures the autoscaler configuration for the WorkerPool.
func WithDynamicConcurrency(cfg DynamicConcurrencyConfig) WorkerOption {
	return func(w *WorkerPool) {
		w.autoscale = &cfg
	}
}

func (w *WorkerPool) setupSemaphore() {
	if w.autoscale == nil {
		w.sem = make(chan struct{}, w.concurrency)
		w.currentConcurrency = w.concurrency
		return
	}
	w.sem = make(chan struct{}, w.autoscale.MaxConcurrency)
	w.currentConcurrency = w.autoscale.MinConcurrency
	diff := w.autoscale.MaxConcurrency - w.autoscale.MinConcurrency
	for i := 0; i < diff; i++ {
		w.sem <- struct{}{}
	}
}

func (w *WorkerPool) clampTarget(target int) int {
	if target < w.autoscale.MinConcurrency {
		return w.autoscale.MinConcurrency
	}
	if target > w.autoscale.MaxConcurrency {
		return w.autoscale.MaxConcurrency
	}
	return target
}

func (w *WorkerPool) scaleUp(diff int) {
	for i := 0; i < diff; i++ {
		<-w.sem
	}
}

func (w *WorkerPool) scaleDown(diff int) {
	go func() {
		for i := 0; i < diff; i++ {
			w.sem <- struct{}{}
		}
	}()
}

func (w *WorkerPool) adjustConcurrency(target int) {
	w.concurrencyMutex.Lock()
	defer w.concurrencyMutex.Unlock()
	target = w.clampTarget(target)
	if target == w.currentConcurrency {
		return
	}
	diff := target - w.currentConcurrency
	w.currentConcurrency = target
	if diff > 0 {
		w.scaleUp(diff)
		return
	}
	w.scaleDown(-diff)
}

func (w *WorkerPool) getCurrentConcurrency() int {
	w.concurrencyMutex.Lock()
	defer w.concurrencyMutex.Unlock()
	return w.currentConcurrency
}

func (w *WorkerPool) getMonitoredPendingCount(ctx context.Context) (int, error) {
	stats, err := w.storage.GetStats(ctx)
	if err != nil {
		return 0, err
	}
	total := 0
	monitoredMap := make(map[string]bool)
	for _, q := range w.monitoredQueues {
		monitoredMap[q] = true
	}
	for _, qs := range stats.Queues {
		if monitoredMap[qs.Name] {
			total += int(qs.Pending)
		}
	}
	return total, nil
}

func (w *WorkerPool) getAutoscaleSteps() (int, int) {
	up := w.autoscale.ScaleUpStep
	if up <= 0 {
		up = 1
	}
	down := w.autoscale.ScaleDownStep
	if down <= 0 {
		down = 1
	}
	return up, down
}

func (w *WorkerPool) runAutoscaleIteration(ctx context.Context) {
	pending, err := w.getMonitoredPendingCount(ctx)
	if err != nil {
		return
	}
	w.concurrencyMutex.Lock()
	curr := w.currentConcurrency
	w.concurrencyMutex.Unlock()
	up, down := w.getAutoscaleSteps()
	if pending > w.autoscale.QueueDepthLimit {
		w.adjustConcurrency(curr + up)
		return
	}
	if pending == 0 {
		w.adjustConcurrency(curr - down)
	}
}

func (w *WorkerPool) startAutoscaler(ctx context.Context) {
	interval := w.autoscale.CheckInterval
	if interval <= 0 {
		interval = 5 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.runAutoscaleIteration(ctx)
		}
	}
}
