package queue

import (
	"errors"
	"sync"
	"time"
)

// ErrCircuitBreakerOpen is returned when client writes are blocked by an open circuit breaker.
var ErrCircuitBreakerOpen = errors.New("circuit breaker is open")

type cbState int

const (
	cbStateClosed cbState = iota
	cbStateOpen
	cbStateHalfOpen
)

// CircuitBreakerConfig configures the failure and latency rules for the circuit breaker.
type CircuitBreakerConfig struct {
	Cooldown         time.Duration
	LatencyThreshold time.Duration
	FailureThreshold int
}

type circuitBreaker struct {
	lastStateChg time.Time
	config       CircuitBreakerConfig
	failures     int
	state        cbState
	mu           sync.RWMutex
}

func newCircuitBreaker(cfg CircuitBreakerConfig) *circuitBreaker {
	return &circuitBreaker{
		config:       cfg,
		lastStateChg: time.Now(),
	}
}

func (cb *circuitBreaker) beforeCall() error {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	if cb.state == cbStateOpen {
		if time.Since(cb.lastStateChg) > cb.config.Cooldown {
			cb.state = cbStateHalfOpen
			cb.lastStateChg = time.Now()
			return nil
		}
		return ErrCircuitBreakerOpen
	}
	return nil
}

func (cb *circuitBreaker) handleFailure() {
	if cb.state == cbStateHalfOpen || cb.failures >= cb.config.FailureThreshold {
		cb.state = cbStateOpen
		cb.lastStateChg = time.Now()
	}
}

func (cb *circuitBreaker) afterCall(err error, elapsed time.Duration) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	isFail := err != nil || (cb.config.LatencyThreshold > 0 && elapsed > cb.config.LatencyThreshold)
	if isFail {
		cb.failures++
		cb.handleFailure()
		return
	}
	if cb.state == cbStateHalfOpen {
		cb.state = cbStateClosed
		cb.lastStateChg = time.Now()
	}
	cb.failures = 0
}
