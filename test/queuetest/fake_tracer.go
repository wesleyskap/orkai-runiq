package queuetest

import (
	"context"
	"time"
)

// LatencyRecord stores recorded latency details.
type LatencyRecord struct {
	Name     string
	Duration time.Duration
	Tags     map[string]string
}

// CounterRecord stores counter increment details.
type CounterRecord struct {
	Name string
	Tags map[string]string
}

// FakeTracer implements queue.Tracer for testing purposes.
type FakeTracer struct {
	ExtractTraceFunc     func(ctx context.Context) (string, string)
	InjectTraceFunc      func(ctx context.Context, traceID, spanID string) context.Context
	RecordLatencyFunc    func(ctx context.Context, name string, duration time.Duration, tags map[string]string)
	IncrementCounterFunc func(ctx context.Context, name string, tags map[string]string)

	Latencies []LatencyRecord
	Counters  []CounterRecord
}

func (t *FakeTracer) ExtractTrace(ctx context.Context) (string, string) {
	if t.ExtractTraceFunc != nil {
		return t.ExtractTraceFunc(ctx)
	}
	return "", ""
}

func (t *FakeTracer) InjectTrace(ctx context.Context, traceID, spanID string) context.Context {
	if t.InjectTraceFunc != nil {
		return t.InjectTraceFunc(ctx, traceID, spanID)
	}
	return ctx
}

func (t *FakeTracer) RecordLatency(ctx context.Context, name string, duration time.Duration, tags map[string]string) {
	record := LatencyRecord{Name: name, Duration: duration, Tags: tags}
	t.Latencies = append(t.Latencies, record)
	if t.RecordLatencyFunc != nil {
		t.RecordLatencyFunc(ctx, name, duration, tags)
	}
}

func (t *FakeTracer) IncrementCounter(ctx context.Context, name string, tags map[string]string) {
	record := CounterRecord{Name: name, Tags: tags}
	t.Counters = append(t.Counters, record)
	if t.IncrementCounterFunc != nil {
		t.IncrementCounterFunc(ctx, name, tags)
	}
}
