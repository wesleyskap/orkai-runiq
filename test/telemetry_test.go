package test

import (
	"context"
	"testing"
	"time"

	"github.com/wesleyskap/orkai-runiq/v2/queue"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/trace"
)

// TestOTelTracer_ExtractAndInjectTrace asserts trace context propagation.
func TestOTelTracer_ExtractAndInjectTrace(t *testing.T) {
	tp := trace.NewTracerProvider()
	mp := metric.NewMeterProvider()
	tracer := queue.NewOTelTracer(tp, mp)

	ctx := context.Background()
	traceID := "4bf92f3577b34da6a3ce929d0e0e4736"
	spanID := "00f067aa0ba902b7"

	ctx2 := tracer.InjectTrace(ctx, traceID, spanID)
	extractedTrace, extractedSpan := tracer.ExtractTrace(ctx2)

	if extractedTrace != traceID {
		t.Errorf("expected trace ID %s, got %s", traceID, extractedTrace)
	}
	if extractedSpan != spanID {
		t.Errorf("expected span ID %s, got %s", spanID, extractedSpan)
	}
}

// TestOTelTracer_Metrics checks if standard counter and histogram metrics are recorded.
func TestOTelTracer_Metrics(t *testing.T) {
	reader := metric.NewManualReader()
	mp := metric.NewMeterProvider(metric.WithReader(reader))
	tracer := queue.NewOTelTracer(nil, mp)

	ctx := context.Background()
	tags := map[string]string{"name": "test-job"}

	tracer.IncrementCounter(ctx, "test_counter", tags)
	tracer.RecordLatency(ctx, "test_latency", 150*time.Millisecond, tags)

	var rm metricdata.ResourceMetrics
	err := reader.Collect(ctx, &rm)
	if err != nil {
		t.Fatalf("failed to collect metrics: %v", err)
	}

	foundCounter := false
	foundLatency := false

	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == "test_counter" {
				foundCounter = true
			}
			if m.Name == "test_latency" {
				foundLatency = true
			}
		}
	}

	if !foundCounter {
		t.Error("test_counter metric was not registered or recorded")
	}
	if !foundLatency {
		t.Error("test_latency metric was not registered or recorded")
	}
}

// TestOTelTracer_QueueDepth validates the asynchronous queue depth metrics observer.
func TestOTelTracer_QueueDepth(t *testing.T) {
	reader := metric.NewManualReader()
	mp := metric.NewMeterProvider(metric.WithReader(reader))
	tracer := queue.NewOTelTracer(nil, mp)

	storage := &FakeStorage{
		StatsToReturn: &queue.Stats{
			Pending: 5,
			Queues: []queue.QueueStats{
				{Name: "high", Pending: 3},
				{Name: "low", Pending: 2},
			},
		},
	}

	err := tracer.RegisterQueueDepthMetrics(storage, []string{"high", "low"})
	if err != nil {
		t.Fatalf("failed to register queue depth metrics: %v", err)
	}

	ctx := context.Background()
	var rm metricdata.ResourceMetrics
	err = reader.Collect(ctx, &rm)
	if err != nil {
		t.Fatalf("failed to collect metrics: %v", err)
	}

	foundGauge := false
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == "runiq_queue_depth" {
				foundGauge = true
			}
		}
	}

	if !foundGauge {
		t.Error("runiq_queue_depth gauge metric was not registered or recorded")
	}
}
