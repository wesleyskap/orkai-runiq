package queue

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// OTelTracer bridges Runiq telemetry calls to native OpenTelemetry trace and metric systems.
type OTelTracer struct {
	tracer trace.Tracer
	meter  metric.Meter
}

// NewOTelTracer initializes a standard OpenTelemetry tracer and meter.
// Usage example:
//	t := queue.NewOTelTracer(tp, mp)
func NewOTelTracer(tp trace.TracerProvider, mp metric.MeterProvider) *OTelTracer {
	if tp == nil {
		tp = otel.GetTracerProvider()
	}
	if mp == nil {
		mp = otel.GetMeterProvider()
	}
	return &OTelTracer{
		tracer: tp.Tracer("orkai-runiq"),
		meter:  mp.Meter("orkai-runiq"),
	}
}

// ExtractTrace retrieves trace contexts from context.
func (o *OTelTracer) ExtractTrace(ctx context.Context) (string, string) {
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return "", ""
	}
	return sc.TraceID().String(), sc.SpanID().String()
}

// InjectTrace inserts trace contexts into context.
func (o *OTelTracer) InjectTrace(ctx context.Context, traceID, spanID string) context.Context {
	tid, err := trace.TraceIDFromHex(traceID)
	if err != nil {
		return ctx
	}
	sid, err := trace.SpanIDFromHex(spanID)
	if err != nil {
		return ctx
	}
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    tid,
		SpanID:     sid,
		TraceFlags: trace.FlagsSampled,
		Remote:     true,
	})
	return trace.ContextWithSpanContext(ctx, sc)
}

// RecordLatency logs task durations to telemetry systems.
func (o *OTelTracer) RecordLatency(ctx context.Context, name string, duration time.Duration, tags map[string]string) {
	hist, err := o.meter.Float64Histogram(name)
	if err != nil {
		return
	}
	attrs := convertTagsToAttrs(tags)
	hist.Record(ctx, float64(duration.Milliseconds()), metric.WithAttributes(attrs...))
}

// IncrementCounter registers occurrences of specific events.
func (o *OTelTracer) IncrementCounter(ctx context.Context, name string, tags map[string]string) {
	counter, err := o.meter.Int64Counter(name)
	if err != nil {
		return
	}
	attrs := convertTagsToAttrs(tags)
	counter.Add(ctx, 1, metric.WithAttributes(attrs...))
}

func convertTagsToAttrs(tags map[string]string) []attribute.KeyValue {
	attrs := make([]attribute.KeyValue, 0, len(tags))
	for k, v := range tags {
		attrs = append(attrs, attribute.String(k, v))
	}
	return attrs
}

// RegisterQueueDepthMetrics registers an observable gauge to report pending queue depths dynamically.
// Usage example:
//	err := t.RegisterQueueDepthMetrics(storage, []string{"default"})
func (o *OTelTracer) RegisterQueueDepthMetrics(storage Storage, queues []string) error {
	gauge, err := o.meter.Int64ObservableGauge("runiq_queue_depth",
		metric.WithDescription("Current depth of the job queue"),
	)
	if err != nil {
		return err
	}
	_, err = o.meter.RegisterCallback(func(ctx context.Context, obs metric.Observer) error {
		stats, err := storage.GetStats(ctx)
		if err != nil {
			return err
		}
		obs.ObserveInt64(gauge, stats.Pending, metric.WithAttributes(attribute.String("queue", "_all")))
		for _, q := range stats.Queues {
			obs.ObserveInt64(gauge, q.Pending, metric.WithAttributes(attribute.String("queue", q.Name)))
		}
		return nil
	}, gauge)
	return err
}
