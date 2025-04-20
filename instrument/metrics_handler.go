package instrument

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.temporal.io/sdk/client"
)

// OpenTelemetryMetricsHandler implements Temporal's MetricsHandler interface using OpenTelemetry
type OpenTelemetryMetricsHandler struct {
	meter metric.Meter
	tags  map[string]string
}

// NewOpenTelemetryMetricsHandler creates a new metrics handler using OpenTelemetry
func NewOpenTelemetryMetricsHandler() client.MetricsHandler {
	meter := otel.GetMeterProvider().Meter("temporal-client")
	return &OpenTelemetryMetricsHandler{
		meter: meter,
	}
}

// WithTags implements MetricsHandler.WithTags
func (h *OpenTelemetryMetricsHandler) WithTags(tags map[string]string) client.MetricsHandler {
	// Create a new handler with the same meter and merged tags
	newTags := make(map[string]string, len(h.tags)+len(tags))

	// First copy existing tags
	for k, v := range h.tags {
		newTags[k] = v
	}

	// Then add/override with new tags
	for k, v := range tags {
		newTags[k] = v
	}

	return &OpenTelemetryMetricsHandler{
		meter: h.meter,
		tags:  newTags,
	}
}

// Counter implements MetricsHandler.Counter
func (h *OpenTelemetryMetricsHandler) Counter(name string) client.MetricsCounter {
	counter, _ := h.meter.Int64Counter(name)
	return &OpenTelemetryCounter{
		counter: counter,
		tags:    h.tags,
	}
}

// Gauge implements MetricsHandler.Gauge
func (h *OpenTelemetryMetricsHandler) Gauge(name string) client.MetricsGauge {
	gauge, _ := h.meter.Float64UpDownCounter(name)
	return &OpenTelemetryGauge{
		gauge: gauge,
		tags:  h.tags,
	}
}

// Timer implements MetricsHandler.Timer
func (h *OpenTelemetryMetricsHandler) Timer(name string) client.MetricsTimer {
	histogram, _ := h.meter.Float64Histogram(name)
	return &OpenTelemetryTimer{
		histogram: histogram,
		tags:      h.tags,
	}
}

// OpenTelemetryCounter implements MetricsCounter
type OpenTelemetryCounter struct {
	counter metric.Int64Counter
	tags    map[string]string
}

// Record implements MetricsCounter.Record
func (c *OpenTelemetryCounter) Record(value int64) {
	c.counter.Add(context.Background(), value, metric.WithAttributes(convertTagsToAttributes(c.tags)...))
}

// Inc implements MetricsCounter.Inc
func (c *OpenTelemetryCounter) Inc(value int64) {
	c.counter.Add(context.Background(), value, metric.WithAttributes(convertTagsToAttributes(c.tags)...))
}

// OpenTelemetryGauge implements MetricsGauge
type OpenTelemetryGauge struct {
	gauge metric.Float64UpDownCounter
	tags  map[string]string
}

// Update implements MetricsGauge.Update
func (g *OpenTelemetryGauge) Update(value float64) {
	g.gauge.Add(context.Background(), value, metric.WithAttributes(convertTagsToAttributes(g.tags)...))
}

// OpenTelemetryTimer implements MetricsTimer
type OpenTelemetryTimer struct {
	histogram metric.Float64Histogram
	tags      map[string]string
}

// Record implements MetricsTimer.Record
func (t *OpenTelemetryTimer) Record(value time.Duration) {
	// fmt.Println("Recording timer", t.histogram.Description(), value.Milliseconds(), convertTagsToAttributes(t.tags))
	t.histogram.Record(context.Background(), float64(value.Milliseconds()), metric.WithAttributes(convertTagsToAttributes(t.tags)...))
}

// convertTagsToAttributes converts Temporal tags to OpenTelemetry attributes
func convertTagsToAttributes(tags map[string]string) []attribute.KeyValue {
	if tags == nil {
		return nil
	}
	attrs := make([]attribute.KeyValue, 0, len(tags))
	for k, v := range tags {
		attrs = append(attrs, attribute.String(k, v))
	}
	return attrs
}
