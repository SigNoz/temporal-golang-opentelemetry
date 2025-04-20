package instrument

import (
	"context"
	"fmt"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	commonpb "go.temporal.io/api/common/v1"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/interceptor"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/workflow"
)

const (
	workflowIDTagKey   = "temporalWorkflowID"
	runIDTagKey        = "temporalRunID"
	activityIDTagKey   = "temporalActivityID"
	updateIDTagKey     = "temporalUpdateID"
	activityTypeTagKey = "temporalActivityType"
	workflowTypeTagKey = "temporalWorkflowType"
	namespaceTagKey    = "temporalNamespace"
	taskQueueTagKey    = "temporalTaskQueue"
)

// DefaultTextMapPropagator is the default OpenTelemetry TextMapPropagator used
// by this implementation if not otherwise set in TracerOptions.
var DefaultTextMapPropagator = propagation.NewCompositeTextMapPropagator(
	propagation.TraceContext{},
	propagation.Baggage{},
)

// Tracer is an interface for tracing implementations as used by
// NewTracingInterceptor. Most callers do not use this directly, but rather use
// the opentracing or opentelemetry packages.
//
// All implementations must embed BaseTracer to safely
// handle future changes.
type Tracer interface {
	// Options returns the options for the tracer. This is only called once on
	// initialization.
	Options() TracerOptions

	// UnmarshalSpan unmarshals the given map into a span reference.
	UnmarshalSpan(map[string]string) (TracerSpanRef, error)

	// MarshalSpan marshals the given span into a map. If the map is empty with no
	// error, the span is simply not set.
	MarshalSpan(TracerSpan) (map[string]string, error)

	// SpanFromContext returns the span from the general Go context or nil if not
	// present.
	SpanFromContext(context.Context) TracerSpan

	// ContextWithSpan creates a general Go context with the given span set.
	ContextWithSpan(context.Context, TracerSpan) context.Context

	// StartSpan starts and returns a span with the given options.
	StartSpan(*TracerStartSpanOptions) (TracerSpan, error)

	// GetLogger returns a log.Logger which may include additional fields in its
	// output in order to support correlation of tracing and log data.
	GetLogger(log.Logger, TracerSpanRef) log.Logger
	// SpanName can be used to give a custom name to a Span according to the input TracerStartSpanOptions,
	// or the decision can be deferred to the BaseTracer implementation.
	SpanName(options *TracerStartSpanOptions) string

	mustEmbedBaseTracer()
}

// BaseTracer is a default implementation of Tracer meant for embedding.
type BaseTracer struct{}

func (BaseTracer) GetLogger(logger log.Logger, ref TracerSpanRef) log.Logger {
	return logger
}
func (BaseTracer) SpanName(options *TracerStartSpanOptions) string {
	return fmt.Sprintf("%s:%s", options.Operation, options.Name)
}

//lint:ignore U1000 Ignore unused method; it is only required to implement the Tracer interface but will never be called.
func (BaseTracer) mustEmbedBaseTracer() {}

// TracerOptions are options provided to NewTracingInterceptor or NewTracer.
type TracerOptions struct {
	// Tracer is the tracer to use. If not set, one is obtained from the global
	// tracer provider using the name "temporal-sdk-go".
	Tracer trace.Tracer

	// DisableSignalTracing can be set to disable signal tracing.
	DisableSignalTracing bool

	// DisableQueryTracing can be set to disable query tracing.
	DisableQueryTracing bool

	// DisableBaggage can be set to disable baggage propagation.
	DisableBaggage bool

	// AllowInvalidParentSpans will swallow errors interpreting parent
	// spans from headers. Useful when migrating from one tracing library
	// to another, while workflows/activities may be in progress.
	AllowInvalidParentSpans bool

	// TextMapPropagator is the propagator to use for serializing spans. If not
	// set, this uses DefaultTextMapPropagator, not the OpenTelemetry global one.
	// To use the OpenTelemetry global one, set this value to the result of the
	// global call.
	TextMapPropagator propagation.TextMapPropagator

	// SpanContextKey is the context key used for internal span tracking (not to
	// be confused with the context key OpenTelemetry uses internally). If not
	// set, this defaults to an internal key (recommended).
	SpanContextKey interface{}

	// HeaderKey is the Temporal header field key used to serialize spans. If
	// empty, this defaults to the one used by all SDKs (recommended).
	HeaderKey string

	// SpanStarter is a callback to create spans. If not set, this creates normal
	// OpenTelemetry spans calling Tracer.Start.
	SpanStarter func(ctx context.Context, t trace.Tracer, spanName string, opts ...trace.SpanStartOption) trace.Span
}

// TracerStartSpanOptions are options for Tracer.StartSpan.
type TracerStartSpanOptions struct {
	// Parent is the optional parent reference of the span.
	Parent TracerSpanRef
	// Operation is the general operation name without the specific name.
	Operation string

	// Name is the specific activity, workflow, etc for the operation.
	Name string

	// Time indicates the start time of the span.
	//
	// For RunWorkflow and RunActivity operation types, this will match workflow.Info.WorkflowStartTime and
	// activity.Info.StartedTime respectively. All other operations use time.Now().
	Time time.Time

	// DependedOn is true if the parent depends on this span or false if it just
	// is related to the parent. In OpenTracing terms, this is true for "ChildOf"
	// reference types and false for "FollowsFrom" reference types.
	DependedOn bool

	// Tags are a set of span tags.
	Tags map[string]string

	// FromHeader is used internally, not by tracer implementations, to determine
	// whether the parent span can be retrieved from the Temporal header.
	FromHeader bool

	// ToHeader is used internally, not by tracer implementations, to determine
	// whether the span should be placed on the Temporal header.
	ToHeader bool

	// IdempotencyKey may optionally be used by tracing implementations to generate
	// deterministic span IDs.
	//
	// This is useful in workflow contexts where spans may need to be "resumed" before
	// ultimately being reported. Generating a deterministic span ID ensures that any
	// child spans created before the parent span is resumed do not become orphaned.
	//
	// IdempotencyKey is not guaranteed to be set for all operations; Tracer
	// implementations MUST therefore ignore zero values for this field.
	//
	// IdempotencyKey should be treated as opaque data by Tracer implementations.
	// Do not attempt to parse it, as the format is subject to change.
	IdempotencyKey string
}

type spanContextKey struct{}

const defaultHeaderKey = "_tracer-data"

type tracer struct {
	BaseTracer
	options *TracerOptions
}

// NewTracer creates a tracer with the given options. Most callers should use
// NewTracingInterceptor instead.
func NewTracer(options TracerOptions) (Tracer, error) {
	if options.Tracer == nil {
		options.Tracer = otel.GetTracerProvider().Tracer("temporal-sdk-go")
	}
	if options.TextMapPropagator == nil {
		options.TextMapPropagator = DefaultTextMapPropagator
	}
	if options.SpanContextKey == nil {
		options.SpanContextKey = spanContextKey{}
	}
	if options.HeaderKey == "" {
		options.HeaderKey = defaultHeaderKey
	}
	if options.SpanStarter == nil {
		options.SpanStarter = func(
			ctx context.Context,
			t trace.Tracer,
			spanName string,
			opts ...trace.SpanStartOption,
		) trace.Span {
			_, span := t.Start(ctx, spanName, opts...)
			return span
		}
	}
	return &tracer{options: &options}, nil
}

func (t *tracer) UnmarshalSpan(m map[string]string) (TracerSpanRef, error) {
	ctx := t.options.TextMapPropagator.Extract(context.Background(), textMapCarrier(m))
	spanCtx := trace.SpanContextFromContext(ctx)
	if !spanCtx.IsValid() {
		return nil, fmt.Errorf("failed extracting OpenTelemetry span from map")
	}
	spanRef := &tracerSpanRef{SpanContext: spanCtx}
	if !t.options.DisableBaggage {
		spanRef.Baggage = baggage.FromContext(ctx)
	}
	return spanRef, nil
}

func (t *tracer) MarshalSpan(span TracerSpan) (map[string]string, error) {
	data := textMapCarrier{}
	tSpan := span.(*tracerSpan)
	ctx := context.Background()
	if !t.options.DisableBaggage {
		ctx = baggage.ContextWithBaggage(ctx, tSpan.Baggage)
	}
	t.options.TextMapPropagator.Inject(trace.ContextWithSpan(ctx, tSpan.Span), data)
	return data, nil
}

func (t *tracer) SpanFromContext(ctx context.Context) TracerSpan {
	span := trace.SpanFromContext(ctx)
	if !span.SpanContext().IsValid() {
		return nil
	}
	tSpan := &tracerSpan{Span: span}
	if !t.options.DisableBaggage {
		tSpan.Baggage = baggage.FromContext(ctx)
	}
	return tSpan
}

func (t *tracer) ContextWithSpan(ctx context.Context, span TracerSpan) context.Context {
	if !t.options.DisableBaggage {
		ctx = baggage.ContextWithBaggage(ctx, span.(*tracerSpan).Baggage)
	}
	return trace.ContextWithSpan(ctx, span.(*tracerSpan).Span)
}

func (t *tracer) StartSpan(opts *TracerStartSpanOptions) (TracerSpan, error) {
	// Create context with parent
	var parent trace.SpanContext
	var bag baggage.Baggage
	switch optParent := opts.Parent.(type) {
	case nil:
	case *tracerSpan:
		parent = optParent.SpanContext()
		bag = optParent.Baggage
	case *tracerSpanRef:
		parent = optParent.SpanContext
		bag = optParent.Baggage
	default:
		return nil, fmt.Errorf("unrecognized parent type %T", optParent)
	}
	ctx := context.Background()
	if parent.IsValid() {
		ctx = trace.ContextWithSpanContext(ctx, parent)
		if !t.options.DisableBaggage {
			ctx = baggage.ContextWithBaggage(ctx, bag)
		}
	}

	// Create span
	span := t.options.SpanStarter(ctx, t.options.Tracer, opts.Operation+":"+opts.Name, trace.WithTimestamp(opts.Time))

	// Set tags
	if len(opts.Tags) > 0 {
		attrs := make([]attribute.KeyValue, 0, len(opts.Tags))
		for k, v := range opts.Tags {
			attrs = append(attrs, attribute.String(k, v))
		}
		span.SetAttributes(attrs...)
	}

	tSpan := &tracerSpan{Span: span}
	if !t.options.DisableBaggage {
		tSpan.Baggage = bag
	}

	return tSpan, nil
}

func (t *tracer) GetLogger(logger log.Logger, ref TracerSpanRef) log.Logger {
	span, ok := ref.(*tracerSpan)
	if !ok {
		return logger
	}

	logger = log.With(logger,
		"TraceID", span.SpanContext().TraceID(),
		"SpanID", span.SpanContext().SpanID(),
	)

	return logger
}

// TracerSpanRef represents a span reference such as a parent.
type TracerSpanRef interface {
}

// TracerSpan represents a span.
type TracerSpan interface {
	TracerSpanRef

	// Finish is called when the span is complete.
	Finish(*TracerFinishSpanOptions)
}

// TracerFinishSpanOptions are options for TracerSpan.Finish.
type TracerFinishSpanOptions struct {
	// Error is present if there was an error in the code traced by this specific
	// span.
	Error error
}

type tracingInterceptor struct {
	interceptor.InterceptorBase
	tracer  Tracer
	options TracerOptions
}

// NewTracingInterceptor creates an interceptor for setting on client options
// that implements OpenTelemetry tracing for workflows.
func NewTracingInterceptor(options TracerOptions) (interceptor.Interceptor, error) {
	t, err := NewTracer(options)
	if err != nil {
		return nil, err
	}

	// Initialize SpanContextKey if not set
	if options.SpanContextKey == nil {
		options.SpanContextKey = struct{}{}
	}

	// Create our own interceptor implementation
	return &tracingInterceptor{
		tracer:  t,
		options: options,
	}, nil
}

func (t *tracingInterceptor) InterceptClient(next interceptor.ClientOutboundInterceptor) interceptor.ClientOutboundInterceptor {
	i := &tracingClientOutboundInterceptor{root: t}
	i.Next = next
	return i
}

func (t *tracingInterceptor) InterceptActivity(
	ctx context.Context,
	next interceptor.ActivityInboundInterceptor,
) interceptor.ActivityInboundInterceptor {
	i := &tracingActivityInboundInterceptor{root: t}
	i.Next = next
	return i
}

func (t *tracingInterceptor) InterceptWorkflow(
	ctx workflow.Context,
	next interceptor.WorkflowInboundInterceptor,
) interceptor.WorkflowInboundInterceptor {
	i := &tracingWorkflowInboundInterceptor{root: t, info: workflow.GetInfo(ctx)}
	i.Next = next
	return i
}

func (t *tracingInterceptor) InterceptNexusOperation(
	ctx context.Context,
	next interceptor.NexusOperationInboundInterceptor,
) interceptor.NexusOperationInboundInterceptor {
	i := &tracingNexusOperationInboundInterceptor{root: t}
	i.Next = next
	return i
}

type tracingClientOutboundInterceptor struct {
	interceptor.ClientOutboundInterceptorBase
	root *tracingInterceptor
}

func (t *tracingClientOutboundInterceptor) CreateSchedule(ctx context.Context, in *interceptor.ScheduleClientCreateInput) (client.ScheduleHandle, error) {
	// Start span and write to header
	span, ctx, err := t.root.startSpanFromContext(ctx, &TracerStartSpanOptions{
		Operation: "CreateSchedule",
		Name:      in.Options.ID,
		ToHeader:  true,
		Time:      time.Now(),
	}, t.root.headerReader(ctx), t.root.headerWriter(ctx))
	if err != nil {
		return nil, err
	}
	var finishOpts TracerFinishSpanOptions
	defer span.Finish(&finishOpts)

	run, err := t.Next.CreateSchedule(ctx, in)
	finishOpts.Error = err
	return run, err
}

func (t *tracingClientOutboundInterceptor) ExecuteWorkflow(
	ctx context.Context,
	in *interceptor.ClientExecuteWorkflowInput,
) (client.WorkflowRun, error) {
	// Start span and write to header
	span, ctx, err := t.root.startSpanFromContext(ctx, &TracerStartSpanOptions{
		Operation: "StartWorkflow",
		Name:      in.WorkflowType,
		Tags: map[string]string{
			workflowIDTagKey:   in.Options.ID,
			workflowTypeTagKey: in.WorkflowType,
			taskQueueTagKey:    in.Options.TaskQueue,
		},
		ToHeader: true,
		Time:     time.Now(),
	}, t.root.headerReader(ctx), t.root.headerWriter(ctx))
	if err != nil {
		return nil, err
	}
	var finishOpts TracerFinishSpanOptions
	defer span.Finish(&finishOpts)

	run, err := t.Next.ExecuteWorkflow(ctx, in)
	finishOpts.Error = err
	return run, err
}

func (t *tracingClientOutboundInterceptor) SignalWorkflow(ctx context.Context, in *interceptor.ClientSignalWorkflowInput) error {
	// Only add tracing if enabled
	if t.root.options.DisableSignalTracing {
		return t.Next.SignalWorkflow(ctx, in)
	}
	// Start span and write to header
	span, ctx, err := t.root.startSpanFromContext(ctx, &TracerStartSpanOptions{
		Operation: "SignalWorkflow",
		Name:      in.SignalName,
		Tags: map[string]string{
			workflowIDTagKey: in.WorkflowID,
			runIDTagKey:      in.RunID,
		},
		ToHeader: true,
		Time:     time.Now(),
	}, t.root.headerReader(ctx), t.root.headerWriter(ctx))
	if err != nil {
		return err
	}
	var finishOpts TracerFinishSpanOptions
	defer span.Finish(&finishOpts)

	err = t.Next.SignalWorkflow(ctx, in)
	finishOpts.Error = err
	return err
}

func (t *tracingClientOutboundInterceptor) SignalWithStartWorkflow(
	ctx context.Context,
	in *interceptor.ClientSignalWithStartWorkflowInput,
) (client.WorkflowRun, error) {
	// Start span and write to header
	span, ctx, err := t.root.startSpanFromContext(ctx, &TracerStartSpanOptions{
		Operation: "SignalWithStartWorkflow",
		Name:      in.WorkflowType,
		Tags:      map[string]string{workflowIDTagKey: in.Options.ID, taskQueueTagKey: in.Options.TaskQueue, workflowTypeTagKey: in.WorkflowType},
		ToHeader:  true,
	}, t.root.headerReader(ctx), t.root.headerWriter(ctx))
	if err != nil {
		return nil, err
	}
	var finishOpts TracerFinishSpanOptions
	defer span.Finish(&finishOpts)

	run, err := t.Next.SignalWithStartWorkflow(ctx, in)
	finishOpts.Error = err
	return run, err
}

func (t *tracingClientOutboundInterceptor) QueryWorkflow(
	ctx context.Context,
	in *interceptor.ClientQueryWorkflowInput,
) (converter.EncodedValue, error) {
	// Only add tracing if enabled
	if t.root.options.DisableQueryTracing {
		return t.Next.QueryWorkflow(ctx, in)
	}
	// Start span and write to header
	span, ctx, err := t.root.startSpanFromContext(ctx, &TracerStartSpanOptions{
		Operation: "QueryWorkflow",
		Name:      in.QueryType,
		Tags:      map[string]string{workflowIDTagKey: in.WorkflowID, runIDTagKey: in.RunID},
		ToHeader:  true,
		Time:      time.Now(),
	}, t.root.headerReader(ctx), t.root.headerWriter(ctx))
	if err != nil {
		return nil, err
	}
	var finishOpts TracerFinishSpanOptions
	defer span.Finish(&finishOpts)

	val, err := t.Next.QueryWorkflow(ctx, in)
	finishOpts.Error = err
	return val, err
}

func (t *tracingClientOutboundInterceptor) UpdateWorkflow(
	ctx context.Context,
	in *interceptor.ClientUpdateWorkflowInput,
) (client.WorkflowUpdateHandle, error) {
	// Only add tracing if enabled
	if t.root.options.DisableSignalTracing {
		return t.Next.UpdateWorkflow(ctx, in)
	}
	// Start span and write to header
	span, ctx, err := t.root.startSpanFromContext(ctx, &TracerStartSpanOptions{
		Operation: "UpdateWorkflow",
		Name:      in.UpdateName,
		Tags:      map[string]string{workflowIDTagKey: in.WorkflowID, runIDTagKey: in.RunID},
		ToHeader:  true,
		Time:      time.Now(),
	}, t.root.headerReader(ctx), t.root.headerWriter(ctx))
	if err != nil {
		return nil, err
	}
	var finishOpts TracerFinishSpanOptions
	defer span.Finish(&finishOpts)

	val, err := t.Next.UpdateWorkflow(ctx, in)
	finishOpts.Error = err
	return val, err
}

func (t *tracingClientOutboundInterceptor) UpdateWithStartWorkflow(
	ctx context.Context,
	in *interceptor.ClientUpdateWithStartWorkflowInput,
) (client.WorkflowUpdateHandle, error) {
	// Only add tracing if enabled
	if t.root.options.DisableSignalTracing {
		return t.Next.UpdateWithStartWorkflow(ctx, in)
	}
	// Start span and write to header
	span, ctx, err := t.root.startSpanFromContext(ctx, &TracerStartSpanOptions{
		Operation: "UpdateWithStartWorkflow",
		Name:      in.UpdateOptions.UpdateName,
		Tags:      map[string]string{workflowIDTagKey: in.UpdateOptions.WorkflowID, updateIDTagKey: in.UpdateOptions.UpdateID, runIDTagKey: in.UpdateOptions.RunID},
		ToHeader:  true,
		Time:      time.Now(),
	}, t.root.headerReader(ctx), t.root.headerWriter(ctx))
	if err != nil {
		return nil, err
	}
	var finishOpts TracerFinishSpanOptions
	defer span.Finish(&finishOpts)

	val, err := t.Next.UpdateWithStartWorkflow(ctx, in)
	finishOpts.Error = err
	return val, err
}

type tracingActivityOutboundInterceptor struct {
	interceptor.ActivityOutboundInterceptorBase
	root *tracingInterceptor
}

func (t *tracingActivityOutboundInterceptor) GetLogger(ctx context.Context) log.Logger {
	if span := t.root.tracer.SpanFromContext(ctx); span != nil {
		return t.root.tracer.GetLogger(t.Next.GetLogger(ctx), span)
	}
	return t.Next.GetLogger(ctx)
}

type tracingActivityInboundInterceptor struct {
	interceptor.ActivityInboundInterceptorBase
	root *tracingInterceptor
}

func (t *tracingActivityInboundInterceptor) Init(outbound interceptor.ActivityOutboundInterceptor) error {
	i := &tracingActivityOutboundInterceptor{root: t.root}
	i.Next = outbound
	return t.Next.Init(i)
}

func (t *tracingActivityInboundInterceptor) ExecuteActivity(
	ctx context.Context,
	in *interceptor.ExecuteActivityInput,
) (interface{}, error) {
	// Start span reading from header
	info := activity.GetInfo(ctx)
	span, ctx, err := t.root.startSpanFromContext(ctx, &TracerStartSpanOptions{
		Operation:  "RunActivity",
		Name:       info.ActivityType.Name,
		DependedOn: true,
		Tags: map[string]string{
			workflowIDTagKey:   info.WorkflowExecution.ID,
			runIDTagKey:        info.WorkflowExecution.RunID,
			activityIDTagKey:   info.ActivityID,
			activityTypeTagKey: info.ActivityType.Name,
			namespaceTagKey:    info.WorkflowNamespace,
			taskQueueTagKey:    info.TaskQueue,
			workflowTypeTagKey: info.WorkflowType.Name,
		},
		FromHeader: true,
		Time:       info.StartedTime,
	}, t.root.headerReader(ctx), t.root.headerWriter(ctx))
	if err != nil {
		return nil, err
	}
	var finishOpts TracerFinishSpanOptions
	defer span.Finish(&finishOpts)

	ret, err := t.Next.ExecuteActivity(ctx, in)
	finishOpts.Error = err
	return ret, err
}

type tracingWorkflowInboundInterceptor struct {
	interceptor.WorkflowInboundInterceptorBase
	root        *tracingInterceptor
	spanCounter uint16
	info        *workflow.Info
}

// newIdempotencyKey returns a new idempotency key by incrementing the span counter and interpolating
// this new value into a string that includes the workflow namespace/id/run id and the interceptor type.
func (t *tracingWorkflowInboundInterceptor) newIdempotencyKey() string {
	t.spanCounter++
	return fmt.Sprintf("WorkflowInboundInterceptor:%s:%s:%s:%d",
		t.info.Namespace,
		t.info.WorkflowExecution.ID,
		t.info.WorkflowExecution.RunID,
		t.spanCounter)
}

func (t *tracingWorkflowInboundInterceptor) Init(outbound interceptor.WorkflowOutboundInterceptor) error {
	i := &tracingWorkflowOutboundInterceptor{root: t.root}
	i.Next = outbound
	return t.Next.Init(i)
}

func (t *tracingWorkflowInboundInterceptor) ExecuteWorkflow(
	ctx workflow.Context,
	in *interceptor.ExecuteWorkflowInput,
) (interface{}, error) {
	// Start span reading from header

	span, ctx, err := t.root.startSpanFromWorkflowContext(ctx, &TracerStartSpanOptions{
		Operation: "RunWorkflow",
		Name:      t.info.WorkflowType.Name,
		Tags: map[string]string{
			workflowIDTagKey:   t.info.WorkflowExecution.ID,
			runIDTagKey:        t.info.WorkflowExecution.RunID,
			workflowTypeTagKey: t.info.WorkflowType.Name,
			namespaceTagKey:    t.info.Namespace,
			taskQueueTagKey:    t.info.TaskQueueName,
		},
		FromHeader:     true,
		Time:           t.info.WorkflowStartTime,
		IdempotencyKey: t.newIdempotencyKey(),
	}, t.root.workflowHeaderReader(ctx), t.root.workflowHeaderWriter(ctx))
	if err != nil {
		return nil, err
	}
	var finishOpts TracerFinishSpanOptions
	defer span.Finish(&finishOpts)

	ret, err := t.Next.ExecuteWorkflow(ctx, in)
	finishOpts.Error = err
	return ret, err
}

func (t *tracingWorkflowInboundInterceptor) HandleSignal(ctx workflow.Context, in *interceptor.HandleSignalInput) error {
	// Only add tracing if enabled and not replaying
	if t.root.options.DisableSignalTracing || workflow.IsReplaying(ctx) {
		return t.Next.HandleSignal(ctx, in)
	}
	// Start span reading from header
	info := workflow.GetInfo(ctx)
	span, ctx, err := t.root.startSpanFromWorkflowContext(ctx, &TracerStartSpanOptions{
		Operation: "HandleSignal",
		Name:      in.SignalName,
		Tags: map[string]string{
			workflowIDTagKey:   info.WorkflowExecution.ID,
			runIDTagKey:        info.WorkflowExecution.RunID,
			workflowTypeTagKey: info.WorkflowType.Name,
			namespaceTagKey:    info.Namespace,
			taskQueueTagKey:    info.TaskQueueName,
		},
		FromHeader:     true,
		Time:           time.Now(),
		IdempotencyKey: t.newIdempotencyKey(),
	}, t.root.workflowHeaderReader(ctx), t.root.workflowHeaderWriter(ctx))
	if err != nil {
		return err
	}
	var finishOpts TracerFinishSpanOptions
	defer span.Finish(&finishOpts)

	err = t.Next.HandleSignal(ctx, in)
	finishOpts.Error = err
	return err
}

func (t *tracingWorkflowInboundInterceptor) HandleQuery(
	ctx workflow.Context,
	in *interceptor.HandleQueryInput,
) (interface{}, error) {
	// Only add tracing if enabled and not replaying
	if t.root.options.DisableQueryTracing || workflow.IsReplaying(ctx) {
		return t.Next.HandleQuery(ctx, in)
	}
	// Start span reading from header
	info := workflow.GetInfo(ctx)
	span, ctx, err := t.root.startSpanFromWorkflowContext(ctx, &TracerStartSpanOptions{
		Operation: "HandleQuery",
		Name:      in.QueryType,
		Tags: map[string]string{
			workflowIDTagKey:   info.WorkflowExecution.ID,
			runIDTagKey:        info.WorkflowExecution.RunID,
			workflowTypeTagKey: info.WorkflowType.Name,
			namespaceTagKey:    info.Namespace,
			taskQueueTagKey:    info.TaskQueueName,
		},
		FromHeader: true,
		Time:       time.Now(),
		// We intentionally do not set IdempotencyKey here because queries are not recorded in
		// workflow history. When the tracing interceptor's span counter is reset between workflow
		// replays, old queries will not be processed which could result in idempotency key
		// collisions with other queries or signals.
	}, t.root.workflowHeaderReader(ctx), t.root.workflowHeaderWriter(ctx))
	if err != nil {
		return nil, err
	}
	var finishOpts TracerFinishSpanOptions
	defer span.Finish(&finishOpts)

	val, err := t.Next.HandleQuery(ctx, in)
	finishOpts.Error = err
	return val, err
}

func (t *tracingWorkflowInboundInterceptor) ValidateUpdate(
	ctx workflow.Context,
	in *interceptor.UpdateInput,
) error {
	// Only add tracing if enabled and not replaying
	if t.root.options.DisableSignalTracing {
		return t.Next.ValidateUpdate(ctx, in)
	}
	// Start span reading from header
	info := workflow.GetInfo(ctx)
	currentUpdateInfo := workflow.GetCurrentUpdateInfo(ctx)
	span, ctx, err := t.root.startSpanFromWorkflowContext(ctx, &TracerStartSpanOptions{
		Operation: "ValidateUpdate",
		Name:      in.Name,
		Tags: map[string]string{
			workflowIDTagKey:   info.WorkflowExecution.ID,
			runIDTagKey:        info.WorkflowExecution.RunID,
			updateIDTagKey:     currentUpdateInfo.ID,
			workflowTypeTagKey: info.WorkflowType.Name,
			namespaceTagKey:    info.Namespace,
			taskQueueTagKey:    info.TaskQueueName,
		},
		FromHeader: true,
		Time:       time.Now(),
		// We intentionally do not set IdempotencyKey here because validation is not run on
		// replay. When the tracing interceptor's span counter is reset between workflow
		// replays, the validator will not be processed which could result in impotency key
		// collisions with other requests.
	}, t.root.workflowHeaderReader(ctx), t.root.workflowHeaderWriter(ctx))
	if err != nil {
		return err
	}
	var finishOpts TracerFinishSpanOptions
	defer span.Finish(&finishOpts)

	err = t.Next.ValidateUpdate(ctx, in)
	finishOpts.Error = err
	return err
}

func (t *tracingWorkflowInboundInterceptor) ExecuteUpdate(
	ctx workflow.Context,
	in *interceptor.UpdateInput,
) (interface{}, error) {
	// Only add tracing if enabled and not replaying
	if t.root.options.DisableSignalTracing {
		return t.Next.ExecuteUpdate(ctx, in)
	}
	// Start span reading from header
	info := workflow.GetInfo(ctx)
	currentUpdateInfo := workflow.GetCurrentUpdateInfo(ctx)
	span, ctx, err := t.root.startSpanFromWorkflowContext(ctx, &TracerStartSpanOptions{
		// Using operation name "HandleUpdate" to match other SDKs and by consistence with other operations
		Operation: "HandleUpdate",
		Name:      in.Name,
		Tags: map[string]string{
			workflowIDTagKey:   info.WorkflowExecution.ID,
			runIDTagKey:        info.WorkflowExecution.RunID,
			updateIDTagKey:     currentUpdateInfo.ID,
			workflowTypeTagKey: t.info.WorkflowType.Name,
			namespaceTagKey:    t.info.Namespace,
			taskQueueTagKey:    t.info.TaskQueueName,
		},
		FromHeader:     true,
		Time:           time.Now(),
		IdempotencyKey: t.newIdempotencyKey(),
	}, t.root.workflowHeaderReader(ctx), t.root.workflowHeaderWriter(ctx))
	if err != nil {
		return nil, err
	}
	var finishOpts TracerFinishSpanOptions
	defer span.Finish(&finishOpts)

	val, err := t.Next.ExecuteUpdate(ctx, in)
	finishOpts.Error = err
	return val, err
}

type tracingWorkflowOutboundInterceptor struct {
	interceptor.WorkflowOutboundInterceptorBase
	root *tracingInterceptor
}

func (t *tracingWorkflowOutboundInterceptor) ExecuteActivity(
	ctx workflow.Context,
	activityType string,
	args ...interface{},
) workflow.Future {
	// Start span writing to header
	span, ctx, err := t.startNonReplaySpan(ctx, "StartActivity", activityType, true, t.root.workflowHeaderWriter(ctx))
	if err != nil {
		return err
	}
	defer span.Finish(&TracerFinishSpanOptions{})

	return t.Next.ExecuteActivity(ctx, activityType, args...)
}

func (t *tracingWorkflowOutboundInterceptor) ExecuteLocalActivity(
	ctx workflow.Context,
	activityType string,
	args ...interface{},
) workflow.Future {
	// Start span writing to header
	span, ctx, err := t.startNonReplaySpan(ctx, "StartActivity", activityType, true, t.root.workflowHeaderWriter(ctx))
	if err != nil {
		return err
	}
	defer span.Finish(&TracerFinishSpanOptions{})

	return t.Next.ExecuteLocalActivity(ctx, activityType, args...)
}

func (t *tracingWorkflowOutboundInterceptor) GetLogger(ctx workflow.Context) log.Logger {
	if span, _ := ctx.Value(t.root.options.SpanContextKey).(TracerSpan); span != nil {
		return t.root.tracer.GetLogger(t.Next.GetLogger(ctx), span)
	}
	return t.Next.GetLogger(ctx)
}

func (t *tracingWorkflowOutboundInterceptor) ExecuteChildWorkflow(
	ctx workflow.Context,
	childWorkflowType string,
	args ...interface{},
) workflow.ChildWorkflowFuture {
	// Start span writing to header
	span, ctx, futErr := t.startNonReplaySpan(ctx, "StartChildWorkflow", childWorkflowType, false, t.root.workflowHeaderWriter(ctx))
	if futErr != nil {
		return childWorkflowFuture{futErr}
	}
	defer span.Finish(&TracerFinishSpanOptions{})

	return t.Next.ExecuteChildWorkflow(ctx, childWorkflowType, args...)
}

func (t *tracingWorkflowOutboundInterceptor) SignalExternalWorkflow(
	ctx workflow.Context,
	workflowID string,
	runID string,
	signalName string,
	arg interface{},
) workflow.Future {
	// Start span writing to header if enabled
	if !t.root.options.DisableSignalTracing {
		var span TracerSpan
		var futErr workflow.Future
		span, ctx, futErr = t.startNonReplaySpan(ctx, "SignalExternalWorkflow", signalName, false, t.root.workflowHeaderWriter(ctx))
		if futErr != nil {
			return futErr
		}
		defer span.Finish(&TracerFinishSpanOptions{})
	}

	return t.Next.SignalExternalWorkflow(ctx, workflowID, runID, signalName, arg)
}

func (t *tracingWorkflowOutboundInterceptor) SignalChildWorkflow(
	ctx workflow.Context,
	workflowID string,
	signalName string,
	arg interface{},
) workflow.Future {
	// Start span writing to header if enabled
	if !t.root.options.DisableSignalTracing {
		var span TracerSpan
		var futErr workflow.Future
		span, ctx, futErr = t.startNonReplaySpan(ctx, "SignalChildWorkflow", signalName, false, t.root.workflowHeaderWriter(ctx))
		if futErr != nil {
			return futErr
		}
		defer span.Finish(&TracerFinishSpanOptions{})
	}

	return t.Next.SignalChildWorkflow(ctx, workflowID, signalName, arg)
}

func (t *tracingWorkflowOutboundInterceptor) ExecuteNexusOperation(ctx workflow.Context, input interceptor.ExecuteNexusOperationInput) workflow.NexusOperationFuture {
	// Start span writing to header
	var ok bool
	var operationName string
	if operationName, ok = input.Operation.(string); ok {
	} else if regOp, ok := input.Operation.(interface{ Name() string }); ok {
		operationName = regOp.Name()
	} else {
		return nexusOperationFuture{workflowFutureFromErr(ctx, fmt.Errorf("unexpected operation type: %v", input.Operation))}
	}
	span, ctx, futErr := t.startNonReplaySpan(ctx, "StartNexusOperation", input.Client.Service()+"/"+operationName, false, t.root.nexusHeaderWriter(input.NexusHeader))
	if futErr != nil {
		return nexusOperationFuture{futErr}
	}
	defer span.Finish(&TracerFinishSpanOptions{})

	return t.Next.ExecuteNexusOperation(ctx, input)
}

func (t *tracingWorkflowOutboundInterceptor) NewContinueAsNewError(
	ctx workflow.Context,
	wfn interface{},
	args ...interface{},
) error {
	err := t.Next.NewContinueAsNewError(ctx, wfn, args...)
	if !workflow.IsReplaying(ctx) {
		if contErr, _ := err.(*workflow.ContinueAsNewError); contErr != nil {
			// Get the current span and write header
			if span, _ := ctx.Value(t.root.options.SpanContextKey).(TracerSpan); span != nil {
				if writeErr := t.root.writeSpanToHeader(span, interceptor.WorkflowHeader(ctx)); writeErr != nil {
					return fmt.Errorf("failed writing span when creating continue as new error: %w", writeErr)
				}
			}
		}
	}
	return err
}

type nopSpan struct{}

func (nopSpan) Finish(*TracerFinishSpanOptions) {}

// Span always returned, even in replay. futErr is non-nil on error.
func (t *tracingWorkflowOutboundInterceptor) startNonReplaySpan(
	ctx workflow.Context,
	operation string,
	name string,
	dependedOn bool,
	headerWriter func(TracerSpan) error,
) (span TracerSpan, newCtx workflow.Context, futErr workflow.Future) {
	// Noop span if replaying
	if workflow.IsReplaying(ctx) {
		return nopSpan{}, ctx, nil
	}
	info := workflow.GetInfo(ctx)
	span, newCtx, err := t.root.startSpanFromWorkflowContext(ctx, &TracerStartSpanOptions{
		Operation:  operation,
		Name:       name,
		DependedOn: dependedOn,
		Tags: map[string]string{
			workflowIDTagKey:   info.WorkflowExecution.ID,
			runIDTagKey:        info.WorkflowExecution.RunID,
			namespaceTagKey:    info.ParentWorkflowNamespace,
			workflowTypeTagKey: info.WorkflowType.Name,
			taskQueueTagKey:    info.TaskQueueName,
		},
		ToHeader: true,
		Time:     time.Now(),
	}, t.root.workflowHeaderReader(ctx), headerWriter)
	if err != nil {
		return nopSpan{}, ctx, workflowFutureFromErr(ctx, err)
	}
	return span, newCtx, nil
}

type tracingNexusOperationInboundInterceptor struct {
	interceptor.NexusOperationInboundInterceptorBase
	root *tracingInterceptor
}

// CancelOperation implements internal.NexusOperationInboundInterceptor.
func (t *tracingNexusOperationInboundInterceptor) CancelOperation(ctx context.Context, input interceptor.NexusCancelOperationInput) error {
	info := nexus.ExtractHandlerInfo(ctx)
	// Start span reading from header
	span, ctx, err := t.root.startSpanFromContext(ctx, &TracerStartSpanOptions{
		Operation:  "RunCancelNexusOperationHandler",
		Name:       info.Service + "/" + info.Operation,
		DependedOn: true,
		FromHeader: true,
		Time:       time.Now(),
	}, t.root.nexusHeaderReader(input.Options.Header), t.root.headerWriter(ctx))
	if err != nil {
		return err
	}
	var finishOpts TracerFinishSpanOptions
	defer span.Finish(&finishOpts)

	err = t.Next.CancelOperation(ctx, input)
	finishOpts.Error = err
	return err
}

// StartOperation implements internal.NexusOperationInboundInterceptor.
func (t *tracingNexusOperationInboundInterceptor) StartOperation(ctx context.Context, input interceptor.NexusStartOperationInput) (nexus.HandlerStartOperationResult[any], error) {
	info := nexus.ExtractHandlerInfo(ctx)
	// Start span reading from header
	span, ctx, err := t.root.startSpanFromContext(ctx, &TracerStartSpanOptions{
		Operation:  "RunStartNexusOperationHandler",
		Name:       info.Service + "/" + info.Operation,
		DependedOn: true,
		FromHeader: true,
		Time:       time.Now(),
	}, t.root.nexusHeaderReader(input.Options.Header), t.root.headerWriter(ctx))
	if err != nil {
		return nil, err
	}
	var finishOpts TracerFinishSpanOptions
	defer span.Finish(&finishOpts)

	ret, err := t.Next.StartOperation(ctx, input)
	finishOpts.Error = err
	return ret, err
}

func (t *tracingInterceptor) startSpanFromContext(
	ctx context.Context,
	options *TracerStartSpanOptions,
	headerReader func() (TracerSpanRef, error),
	headerWriter func(span TracerSpan) error,
) (TracerSpan, context.Context, error) {
	// Try to get parent from context
	options.Parent = t.tracer.SpanFromContext(ctx)
	span, err := t.startSpan(ctx, options, headerReader, headerWriter)
	if err != nil {
		return nil, nil, err
	}
	return span, t.tracer.ContextWithSpan(context.WithValue(ctx, t.options.SpanContextKey, span), span), nil
}

func (t *tracingInterceptor) startSpanFromWorkflowContext(
	ctx workflow.Context,
	options *TracerStartSpanOptions,
	headerReader func() (TracerSpanRef, error),
	headerWriter func(span TracerSpan) error,
) (TracerSpan, workflow.Context, error) {
	span, err := t.startSpan(ctx, options, headerReader, headerWriter)
	if err != nil {
		return nil, nil, err
	}
	return span, workflow.WithValue(ctx, t.options.SpanContextKey, span), nil
}

// Note, this does not put the span on the context
func (t *tracingInterceptor) startSpan(
	ctx interface{ Value(interface{}) interface{} },
	options *TracerStartSpanOptions,
	headerReader func() (TracerSpanRef, error),
	headerWriter func(span TracerSpan) error,
) (TracerSpan, error) {
	// Get parent span from header if not already present and allowed
	if options.Parent == nil && options.FromHeader {
		if span, err := headerReader(); err != nil && !t.options.AllowInvalidParentSpans {
			return nil, err
		} else if span != nil {
			options.Parent = span
		}
	}

	// If no parent span, try to get from context
	if options.Parent == nil {
		options.Parent, _ = ctx.Value(t.options.SpanContextKey).(TracerSpan)
	}

	// Start the span
	span, err := t.tracer.StartSpan(options)
	if err != nil {
		return nil, err
	}

	// Put span in header if wanted
	if options.ToHeader {
		if err := headerWriter(span); err != nil {
			return nil, err
		}
	}
	return span, nil
}

func (t *tracingInterceptor) headerReader(ctx context.Context) func() (TracerSpanRef, error) {
	header := interceptor.Header(ctx)
	return func() (TracerSpanRef, error) {
		return t.readSpanFromHeader(header)
	}
}

func (t *tracingInterceptor) headerWriter(ctx context.Context) func(TracerSpan) error {
	header := interceptor.Header(ctx)
	return func(span TracerSpan) error {
		return t.writeSpanToHeader(span, header)
	}
}

func (t *tracingInterceptor) workflowHeaderReader(ctx workflow.Context) func() (TracerSpanRef, error) {
	header := interceptor.WorkflowHeader(ctx)
	return func() (TracerSpanRef, error) {
		return t.readSpanFromHeader(header)
	}
}

func (t *tracingInterceptor) workflowHeaderWriter(ctx workflow.Context) func(TracerSpan) error {
	header := interceptor.WorkflowHeader(ctx)
	return func(span TracerSpan) error {
		return t.writeSpanToHeader(span, header)
	}
}

func (t *tracingInterceptor) nexusHeaderReader(header nexus.Header) func() (TracerSpanRef, error) {
	return func() (TracerSpanRef, error) {
		return t.readSpanFromNexusHeader(header)
	}
}

func (t *tracingInterceptor) nexusHeaderWriter(header nexus.Header) func(TracerSpan) error {
	return func(span TracerSpan) error {
		return t.writeSpanToNexusHeader(span, header)
	}
}

func (t *tracingInterceptor) readSpanFromHeader(header map[string]*commonpb.Payload) (TracerSpanRef, error) {
	// Get from map
	payload := header[t.options.HeaderKey]
	if payload == nil {
		return nil, nil
	}
	// Convert from the payload
	var data map[string]string
	if err := converter.GetDefaultDataConverter().FromPayload(payload, &data); err != nil {
		return nil, err
	}
	// Unmarshal
	return t.tracer.UnmarshalSpan(data)
}

func (t *tracingInterceptor) writeSpanToHeader(span TracerSpan, header map[string]*commonpb.Payload) error {
	// Serialize span to map
	data, err := t.tracer.MarshalSpan(span)
	if err != nil || len(data) == 0 {
		return err
	}
	// Convert to payload
	payload, err := converter.GetDefaultDataConverter().ToPayload(data)
	if err != nil {
		return err
	}
	// Put on header
	header[t.options.HeaderKey] = payload
	return nil
}

func (t *tracingInterceptor) writeSpanToNexusHeader(span TracerSpan, header nexus.Header) error {
	// Serialize span to map
	data, err := t.tracer.MarshalSpan(span)
	if err != nil || len(data) == 0 {
		return err
	}
	// Put on header
	for k, v := range data {
		header.Set(k, v)
	}
	return nil
}

func (t *tracingInterceptor) readSpanFromNexusHeader(header nexus.Header) (TracerSpanRef, error) {
	return t.tracer.UnmarshalSpan(header)
}

func workflowFutureFromErr(ctx workflow.Context, err error) workflow.Future {
	fut, set := workflow.NewFuture(ctx)
	set.SetError(err)
	return fut
}

type nexusOperationFuture struct{ workflow.Future }

func (e nexusOperationFuture) GetNexusOperationExecution() workflow.Future { return e }

type childWorkflowFuture struct{ workflow.Future }

func (e childWorkflowFuture) GetChildWorkflowExecution() workflow.Future { return e }

func (e childWorkflowFuture) SignalChildWorkflow(ctx workflow.Context, signalName string, data interface{}) workflow.Future {
	return e
}

type tracerSpanRef struct {
	trace.SpanContext
	baggage.Baggage
}

type tracerSpan struct {
	trace.Span
	baggage.Baggage
}

func (t *tracerSpan) Finish(opts *TracerFinishSpanOptions) {
	if opts.Error != nil {
		t.SetStatus(codes.Error, opts.Error.Error())
	}
	t.End()
}

type textMapCarrier map[string]string

func (t textMapCarrier) Get(key string) string        { return t[key] }
func (t textMapCarrier) Set(key string, value string) { t[key] = value }
func (t textMapCarrier) Keys() []string {
	ret := make([]string, 0, len(t))
	for k := range t {
		ret = append(ret, k)
	}
	return ret
}

func (t *tracer) Options() TracerOptions {
	return *t.options
}
