// Package otel integrates Shoebox handlers with OpenTelemetry tracing.
package otel

import (
	"context"

	"github.com/adexaja/shoebox"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const tracerName = "github.com/adexaja/shoebox/otel"

// TraceMiddleware creates one shoebox.process span for each handler
// invocation. A traceparent in message metadata is extracted before the span
// starts, making retries children of the same extracted parent.
func TraceMiddleware(tracer trace.Tracer) shoebox.Middleware {
	if tracer == nil {
		tracer = otel.Tracer(tracerName)
	}
	return func(next shoebox.HandlerFunc) shoebox.HandlerFunc {
		return func(ctx context.Context, msg shoebox.Message) error {
			ctx = ExtractContext(ctx, msg.Metadata)
			ctx, span := tracer.Start(ctx, "shoebox.process",
				trace.WithAttributes(
					attribute.String("shoebox.queue", msg.Queue),
					attribute.String("shoebox.message_id", msg.ID),
					attribute.Int("shoebox.attempts", msg.Attempts),
					attribute.Int("shoebox.priority", msg.Priority),
				))
			defer span.End()
			err := next(ctx, msg)
			if err != nil {
				span.SetStatus(codes.Error, err.Error())
				span.RecordError(err)
				return err
			}
			span.SetStatus(codes.Ok, "")
			return nil
		}
	}
}

// InjectTraceparent returns a copy of metadata with the current span context
// injected as W3C traceparent. The input map is never modified.
func InjectTraceparent(ctx context.Context, metadata map[string]string) map[string]string {
	out := make(map[string]string, len(metadata)+1)
	for k, v := range metadata {
		out[k] = v
	}
	if ctx == nil {
		ctx = context.Background()
	}
	propagation.TraceContext{}.Inject(ctx, propagation.MapCarrier(out))
	return out
}

// ExtractContext extracts a W3C traceparent from a copied metadata carrier.
// Malformed or absent values leave ctx unchanged and never panic.
func ExtractContext(ctx context.Context, metadata map[string]string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	carrier := make(propagation.MapCarrier, len(metadata))
	for k, v := range metadata {
		carrier[k] = v
	}
	return propagation.TraceContext{}.Extract(ctx, carrier)
}
