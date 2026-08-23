package otel

import (
	"context"
	"errors"
	"testing"

	"github.com/adexaja/shoebox"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

func TestTraceMiddlewareAttributesAndError(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	defer func() { _ = provider.Shutdown(context.Background()) }()
	wrapped := TraceMiddleware(provider.Tracer("test"))(func(context.Context, shoebox.Message) error {
		return errors.New("failed")
	})
	if err := wrapped(context.Background(), shoebox.Message{Queue: "orders", ID: "m1", Attempts: 2, Priority: 7}); err == nil {
		t.Fatal("handler error was swallowed")
	}
	spans := recorder.Ended()
	if len(spans) != 1 || spans[0].Name() != "shoebox.process" {
		t.Fatalf("spans = %#v", spans)
	}
	span := spans[0]
	if span.Status().Code.String() != "Error" || len(span.Events()) != 1 {
		t.Fatalf("status/events = %v/%d", span.Status(), len(span.Events()))
	}
	var queue, messageID string
	for _, attr := range span.Attributes() {
		switch string(attr.Key) {
		case "shoebox.queue":
			queue = attr.Value.AsString()
		case "shoebox.message_id":
			messageID = attr.Value.AsString()
		}
	}
	if queue != "orders" || messageID != "m1" {
		t.Fatalf("attributes = %v", span.Attributes())
	}
}

func TestTraceMiddlewareExtractsParentForRetries(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	defer func() { _ = provider.Shutdown(context.Background()) }()
	parentTrace, _ := trace.TraceIDFromHex("0102030405060708090a0b0c0d0e0f10")
	parentSpan, _ := trace.SpanIDFromHex("0102030405060708")
	parent := trace.NewSpanContext(trace.SpanContextConfig{TraceID: parentTrace, SpanID: parentSpan, TraceFlags: trace.FlagsSampled, Remote: true})
	metadata := InjectTraceparent(trace.ContextWithSpanContext(context.Background(), parent), nil)
	wrapped := TraceMiddleware(provider.Tracer("test"))(func(context.Context, shoebox.Message) error { return nil })
	for i := 0; i < 2; i++ {
		if err := wrapped(context.Background(), shoebox.Message{Metadata: metadata}); err != nil {
			t.Fatal(err)
		}
	}
	spans := recorder.Ended()
	if len(spans) != 2 || spans[0].Parent().SpanID() != parentSpan || spans[1].Parent().SpanID() != parentSpan {
		t.Fatalf("retry parents = %#v", spans)
	}
}

func TestPropagationCopiesAndIgnoresMalformed(t *testing.T) {
	original := map[string]string{"x": "y"}
	copy := InjectTraceparent(context.Background(), original)
	copy["x"] = "changed"
	if original["x"] != "y" {
		t.Fatal("InjectTraceparent mutated input")
	}
	if got := ExtractContext(context.Background(), map[string]string{"traceparent": "bad"}); got == nil {
		t.Fatal("malformed extraction returned nil context")
	}
}
