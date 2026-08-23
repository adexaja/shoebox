package main

import (
	"context"
	"log"

	"github.com/adexaja/shoebox"
	shoeboxotel "github.com/adexaja/shoebox/otel"
	"go.opentelemetry.io/otel"
)

func main() {
	q, err := shoebox.New(shoebox.Options{Storage: shoebox.Memory})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = q.Shutdown(context.Background()) }()
	q.Use(shoeboxotel.TraceMiddleware(otel.Tracer("example")))
	q.Handle("jobs", func(context.Context, shoebox.Message) error { return nil })
	if err := q.Enqueue("jobs", []byte("work")); err != nil {
		log.Fatal(err)
	}
}
