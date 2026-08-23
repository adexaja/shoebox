package main

import (
	"context"
	"log"
	"time"

	"github.com/adexaja/shoebox"
)

func main() {
	q, err := shoebox.New(shoebox.Options{Storage: shoebox.Memory})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = q.Shutdown(context.Background()) }()

	q.Handle("reports", func(context.Context, shoebox.Message) error {
		log.Println("report scheduled")
		return nil
	})
	if err := q.RegisterPeriodic(shoebox.PeriodicJob{
		ID: "hourly-report", Queue: "reports", Payload: []byte(`{"kind":"summary"}`),
		Every: time.Hour, Enabled: true,
	}); err != nil {
		log.Fatal(err)
	}
	select {}
}
