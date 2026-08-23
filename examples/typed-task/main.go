package main

import (
	"context"
	"log"

	"github.com/adexaja/shoebox"
)

type Email struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
}

func main() {
	q, err := shoebox.New(shoebox.Options{Storage: shoebox.Memory})
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = q.Shutdown(context.Background()) }()
	shoebox.HandleTask(q, "emails", func(_ context.Context, email Email) error {
		log.Printf("send %s to %s", email.Subject, email.To)
		return nil
	})
	if err := shoebox.EnqueueTask(q, "emails", Email{To: "user@example.com", Subject: "Welcome"}); err != nil {
		log.Fatal(err)
	}
}
