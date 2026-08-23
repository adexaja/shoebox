package shoebox

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestPeriodicJobCadenceAndValidation(t *testing.T) {
	q, err := New(Options{Storage: Memory, Concurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = q.Shutdown(context.Background()) }()

	var runs atomic.Int64
	q.Handle("periodic", func(context.Context, Message) error {
		runs.Add(1)
		return nil
	})
	if err := q.RegisterPeriodic(PeriodicJob{ID: "heartbeat", Queue: "periodic", Payload: []byte("tick"), Every: 20 * time.Millisecond, StartAt: time.Now(), Enabled: true}); err != nil {
		t.Fatal(err)
	}
	if err := q.RegisterPeriodic(PeriodicJob{ID: "heartbeat", Queue: "periodic", Every: time.Second, Enabled: true}); err == nil {
		t.Fatal("duplicate schedule accepted")
	}
	deadline := time.Now().Add(1200 * time.Millisecond)
	for runs.Load() < 3 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := runs.Load(); got < 3 {
		t.Fatalf("periodic runs = %d, want at least 3", got)
	}
	jobs, err := q.PeriodicJobs(context.Background())
	if err != nil || len(jobs) != 1 {
		t.Fatalf("PeriodicJobs = %#v, %v", jobs, err)
	}
	if err := q.RemovePeriodic("heartbeat"); err != nil {
		t.Fatal(err)
	}
	if jobs, err := q.PeriodicJobs(context.Background()); err != nil || len(jobs) != 0 {
		t.Fatalf("removed schedule = %#v, %v", jobs, err)
	}
}

func TestPeriodicJobRejectsInvalidInput(t *testing.T) {
	q, err := New(Options{Storage: Memory})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = q.Shutdown(context.Background()) }()
	for _, job := range []PeriodicJob{{Queue: "q", Every: time.Second}, {ID: "x", Queue: "q", Every: 0}, {ID: "x", Queue: "bad name", Every: time.Second}} {
		if err := q.RegisterPeriodic(job); err == nil {
			t.Fatalf("invalid job accepted: %#v", job)
		}
	}
}

func TestPeriodicJobSQLiteRestart(t *testing.T) {
	path := t.TempDir() + "/periodic.db"
	q, err := New(Options{Storage: SQLite, Path: path})
	if err != nil {
		t.Fatal(err)
	}
	if err := q.RegisterPeriodic(PeriodicJob{ID: "restart", Queue: "jobs", Payload: []byte("x"), Every: time.Minute, Enabled: false}); err != nil {
		t.Fatal(err)
	}
	if err := q.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	q, err = New(Options{Storage: SQLite, Path: path})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = q.Shutdown(context.Background()) }()
	jobs, err := q.PeriodicJobs(context.Background())
	if err != nil || len(jobs) != 1 || jobs[0].ID != "restart" {
		t.Fatalf("recovered schedules = %#v, %v", jobs, err)
	}
}
