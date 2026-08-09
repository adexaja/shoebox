// Command shoeboxd is the standalone server binary for shoebox. In Week 1
// it is a placeholder that prints the configuration it would load and
// exits 0; the real HTTP server, dashboard, and config are Week-4 work.
package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"

	"github.com/adexaja/shoebox"
)

func main() {
	var (
		addr        = flag.String("addr", ":8080", "address to listen on (Week 4)")
		storageKind = flag.String("storage", "memory", "memory | sqlite | postgres (sqlite/postgres are Week 2)")
		dbPath      = flag.String("path", "shoebox.db", "SQLite database path (Week 2)")
		dsn         = flag.String("dsn", "", "Postgres DSN (Week 2)")
	)
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	opts := shoebox.Options{Logger: logger}
	switch *storageKind {
	case "memory":
		opts.Storage = shoebox.Memory
	case "sqlite":
		opts.Storage = shoebox.SQLite
		opts.Path = *dbPath
	case "postgres":
		opts.Storage = shoebox.Postgres
		opts.DSN = *dsn
	default:
		fmt.Fprintf(os.Stderr, "shoeboxd: unknown --storage=%q\n", *storageKind)
		os.Exit(2)
	}

	q, err := shoebox.New(opts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "shoeboxd: %v\n", err)
		os.Exit(1)
	}
	_ = q

	fmt.Fprintf(os.Stderr,
		"shoeboxd: standalone server not yet implemented (Week 4 milestone).\n"+
			"           config: addr=%s storage=%s path=%s\n",
		*addr, *storageKind, *dbPath)
	os.Exit(0)
}
