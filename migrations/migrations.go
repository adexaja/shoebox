// Package migrations embeds the canonical shoebox schema migrations.
//
// The files in this directory are the single source of truth for the schema
// of both persistent backends. The storage package applies them on open
// (forward-only, automatic), and external migration tools can consume the
// same files directly thanks to the goose-style
// NNNN_name.<dialect>.<up|down>.sql naming.
package migrations

import (
	"embed"
	"fmt"
	"regexp"
	"sort"
	"strconv"
)

//go:embed *.sql
var files embed.FS

// nameRe matches migration filenames like 0001_init_schema.postgres.up.sql.
var nameRe = regexp.MustCompile(`^(\d{4})_[a-z0-9_]+\.(postgres|sqlite)\.(up|down)\.sql$`)

// Migration is one embedded migration file.
type Migration struct {
	Version int    // numeric filename prefix
	Dialect string // "postgres" or "sqlite"
	Up      bool   // true for .up files, false for .down
	Name    string // full filename
	SQL     string // file contents
}

// All returns every embedded migration for the dialect, ordered by version
// with up files before down files of the same version.
func All(dialect string) ([]Migration, error) {
	entries, err := files.ReadDir(".")
	if err != nil {
		return nil, err
	}
	var out []Migration
	for _, e := range entries {
		m, err := parse(e.Name())
		if err != nil {
			continue // stray file (e.g. this package's sources)
		}
		if m.Dialect == dialect {
			out = append(out, m)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Version != out[j].Version {
			return out[i].Version < out[j].Version
		}
		if out[i].Up != out[j].Up {
			return out[i].Up
		}
		return out[i].Name < out[j].Name
	})
	return out, nil
}

// Up returns the dialect's up migrations in version order.
func Up(dialect string) ([]Migration, error) {
	all, err := All(dialect)
	if err != nil {
		return nil, err
	}
	var out []Migration
	for _, m := range all {
		if m.Up {
			out = append(out, m)
		}
	}
	return out, nil
}

// Latest returns the highest up-migration version for the dialect.
func Latest(dialect string) (int, error) {
	ups, err := Up(dialect)
	if err != nil {
		return 0, err
	}
	if len(ups) == 0 {
		return 0, fmt.Errorf("no up migrations embedded for dialect %q", dialect)
	}
	return ups[len(ups)-1].Version, nil
}

// Read returns the SQL of a specific migration file by filename.
func Read(name string) (string, error) {
	b, err := files.ReadFile(name)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func parse(name string) (Migration, error) {
	m := nameRe.FindStringSubmatch(name)
	if m == nil {
		return Migration{}, fmt.Errorf("not a migration filename: %q", name)
	}
	v, err := strconv.Atoi(m[1])
	if err != nil {
		return Migration{}, err
	}
	sql, err := Read(name)
	if err != nil {
		return Migration{}, err
	}
	return Migration{
		Version: v,
		Dialect: m[2],
		Up:      m[3] == "up",
		Name:    name,
		SQL:     sql,
	}, nil
}
