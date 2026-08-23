package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoad_EmptyPath(t *testing.T) {
	c, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if c.Server.Addr != "127.0.0.1:8080" {
		t.Fatalf("default addr = %q, want 127.0.0.1:8080", c.Server.Addr)
	}
	if c.Storage.Kind != "memory" {
		t.Fatalf("default kind = %q, want memory", c.Storage.Kind)
	}
	if c.Storage.Schema != "public" {
		t.Fatalf("default schema = %q, want public", c.Storage.Schema)
	}
	if len(c.Webhooks) != 0 {
		t.Fatalf("webhooks = %d, want 0", len(c.Webhooks))
	}
}

func TestLoad_PostgresSchema(t *testing.T) {
	yaml := `
storage:
  kind: postgres
  dsn: "host=localhost"
  schema: worker
`
	c, err := Load(writeTemp(t, yaml))
	if err != nil {
		t.Fatal(err)
	}
	if c.Storage.Schema != "worker" {
		t.Fatalf("schema = %q, want worker", c.Storage.Schema)
	}
}

func TestLoad_ValidFile(t *testing.T) {
	yaml := `
server:
  addr: ":9090"
  auth_token: "secret"
storage:
  kind: sqlite
  path: "/data/shoebox.db"
webhooks:
  orders:
    url: "https://hooks.example.com/orders"
    timeout: 30s
    content_type: "text/plain"
  emails:
    url: "https://hooks.example.com/emails"
`
	path := writeTemp(t, yaml)
	c, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}

	if c.Server.Addr != ":9090" {
		t.Fatalf("addr = %q", c.Server.Addr)
	}
	if c.Server.AuthToken != "secret" {
		t.Fatalf("auth_token = %q", c.Server.AuthToken)
	}
	if c.Storage.Kind != "sqlite" {
		t.Fatalf("kind = %q", c.Storage.Kind)
	}
	if c.Storage.Path != "/data/shoebox.db" {
		t.Fatalf("path = %q", c.Storage.Path)
	}

	if len(c.Webhooks) != 2 {
		t.Fatalf("webhooks = %d, want 2", len(c.Webhooks))
	}

	wh := c.Webhooks["orders"]
	if wh.URL != "https://hooks.example.com/orders" {
		t.Fatalf("orders url = %q", wh.URL)
	}
	if wh.Timeout != 30*time.Second {
		t.Fatalf("orders timeout = %v", wh.Timeout)
	}
	if wh.ContentType != "text/plain" {
		t.Fatalf("orders content_type = %q", wh.ContentType)
	}

	// emails has no timeout/content_type → should get defaults.
	emails := c.Webhooks["emails"]
	if emails.Timeout != 10*time.Second {
		t.Fatalf("emails timeout default = %v, want 10s", emails.Timeout)
	}
	if emails.ContentType != "application/json" {
		t.Fatalf("emails content_type default = %q", emails.ContentType)
	}
}

func TestLoad_DefaultsApplied(t *testing.T) {
	yaml := `
storage:
  kind: memory
webhooks:
  q1:
    url: "https://example.com"
`
	path := writeTemp(t, yaml)
	c, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	// Server.Addr should default.
	if c.Server.Addr != "127.0.0.1:8080" {
		t.Fatalf("default addr = %q", c.Server.Addr)
	}
	// Webhook timeout/content_type should default.
	wh := c.Webhooks["q1"]
	if wh.Timeout != 10*time.Second {
		t.Fatalf("timeout = %v, want 10s", wh.Timeout)
	}
	if wh.ContentType != "application/json" {
		t.Fatalf("content_type = %q", wh.ContentType)
	}
}

func TestLoad_InvalidStorageKind(t *testing.T) {
	yaml := `
storage:
  kind: flatfile
`
	path := writeTemp(t, yaml)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for invalid storage kind")
	}
}

func TestLoad_SqliteDefaultPath(t *testing.T) {
	yaml := `
storage:
  kind: sqlite
  path: ""
`
	path := writeTemp(t, yaml)
	c, err := Load(path)
	if err != nil {
		t.Fatalf("expected default path to apply, got error: %v", err)
	}
	if c.Storage.Path != "shoebox.db" {
		t.Fatalf("path = %q, want shoebox.db (default)", c.Storage.Path)
	}
}

func TestLoad_PostgresNoDSN(t *testing.T) {
	yaml := `
storage:
  kind: postgres
  dsn: ""
`
	path := writeTemp(t, yaml)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for postgres without dsn")
	}
}

func TestLoad_WebhookEmptyURL(t *testing.T) {
	yaml := `
webhooks:
  bad:
    url: ""
`
	path := writeTemp(t, yaml)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for webhook with empty url")
	}
}

func TestLoad_FileNotFound(t *testing.T) {
	_, err := Load("/nonexistent/path/file.yaml")
	if err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestParse_InvalidYAML(t *testing.T) {
	_, err := Parse([]byte("  bad: [\n  yaml:"))
	if err == nil {
		t.Fatal("expected error for malformed YAML")
	}
}

func TestParse_NoWebhooks(t *testing.T) {
	c, err := Parse([]byte(`server: {addr: ":8080"}`))
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Webhooks) > 0 {
		t.Fatalf("webhooks should be empty, got %d", len(c.Webhooks))
	}
}

// writeTemp writes content to a temp file and returns its path.
func writeTemp(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	return path
}
