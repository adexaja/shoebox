// Package config parses the shoeboxd YAML configuration file. The config
// file is the canonical way to declare per-queue webhook targets and other
// server-mode settings that are awkward to express as repeatable CLI flags.
//
// Example config.yaml:
//
//	# shoeboxd configuration
//	server:
//	  addr: ":8080"
//	  auth_token: "secret"
//
//	storage:
//	  kind: sqlite           # memory | sqlite | postgres
//	  path: "shoebox.db"
//	  dsn: ""
//
//	webhooks:
//	  orders:
//	    url: "https://hooks.example.com/orders"
//	  emails:
//	    url: "https://hooks.example.com/emails"
//	    timeout: 30s
//	    content_type: "text/plain"
//
// Command-line flags override config-file values for the same field.
package config

import (
	"fmt"
	"os"
	"time"
)

// Config is the top-level shoeboxd configuration.
type Config struct {
	Server   ServerConfig             `yaml:"server"`
	Storage  StorageConfig            `yaml:"storage"`
	Webhooks map[string]WebhookConfig `yaml:"webhooks"`
}

// ServerConfig holds HTTP server settings.
type ServerConfig struct {
	Addr              string `yaml:"addr"`
	AuthToken         string `yaml:"auth_token"`         // API token (X-API-Key / Bearer)
	DashboardUser     string `yaml:"dashboard_user"`     // Basic Auth username for dashboard
	DashboardPassword string `yaml:"dashboard_password"` // Basic Auth password for dashboard
}

// StorageConfig selects and configures the storage backend.
type StorageConfig struct {
	Kind   string `yaml:"kind"`   // memory | sqlite | postgres
	Path   string `yaml:"path"`   // SQLite database path
	DSN    string `yaml:"dsn"`    // Postgres DSN
	Schema string `yaml:"schema"` // PostgreSQL schema
}

// WebhookConfig declares push delivery for a single queue.
type WebhookConfig struct {
	URL         string        `yaml:"url"`
	Timeout     time.Duration `yaml:"timeout"`
	ContentType string        `yaml:"content_type"`
	Secret      string        `yaml:"secret"` // HMAC-SHA256 signing key (empty = unsigned)
}

// defaults applies sensible defaults to an empty config.
func (c *Config) defaults() {
	if c.Server.Addr == "" {
		c.Server.Addr = "127.0.0.1:8080"
	}
	if c.Storage.Kind == "" {
		c.Storage.Kind = "memory"
	}
	if c.Storage.Path == "" {
		c.Storage.Path = "shoebox.db"
	}
	if c.Storage.Schema == "" {
		c.Storage.Schema = "public"
	}
	for name, wh := range c.Webhooks {
		if wh.Timeout == 0 {
			wh.Timeout = 10 * time.Second
		}
		if wh.ContentType == "" {
			wh.ContentType = "application/json"
		}
		c.Webhooks[name] = wh
	}
}

// validate checks the config for structural errors after parsing.
func (c *Config) validate() error {
	switch c.Storage.Kind {
	case "memory", "sqlite", "postgres":
	default:
		return fmt.Errorf("config: unknown storage.kind %q (want memory|sqlite|postgres)", c.Storage.Kind)
	}
	if c.Storage.Kind == "sqlite" && c.Storage.Path == "" {
		return fmt.Errorf("config: storage.path is required for sqlite")
	}
	if c.Storage.Kind == "postgres" && c.Storage.DSN == "" {
		return fmt.Errorf("config: storage.dsn is required for postgres")
	}
	for name, wh := range c.Webhooks {
		if wh.URL == "" {
			return fmt.Errorf("config: webhooks.%s.url is empty", name)
		}
	}
	return nil
}

// File is an interface around os.ReadFile so tests can inject content.
type fileReader func(name string) ([]byte, error)

// Parse parses YAML config bytes into a Config.
func Parse(data []byte) (*Config, error) {
	return parseBytes(data)
}

// Load reads and parses a YAML config file, then applies defaults.
// Returns a zero-value Config (not nil) if path is empty.
func Load(path string) (*Config, error) {
	return loadWith(path, os.ReadFile)
}

func loadWith(path string, read fileReader) (*Config, error) {
	if path == "" {
		c := &Config{}
		c.defaults()
		return c, nil
	}

	data, err := read(path)
	if err != nil {
		return nil, fmt.Errorf("config: read %s: %w", path, err)
	}

	c, err := parseBytes(data)
	if err != nil {
		return nil, fmt.Errorf("config: parse %s: %w", path, err)
	}

	c.defaults()
	if err := c.validate(); err != nil {
		return nil, err
	}
	return c, nil
}
