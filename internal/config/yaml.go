package config

import (
	"fmt"

	"gopkg.in/yaml.v3"
)

// parseBytes unmarshals YAML bytes into a Config.
func parseBytes(data []byte) (*Config, error) {
	var c Config
	if err := yaml.Unmarshal(data, &c); err != nil {
		return nil, fmt.Errorf("yaml: %w", err)
	}
	return &c, nil
}
