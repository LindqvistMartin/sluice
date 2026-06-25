package config

import (
	"fmt"
	"io"
	"os"
	"time"

	"go.yaml.in/yaml/v3"
)

// Config is the parsed and validated sluice configuration.
type Config struct {
	Listen        string  `yaml:"listen"`
	MetricsListen string  `yaml:"metrics_listen"` // empty disables the metrics endpoint
	Limits        Limits  `yaml:"limits"`
	DLQ           DLQ     `yaml:"dlq"`
	Routes        []Route `yaml:"routes"`
}

// Limits holds the inbound guardrails applied to every request.
type Limits struct {
	MaxBodyBytes int64  `yaml:"max_body_bytes"`
	PerIPRate    string `yaml:"per_ip_rate"` // e.g. "100/min"; see Rate.

	parsedRate Rate // parsed from PerIPRate during validation
}

// DLQ configures the on-disk dead-letter queue.
type DLQ struct {
	Path      string `yaml:"path"`
	MaxSizeMB int    `yaml:"max_size_mb"`
}

// Route maps an inbound path (optionally gated by headers) to a set of targets.
type Route struct {
	Path   string   `yaml:"path"`
	Match  Match    `yaml:"match"`
	Fanout []Target `yaml:"fanout"`
}

// Match is an optional additional gate on a route. An empty Header matches any
// request that reached the route's path.
type Match struct {
	Header map[string]string `yaml:"header"`
}

// Target is a single downstream endpoint a route fans out to.
type Target struct {
	URL     string   `yaml:"url"`
	Timeout Duration `yaml:"timeout"`
	Retry   Retry    `yaml:"retry"`
}

// Retry controls per-target redelivery.
type Retry struct {
	Max     int    `yaml:"max"`
	Backoff string `yaml:"backoff"`
}

const (
	defaultListen       = ":8080"
	defaultMaxBodyBytes = 1 << 20 // 1 MiB
	defaultPerIPRate    = "100/min"
	defaultDLQPath      = "./dlq.db"
	defaultMaxSizeMB    = 100
	defaultTimeout      = 10 * time.Second
	defaultRetryMax     = 5
	defaultBackoff      = "exponential"
)

// Rate returns the per-IP rate parsed during Load. It is the zero Rate on a
// Config that has not been validated.
func (l Limits) Rate() Rate {
	return l.parsedRate
}

// Load reads, applies defaults to, and validates the configuration at path.
func Load(path string) (*Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open config: %w", err)
	}
	defer func() { _ = f.Close() }()

	return parse(f)
}

// parse decodes the configuration from r in strict mode, applies defaults, and
// validates the result.
func parse(r io.Reader) (*Config, error) {
	dec := yaml.NewDecoder(r)
	dec.KnownFields(true) // reject unknown keys so typos fail fast.

	var cfg Config
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	cfg.applyDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// applyDefaults fills omitted fields. It runs before validation so that an
// omitted timeout or retry block is not mistaken for an invalid zero value.
func (c *Config) applyDefaults() {
	if c.Listen == "" {
		c.Listen = defaultListen
	}
	if c.Limits.MaxBodyBytes == 0 {
		c.Limits.MaxBodyBytes = defaultMaxBodyBytes
	}
	if c.Limits.PerIPRate == "" {
		c.Limits.PerIPRate = defaultPerIPRate
	}
	if c.DLQ.Path == "" {
		c.DLQ.Path = defaultDLQPath
	}
	if c.DLQ.MaxSizeMB == 0 {
		c.DLQ.MaxSizeMB = defaultMaxSizeMB
	}

	for i := range c.Routes {
		for j := range c.Routes[i].Fanout {
			t := &c.Routes[i].Fanout[j]
			if t.Timeout.Duration == 0 {
				t.Timeout.Duration = defaultTimeout
			}
			if t.Retry.Max == 0 {
				t.Retry.Max = defaultRetryMax
			}
			if t.Retry.Backoff == "" {
				t.Retry.Backoff = defaultBackoff
			}
		}
	}
}
