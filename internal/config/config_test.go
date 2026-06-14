package config

import (
	"strings"
	"testing"
	"time"

	"go.yaml.in/yaml/v3"
)

func TestParse_Defaults(t *testing.T) {
	cfg, err := parse(strings.NewReader(`
routes:
  - path: /hook
    fanout:
      - url: http://example.com/in
`))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	if cfg.Listen != ":8080" {
		t.Errorf("listen = %q, want :8080", cfg.Listen)
	}
	if cfg.Limits.MaxBodyBytes != 1<<20 {
		t.Errorf("max_body_bytes = %d, want %d", cfg.Limits.MaxBodyBytes, 1<<20)
	}
	if cfg.Limits.PerIPRate != "100/min" {
		t.Errorf("per_ip_rate = %q, want 100/min", cfg.Limits.PerIPRate)
	}
	if cfg.DLQ.Path != "./dlq.db" {
		t.Errorf("dlq.path = %q, want ./dlq.db", cfg.DLQ.Path)
	}
	if cfg.DLQ.MaxSizeMB != 100 {
		t.Errorf("dlq.max_size_mb = %d, want 100", cfg.DLQ.MaxSizeMB)
	}

	tgt := cfg.Routes[0].Fanout[0]
	if tgt.Timeout.Duration != 10*time.Second {
		t.Errorf("timeout = %v, want 10s", tgt.Timeout.Duration)
	}
	if tgt.Retry.Max != 5 {
		t.Errorf("retry.max = %d, want 5", tgt.Retry.Max)
	}
	if tgt.Retry.Backoff != "exponential" {
		t.Errorf("retry.backoff = %q, want exponential", tgt.Retry.Backoff)
	}
}

// TestLoad_Example guards the shipped example config against drift: it must load,
// validate, and parse into the shape the docs promise.
func TestLoad_Example(t *testing.T) {
	cfg, err := Load("../../sluice.example.yml")
	if err != nil {
		t.Fatalf("load example: %v", err)
	}
	if len(cfg.Routes) != 2 {
		t.Fatalf("routes = %d, want 2", len(cfg.Routes))
	}
	if cfg.Routes[0].Path != "/prometheus" {
		t.Errorf("routes[0].path = %q, want /prometheus", cfg.Routes[0].Path)
	}
	if len(cfg.Routes[0].Fanout) != 2 {
		t.Errorf("routes[0].fanout len = %d, want 2", len(cfg.Routes[0].Fanout))
	}
	if got := cfg.Routes[1].Match.Header["X-GitHub-Event"]; got != "workflow_run" {
		t.Errorf("github header gate = %q, want workflow_run", got)
	}
	if got := cfg.Limits.Rate(); got.Count != 100 || got.Window != time.Minute {
		t.Errorf("rate = %+v, want {100 1m}", got)
	}
}

func TestParse_ValidationErrors(t *testing.T) {
	tests := []struct {
		name string
		yaml string
		want string
	}{
		{
			name: "no routes",
			yaml: "listen: :8080\n",
			want: "routes: at least one route is required",
		},
		{
			name: "empty path",
			yaml: "routes:\n  - fanout:\n      - url: http://x/y\n",
			want: "routes[0].path: must not be empty",
		},
		{
			name: "path without slash",
			yaml: "routes:\n  - path: hook\n    fanout:\n      - url: http://x/y\n",
			want: `routes[0].path: must start with "/"`,
		},
		{
			name: "duplicate path",
			yaml: "routes:\n  - path: /a\n    fanout:\n      - url: http://x/y\n  - path: /a\n    fanout:\n      - url: http://x/z\n",
			want: `routes[1].path: duplicate route path "/a"`,
		},
		{
			name: "empty fanout",
			yaml: "routes:\n  - path: /a\n    fanout: []\n",
			want: "routes[0].fanout: must list at least one target",
		},
		{
			name: "empty url",
			yaml: "routes:\n  - path: /a\n    fanout:\n      - url: \"\"\n",
			want: "routes[0].fanout[0].url: must not be empty",
		},
		{
			name: "bad url scheme",
			yaml: "routes:\n  - path: /a\n    fanout:\n      - url: ftp://h/x\n",
			want: `routes[0].fanout[0].url: invalid URL "ftp://h/x": scheme must be http or https`,
		},
		{
			name: "non-positive timeout",
			yaml: "routes:\n  - path: /a\n    fanout:\n      - url: http://x/y\n        timeout: -1s\n",
			want: "routes[0].fanout[0].timeout: must be greater than 0",
		},
		{
			name: "negative max body",
			yaml: "limits:\n  max_body_bytes: -5\nroutes:\n  - path: /a\n    fanout:\n      - url: http://x/y\n",
			want: "limits.max_body_bytes: must be greater than 0",
		},
		{
			name: "unsupported backoff",
			yaml: "routes:\n  - path: /a\n    fanout:\n      - url: http://x/y\n        retry:\n          max: 3\n          backoff: linear\n",
			want: `routes[0].fanout[0].retry.backoff: unsupported backoff "linear"`,
		},
		{
			name: "malformed rate",
			yaml: "limits:\n  per_ip_rate: 100/hour\nroutes:\n  - path: /a\n    fanout:\n      - url: http://x/y\n",
			want: `limits.per_ip_rate: invalid rate "100/hour": window must be one of min, sec`,
		},
		{
			name: "empty header gate value",
			yaml: "routes:\n  - path: /a\n    match:\n      header:\n        X-Foo: \"\"\n    fanout:\n      - url: http://x/y\n",
			want: `routes[0].match.header: value for "X-Foo" must not be empty`,
		},
		{
			name: "invalid listen address",
			yaml: "listen: garbage\nroutes:\n  - path: /a\n    fanout:\n      - url: http://x/y\n",
			want: "listen: invalid listen address",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parse(strings.NewReader(tt.yaml))
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tt.want)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("error = %q\nwant substring %q", err.Error(), tt.want)
			}
		})
	}
}

func TestParse_UnknownKey(t *testing.T) {
	_, err := parse(strings.NewReader("bogus: 1\nroutes:\n  - path: /a\n    fanout:\n      - url: http://x/y\n"))
	if err == nil {
		t.Fatal("expected error for unknown key, got nil")
	}
}

func TestLoad_MissingFile(t *testing.T) {
	_, err := Load("does-not-exist.yml")
	if err == nil {
		t.Fatal("expected error for missing file, got nil")
	}
	if !strings.Contains(err.Error(), "does-not-exist.yml") {
		t.Errorf("error should mention the path: %v", err)
	}
}

func TestParseRate(t *testing.T) {
	ok := []struct {
		in     string
		count  int
		window time.Duration
	}{
		{"100/min", 100, time.Minute},
		{"5/sec", 5, time.Second},
	}
	for _, tt := range ok {
		r, err := parseRate(tt.in)
		if err != nil {
			t.Errorf("parseRate(%q): unexpected error %v", tt.in, err)
			continue
		}
		if r.Count != tt.count || r.Window != tt.window {
			t.Errorf("parseRate(%q) = %+v, want {%d %v}", tt.in, r, tt.count, tt.window)
		}
	}

	for _, in := range []string{"100/hour", "abc/min", "0/min", "-1/min", "100", ""} {
		if _, err := parseRate(in); err == nil {
			t.Errorf("parseRate(%q): expected error, got nil", in)
		}
	}
}

func TestDuration_Unmarshal(t *testing.T) {
	var doc struct {
		D Duration `yaml:"d"`
	}
	if err := yaml.Unmarshal([]byte("d: 1500ms"), &doc); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if doc.D.Duration != 1500*time.Millisecond {
		t.Errorf("duration = %v, want 1.5s", doc.D.Duration)
	}
	if err := yaml.Unmarshal([]byte("d: nope"), &doc); err == nil {
		t.Error("expected error for malformed duration, got nil")
	}
}

func TestParse_ExplicitZeroBecomesDefault(t *testing.T) {
	cfg, err := parse(strings.NewReader(`
limits:
  max_body_bytes: 0
routes:
  - path: /a
    fanout:
      - url: http://x/y
        timeout: 0s
        retry:
          max: 0
`))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if cfg.Limits.MaxBodyBytes != 1<<20 {
		t.Errorf("max_body_bytes = %d, want default 1 MiB", cfg.Limits.MaxBodyBytes)
	}
	tgt := cfg.Routes[0].Fanout[0]
	if tgt.Timeout.Duration != 10*time.Second {
		t.Errorf("timeout = %v, want default 10s", tgt.Timeout.Duration)
	}
	if tgt.Retry.Max != 5 {
		t.Errorf("retry.max = %d, want default 5", tgt.Retry.Max)
	}
}

func TestParse_EmptyHeaderMapIsValid(t *testing.T) {
	cfg, err := parse(strings.NewReader(`
routes:
  - path: /a
    match:
      header: {}
    fanout:
      - url: http://x/y
`))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(cfg.Routes[0].Match.Header) != 0 {
		t.Errorf("empty header gate should stay empty, got %v", cfg.Routes[0].Match.Header)
	}
}
