package config

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"
)

// Duration is a time.Duration that unmarshals from a YAML string such as "10s".
// The standard library expects a duration to be an integer count of nanoseconds,
// which makes human-friendly values like "10s" or "500ms" fail to decode.
type Duration struct {
	time.Duration
}

// UnmarshalYAML decodes a Go duration string, for example "10s" or "1500ms".
func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	var s string
	if err := value.Decode(&s); err != nil {
		return err
	}
	parsed, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", s, err)
	}
	d.Duration = parsed
	return nil
}

// Rate is a request budget expressed as a count over a time window, parsed from
// strings like "100/min". The count doubles as the limiter burst.
type Rate struct {
	Count  int
	Window time.Duration
}

// PerSecond returns the sustained refill rate in events per second.
func (r Rate) PerSecond() float64 {
	return float64(r.Count) / r.Window.Seconds()
}

// parseRate reads a rate of the form "<count>/<window>", where window is "min"
// or "sec". It is applied during validation rather than as a YAML unmarshaler so
// that errors can be reported against the configuration field path.
func parseRate(s string) (Rate, error) {
	parts := strings.Split(s, "/")
	if len(parts) != 2 {
		return Rate{}, fmt.Errorf("invalid rate %q: expected the form \"100/min\"", s)
	}

	count, err := strconv.Atoi(strings.TrimSpace(parts[0]))
	if err != nil || count <= 0 {
		return Rate{}, fmt.Errorf("invalid rate %q: count must be a positive integer", s)
	}

	var window time.Duration
	switch strings.TrimSpace(parts[1]) {
	case "min":
		window = time.Minute
	case "sec":
		window = time.Second
	default:
		return Rate{}, fmt.Errorf("invalid rate %q: window must be one of min, sec", s)
	}

	return Rate{Count: count, Window: window}, nil
}
