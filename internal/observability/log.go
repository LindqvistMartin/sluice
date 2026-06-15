package observability

import (
	"fmt"
	"io"
	"log/slog"
)

// LogFormat selects the structured-logging output encoding.
type LogFormat string

const (
	// LogText is the human-readable encoding used in development.
	LogText LogFormat = "text"
	// LogJSON is the machine-readable encoding suited to log aggregation.
	LogJSON LogFormat = "json"
)

// ParseLogFormat validates a log-format string from the command line.
func ParseLogFormat(s string) (LogFormat, error) {
	switch LogFormat(s) {
	case LogText, LogJSON:
		return LogFormat(s), nil
	default:
		return "", fmt.Errorf("unsupported log format %q (want text or json)", s)
	}
}

// NewLogger builds a logger that writes to w in the given format at info level.
func NewLogger(w io.Writer, format LogFormat) *slog.Logger {
	opts := &slog.HandlerOptions{Level: slog.LevelInfo}

	var h slog.Handler
	if format == LogJSON {
		h = slog.NewJSONHandler(w, opts)
	} else {
		h = slog.NewTextHandler(w, opts)
	}
	return slog.New(h)
}
