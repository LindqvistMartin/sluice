package observability

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func TestNewLogger_Text(t *testing.T) {
	var buf bytes.Buffer
	NewLogger(&buf, LogText).Info("hello", "k", "v")

	out := buf.String()
	if !strings.Contains(out, "level=INFO") || !strings.Contains(out, "k=v") {
		t.Errorf("text output not in expected shape: %q", out)
	}
	if json.Valid(buf.Bytes()) {
		t.Error("text format should not emit valid JSON")
	}
}

func TestNewLogger_JSON(t *testing.T) {
	var buf bytes.Buffer
	NewLogger(&buf, LogJSON).Info("hello", "k", "v")

	var rec map[string]any
	if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
		t.Fatalf("output is not valid JSON: %v (%q)", err, buf.String())
	}
	if rec["level"] != "INFO" || rec["k"] != "v" {
		t.Errorf("unexpected JSON record: %v", rec)
	}
}

func TestParseLogFormat(t *testing.T) {
	for _, s := range []string{"text", "json"} {
		if _, err := ParseLogFormat(s); err != nil {
			t.Errorf("ParseLogFormat(%q): unexpected error %v", s, err)
		}
	}
	for _, s := range []string{"xml", "", "TEXT"} {
		if _, err := ParseLogFormat(s); err == nil {
			t.Errorf("ParseLogFormat(%q): expected an error", s)
		}
	}
}
