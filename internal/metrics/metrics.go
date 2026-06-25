// Package metrics serves a minimal Prometheus text exposition for the daemon. It is
// hand-rolled rather than pulling a metrics client: the daemon exposes a handful of
// values, and a dependency-free renderer keeps the single static binary small.
package metrics

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"strings"
)

// Snapshot is the set of values rendered on a scrape. The caller gathers them; this
// package only formats them.
type Snapshot struct {
	Version string
	Pending int64
	Dead    int64
	Evicted int64
}

// Handler serves the exposition. gather is called once per scrape so the numbers are
// current; an error from it yields 503 (a failed scrape) rather than stale or partial
// output.
type Handler struct {
	gather func(context.Context) (Snapshot, error)
}

// NewHandler returns a Handler that renders the Snapshot from gather on each request.
func NewHandler(gather func(context.Context) (Snapshot, error)) *Handler {
	return &Handler{gather: gather}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	snap, err := h.gather(r.Context())
	if err != nil {
		http.Error(w, "metrics unavailable", http.StatusServiceUnavailable)
		return
	}

	var b bytes.Buffer
	writeExposition(&b, snap)

	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	_, _ = w.Write(b.Bytes())
}

// writeExposition renders snap in the Prometheus text format. Each metric is preceded
// by its HELP and TYPE; the _total suffix marks the one counter, the rest are gauges.
func writeExposition(b *bytes.Buffer, s Snapshot) {
	b.WriteString("# HELP sluice_build_info Build information; the value is always 1.\n")
	b.WriteString("# TYPE sluice_build_info gauge\n")
	_, _ = fmt.Fprintf(b, "sluice_build_info{version=\"%s\"} 1\n", escapeLabel(s.Version))

	b.WriteString("# HELP sluice_dlq_pending Deliveries waiting to be delivered.\n")
	b.WriteString("# TYPE sluice_dlq_pending gauge\n")
	_, _ = fmt.Fprintf(b, "sluice_dlq_pending %d\n", s.Pending)

	b.WriteString("# HELP sluice_dlq_dead Deliveries parked after exhausting their retry budget.\n")
	b.WriteString("# TYPE sluice_dlq_dead gauge\n")
	_, _ = fmt.Fprintf(b, "sluice_dlq_dead %d\n", s.Dead)

	b.WriteString("# HELP sluice_dlq_evicted_total Events evicted to keep the DLQ under its size bound.\n")
	b.WriteString("# TYPE sluice_dlq_evicted_total counter\n")
	_, _ = fmt.Fprintf(b, "sluice_dlq_evicted_total %d\n", s.Evicted)
}

// labelEscaper escapes a Prometheus label value: backslash, double quote, newline.
// The Replacer substitutes simultaneously, so the backslash rule does not re-process
// the escapes the others introduce.
var labelEscaper = strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)

func escapeLabel(v string) string { return labelEscaper.Replace(v) }
