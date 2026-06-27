// Package metrics serves a minimal Prometheus text exposition for the daemon. It is
// hand-rolled rather than pulling a metrics client: the daemon exposes a handful of
// values, and a dependency-free renderer keeps the single static binary small.
package metrics

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"sync"
)

// Snapshot is the set of values rendered on a scrape. The caller gathers them; this
// package only formats them.
type Snapshot struct {
	Version string
	Pending int64
	Dead    int64
	Evicted int64
	Targets []TargetStat
}

// TargetStat is one target's delivery tally, identified by the (route, target)
// pair: a target URL is unique only within its route, so the route is part of the
// key. It becomes a labelled sample under each per-target counter family.
type TargetStat struct {
	Route     string
	Target    string
	Delivered int64
	Failed    int64
}

// Counters tallies per-target delivery outcomes. Unlike the DLQ gauges — counted
// at scrape time from the store — a delivered row is removed once it lands, so a
// monotonic count cannot be recovered from the database after the fact and must be
// kept in process as the deliveries happen.
//
// A single mutex guards the map: outcomes are reported once per delivery, not per
// packet, so contention is negligible and the lock keeps the type plainly race-free.
type Counters struct {
	mu sync.Mutex
	m  map[targetKey]*targetCounts
}

type targetKey struct {
	route  string
	target string
}

type targetCounts struct {
	delivered int64
	failed    int64
}

// NewCounters returns an empty registry ready for concurrent use.
func NewCounters() *Counters {
	return &Counters{m: make(map[targetKey]*targetCounts)}
}

// IncDelivered records a successful delivery to the (route, target) pair.
func (c *Counters) IncDelivered(route, target string) {
	c.mu.Lock()
	c.entry(route, target).delivered++
	c.mu.Unlock()
}

// IncFailed records a failed delivery attempt to the (route, target) pair.
func (c *Counters) IncFailed(route, target string) {
	c.mu.Lock()
	c.entry(route, target).failed++
	c.mu.Unlock()
}

// entry returns the counts for a key, creating them on first use. The caller holds mu.
func (c *Counters) entry(route, target string) *targetCounts {
	k := targetKey{route: route, target: target}
	tc := c.m[k]
	if tc == nil {
		tc = &targetCounts{}
		c.m[k] = tc
	}
	return tc
}

// Snapshot copies the current tallies into a slice sorted by (route, target) so the
// exposition is stable across scrapes regardless of map iteration order.
func (c *Counters) Snapshot() []TargetStat {
	c.mu.Lock()
	out := make([]TargetStat, 0, len(c.m))
	for k, tc := range c.m {
		out = append(out, TargetStat{Route: k.route, Target: k.target, Delivered: tc.delivered, Failed: tc.failed})
	}
	c.mu.Unlock()

	slices.SortFunc(out, func(a, b TargetStat) int {
		if a.Route != b.Route {
			return strings.Compare(a.Route, b.Route)
		}
		return strings.Compare(a.Target, b.Target)
	})
	return out
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
// by its HELP and TYPE; the _total suffix marks the counters, sluice_dlq_pending and
// sluice_dlq_dead are gauges. The per-target families render one labelled line per
// target, so their HELP and TYPE are written once ahead of the samples.
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

	b.WriteString("# HELP sluice_deliveries_total Deliveries that returned a 2xx, by route and target.\n")
	b.WriteString("# TYPE sluice_deliveries_total counter\n")
	for _, t := range s.Targets {
		_, _ = fmt.Fprintf(b, "sluice_deliveries_total{route=\"%s\",target=\"%s\"} %d\n",
			escapeLabel(t.Route), escapeLabel(t.Target), t.Delivered)
	}

	b.WriteString("# HELP sluice_delivery_failures_total Failed delivery attempts (non-2xx or transport error), by route and target.\n")
	b.WriteString("# TYPE sluice_delivery_failures_total counter\n")
	for _, t := range s.Targets {
		_, _ = fmt.Fprintf(b, "sluice_delivery_failures_total{route=\"%s\",target=\"%s\"} %d\n",
			escapeLabel(t.Route), escapeLabel(t.Target), t.Failed)
	}
}

// labelEscaper escapes a Prometheus label value: backslash, double quote, newline.
// The Replacer substitutes simultaneously, so the backslash rule does not re-process
// the escapes the others introduce.
var labelEscaper = strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)

func escapeLabel(v string) string { return labelEscaper.Replace(v) }
