package route

import (
	"net/http"
	"net/textproto"

	"github.com/LindqvistMartin/sluice/internal/config"
)

// Route is the resolved set of targets for one inbound path.
type Route struct {
	Path   string
	Header map[string]string // canonical header keys; empty means no header gate
	Fanout []config.Target
}

// Matcher resolves an inbound request to a route by exact path and an optional
// header gate. Paths are unique (the config rejects duplicates), so the lookup
// is a map keyed by path.
type Matcher struct {
	routes map[string]Route
}

// New builds a Matcher from validated configuration. Header keys are canonicalized
// once here so matching compares directly against the canonical keys used by
// http.Header.
func New(cfg *config.Config) *Matcher {
	routes := make(map[string]Route, len(cfg.Routes))
	for _, r := range cfg.Routes {
		var header map[string]string
		if len(r.Match.Header) > 0 {
			header = make(map[string]string, len(r.Match.Header))
			for k, v := range r.Match.Header {
				header[textproto.CanonicalMIMEHeaderKey(k)] = v
			}
		}
		routes[r.Path] = Route{
			Path:   r.Path,
			Header: header,
			Fanout: r.Fanout,
		}
	}
	return &Matcher{routes: routes}
}

// Match returns the route for r. It reports false when no route's path matches or
// when a matched route's header gate is not satisfied; the caller treats both as
// a single not-found response rather than leaking which routes exist.
func (m *Matcher) Match(r *http.Request) (Route, bool) {
	route, ok := m.routes[r.URL.Path]
	if !ok {
		return Route{}, false
	}
	for k, v := range route.Header {
		if r.Header.Get(k) != v {
			return Route{}, false
		}
	}
	return route, true
}
