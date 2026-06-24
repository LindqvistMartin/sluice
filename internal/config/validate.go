package config

import (
	"fmt"
	"net"
	"net/url"
	"strings"
)

// ValidationError is a single configuration problem tied to its field path.
type ValidationError struct {
	Path string
	Msg  string
}

func (e *ValidationError) Error() string {
	return e.Path + ": " + e.Msg
}

// ValidationErrors is the set of problems found in one validation pass.
type ValidationErrors []*ValidationError

func (e ValidationErrors) Error() string {
	msgs := make([]string, len(e))
	for i, ve := range e {
		msgs[i] = ve.Error()
	}
	return strings.Join(msgs, "\n")
}

// validate reports every problem it finds in one pass, so a single run surfaces
// all configuration mistakes rather than only the first.
func (c *Config) validate() error {
	var errs ValidationErrors
	add := func(path, msg string) {
		errs = append(errs, &ValidationError{Path: path, Msg: msg})
	}

	if _, _, err := net.SplitHostPort(c.Listen); err != nil {
		add("listen", fmt.Sprintf("invalid listen address %q: %v", c.Listen, err))
	}
	if c.Limits.MaxBodyBytes <= 0 {
		add("limits.max_body_bytes", "must be greater than 0")
	}
	if r, err := parseRate(c.Limits.PerIPRate); err != nil {
		add("limits.per_ip_rate", err.Error())
	} else {
		c.Limits.parsedRate = r
	}
	if c.DLQ.MaxSizeMB <= 0 {
		add("dlq.max_size_mb", "must be greater than 0")
	}

	if len(c.Routes) == 0 {
		add("routes", "at least one route is required")
	}

	seen := make(map[string]bool)
	for i, r := range c.Routes {
		rp := fmt.Sprintf("routes[%d]", i)
		switch {
		case r.Path == "":
			add(rp+".path", "must not be empty")
		case !strings.HasPrefix(r.Path, "/"):
			add(rp+".path", `must start with "/"`)
		case seen[r.Path]:
			add(rp+".path", fmt.Sprintf("duplicate route path %q", r.Path))
		default:
			seen[r.Path] = true
		}

		for k, v := range r.Match.Header {
			if v == "" {
				add(rp+".match.header", fmt.Sprintf("value for %q must not be empty", k))
			}
		}

		if len(r.Fanout) == 0 {
			add(rp+".fanout", "must list at least one target")
		}
		seenURL := make(map[string]bool)
		for j, t := range r.Fanout {
			tp := fmt.Sprintf("%s.fanout[%d]", rp, j)
			validateURL(tp+".url", t.URL, add)
			if t.URL != "" && seenURL[t.URL] {
				add(tp+".url", fmt.Sprintf("duplicate target url %q in fanout", t.URL))
			}
			seenURL[t.URL] = true
			if t.Timeout.Duration <= 0 {
				add(tp+".timeout", "must be greater than 0")
			}
			if t.Retry.Max <= 0 {
				add(tp+".retry.max", "must be greater than 0")
			}
			if t.Retry.Backoff != defaultBackoff {
				add(tp+".retry.backoff", fmt.Sprintf("unsupported backoff %q (only %q in this version)", t.Retry.Backoff, defaultBackoff))
			}
		}
	}

	if len(errs) == 0 {
		return nil
	}
	return errs
}

func validateURL(path, raw string, add func(path, msg string)) {
	if raw == "" {
		add(path, "must not be empty")
		return
	}
	u, err := url.Parse(raw)
	if err != nil {
		add(path, fmt.Sprintf("invalid URL %q: %v", raw, err))
		return
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		add(path, fmt.Sprintf("invalid URL %q: scheme must be http or https", raw))
		return
	}
	if u.Host == "" {
		add(path, fmt.Sprintf("invalid URL %q: missing host", raw))
	}
}
