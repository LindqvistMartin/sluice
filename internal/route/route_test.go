package route

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/LindqvistMartin/sluice/internal/config"
)

func testMatcher() *Matcher {
	return New(&config.Config{
		Routes: []config.Route{
			{
				Path:   "/prometheus",
				Fanout: []config.Target{{URL: "http://flare/in"}},
			},
			{
				Path:   "/github",
				Match:  config.Match{Header: map[string]string{"x-github-event": "workflow_run"}},
				Fanout: []config.Target{{URL: "http://flare/gh"}},
			},
		},
	})
}

func TestMatch(t *testing.T) {
	m := testMatcher()
	tests := []struct {
		name    string
		path    string
		headers map[string]string
		want    bool
	}{
		{"path with no gate", "/prometheus", nil, true},
		{"unknown path", "/nope", nil, false},
		{"header gate satisfied", "/github", map[string]string{"X-GitHub-Event": "workflow_run"}, true},
		{"header gate wrong value", "/github", map[string]string{"X-GitHub-Event": "push"}, false},
		{"header gate absent", "/github", nil, false},
		{"trailing slash is not the same path", "/prometheus/", nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, tt.path, nil)
			for k, v := range tt.headers {
				req.Header.Set(k, v)
			}
			got, ok := m.Match(req)
			if ok != tt.want {
				t.Fatalf("Match ok = %v, want %v", ok, tt.want)
			}
			if ok && got.Path != tt.path {
				t.Errorf("matched route path = %q, want %q", got.Path, tt.path)
			}
		})
	}
}

func TestNew_CanonicalizesHeaderKeys(t *testing.T) {
	m := testMatcher()
	gh := m.routes["/github"]
	if _, ok := gh.Header["X-Github-Event"]; !ok {
		t.Errorf("header key was not canonicalized: %v", gh.Header)
	}
}

func TestMatch_MultipleHeaderGateIsAnd(t *testing.T) {
	m := New(&config.Config{
		Routes: []config.Route{
			{
				Path: "/gh",
				Match: config.Match{Header: map[string]string{
					"X-GitHub-Event":  "push",
					"X-GitHub-Action": "created",
				}},
				Fanout: []config.Target{{URL: "http://x/y"}},
			},
		},
	})
	req := func(headers map[string]string) *http.Request {
		r := httptest.NewRequest(http.MethodPost, "/gh", nil)
		for k, v := range headers {
			r.Header.Set(k, v)
		}
		return r
	}

	if _, ok := m.Match(req(map[string]string{"X-GitHub-Event": "push", "X-GitHub-Action": "created"})); !ok {
		t.Error("both gate headers present should match")
	}
	if _, ok := m.Match(req(map[string]string{"X-GitHub-Event": "push"})); ok {
		t.Error("a missing gate header should not match")
	}
}

func TestMatch_UsesFirstHeaderValueOnly(t *testing.T) {
	m := testMatcher()
	r := httptest.NewRequest(http.MethodPost, "/github", nil)
	r.Header.Add("X-GitHub-Event", "push")         // first value, != expected
	r.Header.Add("X-GitHub-Event", "workflow_run") // second value is ignored by Get
	if _, ok := m.Match(r); ok {
		t.Error("matching reads only the first header value, so this should not match")
	}
}
