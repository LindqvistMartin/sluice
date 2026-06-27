package main

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// freeAddrs returns n distinct loopback addresses with currently-free ports. It holds
// all n listeners open at once so the ports differ, then releases them.
func freeAddrs(t *testing.T, n int) []string {
	t.Helper()
	ls := make([]net.Listener, n)
	addrs := make([]string, n)
	for i := range ls {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("reserve port: %v", err)
		}
		ls[i] = l
		addrs[i] = l.Addr().String()
	}
	for _, l := range ls {
		_ = l.Close()
	}
	return addrs
}

// writeDaemonConfig writes a minimal valid config with the given listen and (optional)
// metrics_listen addresses and a temp DLQ path, returning the config path.
func writeDaemonConfig(t *testing.T, listen, metricsListen string) string {
	t.Helper()
	dir := t.TempDir()
	dbPath := filepath.ToSlash(filepath.Join(dir, "dlq.db"))
	cfg := "listen: " + listen + "\n"
	if metricsListen != "" {
		cfg += "metrics_listen: " + metricsListen + "\n"
	}
	cfg += "dlq:\n  path: '" + dbPath + "'\n" +
		"routes:\n  - path: /hook\n    fanout:\n      - url: http://localhost:9/in\n"
	cfgPath := filepath.Join(dir, "sluice.yml")
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return cfgPath
}

func TestRun_MetricsEndpoint(t *testing.T) {
	addrs := freeAddrs(t, 2)
	inbound, metricsAddr := addrs[0], addrs[1]
	cfgPath := writeDaemonConfig(t, inbound, metricsAddr)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		var out, errb bytes.Buffer
		done <- run(ctx, []string{"-c", cfgPath}, &out, &errb)
	}()
	defer func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("run returned %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Error("run did not return after cancel")
		}
	}()

	// Poll until the metrics endpoint serves.
	var body string
	deadline := time.After(10 * time.Second)
	for body == "" {
		if resp, err := http.Get("http://" + metricsAddr + "/metrics"); err == nil {
			b, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				body = string(b)
				break
			}
		}
		select {
		case <-deadline:
			t.Fatal("metrics endpoint never served 200")
		case <-time.After(20 * time.Millisecond):
		}
	}

	for _, want := range []string{"sluice_dlq_pending", "sluice_dlq_dead", "sluice_dlq_evicted_total", "sluice_build_info"} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics body missing %q\n%s", want, body)
		}
	}

	// The inbound port serves the bare ingest handler, not metrics.
	resp, err := http.Get("http://" + inbound + "/metrics")
	if err != nil {
		t.Fatalf("inbound GET: %v", err)
	}
	ib, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if strings.Contains(string(ib), "sluice_dlq_") {
		t.Error("inbound port served metrics; want them only on metrics_listen")
	}
}

func TestRun_MetricsEndpoint_PerTarget(t *testing.T) {
	// A real target so the delivery actually succeeds and the per-target counter ticks.
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()

	addrs := freeAddrs(t, 2)
	inbound, metricsAddr := addrs[0], addrs[1]

	dir := t.TempDir()
	dbPath := filepath.ToSlash(filepath.Join(dir, "dlq.db"))
	cfg := "listen: " + inbound + "\n" +
		"metrics_listen: " + metricsAddr + "\n" +
		"dlq:\n  path: '" + dbPath + "'\n" +
		"routes:\n  - path: /hook\n    fanout:\n      - url: " + target.URL + "\n"
	cfgPath := filepath.Join(dir, "sluice.yml")
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		var out, errb bytes.Buffer
		done <- run(ctx, []string{"-c", cfgPath}, &out, &errb)
	}()
	defer func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("run returned %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Error("run did not return after cancel")
		}
	}()

	// Wait for the inbound listener, then POST one webhook to /hook.
	postDeadline := time.After(10 * time.Second)
	for {
		resp, err := http.Post("http://"+inbound+"/hook", "application/json", strings.NewReader(`{"x":1}`))
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				break
			}
		}
		select {
		case <-postDeadline:
			t.Fatal("inbound never accepted the webhook")
		case <-time.After(20 * time.Millisecond):
		}
	}

	// Delivery is async, so poll until the per-target line shows the delivery landed.
	wantLine := `sluice_deliveries_total{route="/hook",target="` + target.URL + `"} 1`
	deadline := time.After(10 * time.Second)
	for {
		if resp, err := http.Get("http://" + metricsAddr + "/metrics"); err == nil {
			b, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if strings.Contains(string(b), wantLine+"\n") {
				return
			}
		}
		select {
		case <-deadline:
			t.Fatalf("metrics never showed %q", wantLine)
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func TestRun_DeliversWithMetricsDisabled(t *testing.T) {
	// The default config sets no metrics_listen, so the pool is handed the no-op
	// counter. Boxing a nil *metrics.Counters into the interface instead would panic on
	// the first delivery in this default configuration; a hit here proves it does not.
	got := make(chan struct{}, 1)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		select {
		case got <- struct{}{}:
		default:
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()

	inbound := freeAddrs(t, 1)[0]
	dir := t.TempDir()
	dbPath := filepath.ToSlash(filepath.Join(dir, "dlq.db"))
	cfg := "listen: " + inbound + "\n" +
		"dlq:\n  path: '" + dbPath + "'\n" +
		"routes:\n  - path: /hook\n    fanout:\n      - url: " + target.URL + "\n"
	cfgPath := filepath.Join(dir, "sluice.yml")
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		var out, errb bytes.Buffer
		done <- run(ctx, []string{"-c", cfgPath}, &out, &errb)
	}()
	defer func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("run returned %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Error("run did not return after cancel")
		}
	}()

	// POST one webhook once the inbound listener is up, then wait for the delivery.
	postDeadline := time.After(10 * time.Second)
	for {
		resp, err := http.Post("http://"+inbound+"/hook", "application/json", strings.NewReader(`{"x":1}`))
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				break
			}
		}
		select {
		case <-postDeadline:
			t.Fatal("inbound never accepted the webhook")
		case <-time.After(20 * time.Millisecond):
		}
	}

	select {
	case <-got:
	case <-time.After(10 * time.Second):
		t.Fatal("target never received the delivery with metrics disabled")
	}
}

func TestRun_MetricsBindFailure(t *testing.T) {
	// Hold a port open so the metrics listener cannot bind it.
	held, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("hold port: %v", err)
	}
	defer func() { _ = held.Close() }()

	cfgPath := writeDaemonConfig(t, freeAddrs(t, 1)[0], held.Addr().String())

	done := make(chan error, 1)
	go func() {
		var out, errb bytes.Buffer
		done <- run(context.Background(), []string{"-c", cfgPath}, &out, &errb)
	}()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "metrics listener") {
			t.Fatalf("run = %v, want a metrics listener bind failure", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("run did not return on metrics bind failure")
	}
}
