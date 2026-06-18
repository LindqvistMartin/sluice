package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestRun_Version(t *testing.T) {
	var out, errb bytes.Buffer
	if err := run(context.Background(), []string{"-version"}, &out, &errb); err != nil {
		t.Fatalf("run: %v", err)
	}
	if !strings.Contains(out.String(), "sluice ") {
		t.Errorf("version output = %q", out.String())
	}
}

func TestRun_ConfigCheckOK(t *testing.T) {
	var out, errb bytes.Buffer
	err := run(context.Background(), []string{"-t", "-c", "testdata/good.yml"}, &out, &errb)
	if err != nil {
		t.Fatalf("run: %v (stderr=%q)", err, errb.String())
	}
	if !strings.Contains(out.String(), "ok") {
		t.Errorf("stdout = %q, want it to contain ok", out.String())
	}
}

func TestRun_ConfigCheckBad(t *testing.T) {
	var out, errb bytes.Buffer
	err := run(context.Background(), []string{"-t", "-c", "testdata/bad.yml"}, &out, &errb)
	if err == nil {
		t.Fatal("expected an error for an invalid config")
	}
	if !strings.Contains(err.Error(), "invalid") {
		t.Errorf("error = %q, want it to report invalid", err.Error())
	}
	if !strings.Contains(err.Error(), "fanout[0].url") {
		t.Errorf("error = %q, want a field path", err.Error())
	}
}

func TestRun_BadLogFormat(t *testing.T) {
	var out, errb bytes.Buffer
	err := run(context.Background(), []string{"--log-format", "xml", "-c", "testdata/good.yml"}, &out, &errb)
	if err == nil {
		t.Fatal("expected an error for an unsupported log format")
	}
}

func TestRun_ConfigCheckOK_JSON(t *testing.T) {
	var out, errb bytes.Buffer
	err := run(context.Background(), []string{"-t", "--log-format", "json", "-c", "testdata/good.yml"}, &out, &errb)
	if err != nil {
		t.Fatalf("run: %v (stderr=%q)", err, errb.String())
	}
	if !strings.Contains(out.String(), "ok") {
		t.Errorf("stdout = %q, want it to contain ok", out.String())
	}
}

func TestRun_VersionBeforeLogFormat(t *testing.T) {
	var out, errb bytes.Buffer
	// A bad --log-format must not stop -version from printing.
	if err := run(context.Background(), []string{"-version", "--log-format", "xml"}, &out, &errb); err != nil {
		t.Fatalf("version should win over log-format validation: %v", err)
	}
	if !strings.Contains(out.String(), "sluice ") {
		t.Errorf("version output = %q", out.String())
	}
}
