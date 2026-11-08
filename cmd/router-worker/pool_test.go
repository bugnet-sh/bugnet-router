package main

import (
	"log/slog"
	"strings"
	"testing"
)

func TestSmallPoolWarns(t *testing.T) {
	var logs strings.Builder
	log := slog.New(slog.NewTextHandler(&logs, nil))
	checkPool(log, 10, 32)
	if n := strings.Count(logs.String(), "level=WARN"); n != 1 ||
		!strings.Contains(logs.String(), "pool_max_conns") {
		t.Fatalf("10 connections for 32 workers logged %d warnings:\n%s", n, logs.String())
	}
	logs.Reset()
	checkPool(log, 32, 32)
	if !strings.Contains(logs.String(), "level=WARN") {
		t.Fatalf("as many connections as workers gave no warning:\n%s", logs.String())
	}
	logs.Reset()
	checkPool(log, 48, 32)
	if logs.Len() != 0 {
		t.Fatalf("48 connections for 32 workers warned:\n%s", logs.String())
	}
}
