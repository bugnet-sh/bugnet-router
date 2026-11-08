package main

import (
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/config"
)

func TestEmptyStatsAddrOpensNoListener(t *testing.T) {
	srv, ln, err := statsServer(config.Config{}, http.NotFoundHandler())
	if srv != nil || ln != nil || err != nil {
		t.Fatalf("an empty stats.listen_addr gave %v, %v, %v", srv, ln, err)
	}
}

func TestStatsListenerServesOnLoopback(t *testing.T) {
	cfg := config.Config{Stats: config.Stats{ListenAddr: "127.0.0.1:0"}}
	h := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "feed")
	})
	srv, ln, err := statsServer(cfg, h)
	if err != nil {
		t.Fatal(err)
	}
	if srv.ReadHeaderTimeout != 5*time.Second || srv.WriteTimeout != 10*time.Second ||
		srv.IdleTimeout != time.Minute || srv.MaxHeaderBytes != 8<<10 || srv.ErrorLog == nil {
		t.Fatalf("server settings %+v", srv)
	}
	go func() { _ = srv.Serve(ln) }()
	defer func() { _ = srv.Close() }()
	resp, err := http.Get("http://" + ln.Addr().String() + "/api/stats")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if string(body) != "feed" || resp.ProtoMajor != 1 {
		t.Fatalf("got %q over %s", body, resp.Proto)
	}
}
