package testenv

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
)

// LocalInternet sends every default transport call to a local TLS server running h until t ends
func LocalInternet(t *testing.T, h http.Handler) {
	t.Helper()
	srv := httptest.NewTLSServer(h)
	tr := srv.Client().Transport.(*http.Transport).Clone()
	// The test certificate names example.com, whichever host a call asks for
	tr.TLSClientConfig.ServerName = "example.com"
	tr.DialContext = func(ctx context.Context, _, _ string) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "tcp", srv.Listener.Addr().String())
	}
	prev := http.DefaultTransport
	http.DefaultTransport = tr
	t.Cleanup(func() {
		http.DefaultTransport = prev
		tr.CloseIdleConnections()
		srv.Close()
	})
}
