package contract

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

// Redirector serves only redirects to a second server and reports whether that one was reached
func Redirector(t *testing.T) (string, *atomic.Bool) {
	t.Helper()
	reached := &atomic.Bool{}
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		reached.Store(true)
		_, _ = io.WriteString(w, "INTERNAL-SECRET")
	}))
	t.Cleanup(target.Close)
	agent := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 307 keeps the method and body, so writes would be replayed against the target too
		http.Redirect(w, r, target.URL+"/metadata", http.StatusTemporaryRedirect)
	}))
	t.Cleanup(agent.Close)
	return agent.URL, reached
}
