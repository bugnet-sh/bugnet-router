package contract

import (
	"io"
	"net/http"
	"net/http/httptest"
	"path"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/oynozan/bugnet-router/internal/provider"
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

// ListLimit writes five files under dir and checks that a listing with room for three stops there
func ListLimit(t *testing.T, p provider.Provider, ref provider.Ref, dir string) {
	t.Helper()
	ctx := t.Context()
	for i := range 5 {
		name := path.Join(dir, "f"+strconv.Itoa(i))
		if err := p.WriteFile(ctx, ref, name, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	short := provider.NewListing(3)
	if err := p.ListFiles(ctx, ref, dir, 1, short); err != nil || len(short.Entries) != 3 ||
		!short.Truncated {
		t.Fatalf("listing with room for 3 of 5 gave %d entries truncated %v, %v",
			len(short.Entries), short.Truncated, err)
	}
	whole := provider.NewListing(5)
	if err := p.ListFiles(ctx, ref, dir, 1, whole); err != nil || len(whole.Entries) != 5 ||
		whole.Truncated {
		t.Fatalf("listing with room for all 5 gave %d entries truncated %v, %v",
			len(whole.Entries), whole.Truncated, err)
	}
}
