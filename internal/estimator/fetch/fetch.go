// Package fetch reads a source's facts from GitHub and Hugging Face within the estimator's budgets
//
// Every answer is cached by commit or revision in Valkey and Postgres. A fetch that outlives its
// caller's budget goes on in the background to fill the cache, and callers of one source share one
// upstream fetch in a replica and, through a Valkey lock, across replicas. Only the configured
// hosts are ever called, and nothing a repository holds enters a query's text.
package fetch

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/bugnet-sh/bugnet-router/internal/estimator/facts"
	"github.com/bugnet-sh/bugnet-router/internal/estimator/rules"
	"github.com/bugnet-sh/bugnet-router/internal/estimator/source"
)

// Resolved says what a fetch learned about a source, or why it read nothing
type Resolved struct {
	// Ref is the ref or revision asked for, empty for the default branch
	Ref string
	// Subdir is the subdirectory a GitHub source named
	Subdir []string
	// Revision is the commit or revision SHA read
	Revision string
	// Unread is empty when the source was read
	Unread rules.UnreadReason
}

// Read says the source was read
func (r Resolved) Read() bool { return r.Unread == "" }

// Fetcher reads one source's facts at a pinned revision, within ctx's deadline
type Fetcher interface {
	Facts(ctx context.Context, src source.Source) (facts.Facts, Resolved, error)
}

// errTooBig means an upstream body went past its cap
var errTooBig = errors.New("fetch: the response is larger than its cap")

// Body caps of CP 4.4
const (
	maxTreeBytes  = 8 << 20
	maxModelBytes = 8 << 20
	maxRefsBytes  = 4 << 20
	maxGraphQL    = 8 << 20
)

// readCapped reads at most limit bytes of r and fails with errTooBig past them
func readCapped(r io.Reader, limit int64) ([]byte, error) {
	b, err := io.ReadAll(io.LimitReader(r, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(b)) > limit {
		return nil, errTooBig
	}
	return b, nil
}

// classify maps a failed upstream status to the reason the source went unread
func classify(status int, h http.Header, private401 bool) rules.UnreadReason {
	switch {
	case status == http.StatusNotFound:
		return rules.UnreadNotFound
	case status == http.StatusUnauthorized && private401:
		return rules.UnreadNotFound
	case status == http.StatusUnavailableForLegalReasons:
		return rules.UnreadLegal
	case status == http.StatusTooManyRequests:
		return rules.UnreadRateLimited
	case status == http.StatusForbidden && (h.Get("Retry-After") != "" ||
		h.Get("X-Ratelimit-Remaining") == "0"):
		return rules.UnreadRateLimited
	}
	return rules.UnreadUpstreamError
}

// negative says the reason is one worth remembering for negative_ttl
func negative(r rules.UnreadReason) bool {
	return r == rules.UnreadNotFound || r == rules.UnreadLegal
}

// retryAfter reads Retry-After as seconds, 0 when absent or not a number
func retryAfter(h http.Header) int {
	n, err := strconv.Atoi(strings.TrimSpace(h.Get("Retry-After")))
	if err != nil || n < 0 {
		return 0
	}
	return min(n, 3600)
}

// FactsKey is the source_facts key of a source's facts, the repository and then its subdirectory
func FactsKey(src source.Source, r Resolved) string {
	if src.Kind == source.KindHuggingFace {
		return src.Key()
	}
	return facts1Key(src, r.Subdir)
}
