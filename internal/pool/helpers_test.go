package pool_test

import (
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

func newPool(t *testing.T, url string) (*pgxpool.Pool, error) {
	t.Helper()
	p, err := pgxpool.New(t.Context(), url)
	if err == nil {
		t.Cleanup(p.Close)
	}
	return p, err
}

func refOf(id string) provider.Ref { return provider.Ref{Provider: "local", ID: id} }
