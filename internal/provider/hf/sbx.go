package hf

import (
	"net/http"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

// sbxClient calls sbx-server in a Job through HF's proxy, carrying the sandbox's own token alone
type sbxClient struct {
	client *http.Client
}

func newSBXClient() *sbxClient {
	return &sbxClient{client: &http.Client{CheckRedirect: provider.NoRedirect}}
}
