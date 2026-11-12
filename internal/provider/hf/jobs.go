package hf

import (
	"log/slog"
	"net/http"

	"golang.org/x/time/rate"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

const (
	// jobsHost is the one host the Jobs client sends the token to
	jobsHost = "huggingface.co"
	jobsBase = "https://" + jobsHost + "/api/jobs"
)

// jobsClient calls HF's Jobs API under one namespace, pacing every call
type jobsClient struct {
	token, namespace string
	client           *http.Client
	log              *slog.Logger
	// ops paces every call, nil for no pacing
	ops *rate.Limiter
}

func newJobsClient(token, namespace string, perSecond int, log *slog.Logger) *jobsClient {
	return &jobsClient{
		token: token, namespace: namespace, log: log,
		client: &http.Client{CheckRedirect: provider.NoRedirect},
		ops:    limiter(perSecond),
	}
}

// limiter paces perSecond calls with no burst, nil for 0
func limiter(perSecond int) *rate.Limiter {
	if perSecond <= 0 {
		return nil
	}
	return rate.NewLimiter(rate.Limit(perSecond), 1)
}
