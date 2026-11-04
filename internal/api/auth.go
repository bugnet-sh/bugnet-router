package api

import (
	"context"
	"crypto/subtle"
	"errors"
	"net/http"
	"strings"

	"connectrpc.com/connect"
)

var errUnauthenticated = errors.New("api: missing or wrong internal token")

// auth admits only calls bearing the internal token, an empty token admitting none
type auth struct{ token []byte }

func (a auth) check(h http.Header) error {
	scheme, cred, ok := strings.Cut(h.Get("Authorization"), " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") || len(a.token) == 0 ||
		subtle.ConstantTimeCompare([]byte(cred), a.token) != 1 {
		return connect.NewError(connect.CodeUnauthenticated, errUnauthenticated)
	}
	return nil
}

func (a auth) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		if err := a.check(req.Header()); err != nil {
			return nil, err
		}
		return next(ctx, req)
	}
}

func (a auth) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return next
}

func (a auth) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return func(ctx context.Context, conn connect.StreamingHandlerConn) error {
		if err := a.check(conn.RequestHeader()); err != nil {
			return err
		}
		return next(ctx, conn)
	}
}
