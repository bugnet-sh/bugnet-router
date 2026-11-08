package api

import (
	"cmp"
	"context"
	"crypto/sha256"
	"errors"
	"time"

	"connectrpc.com/connect"
	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
	"github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1/routerv1connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/bugnet-sh/bugnet-router/internal/dpkeys"
	"github.com/bugnet-sh/bugnet-router/internal/secrets"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

// SecretsRouteServer implements routerv1connect.SecretsRouteServiceHandler for bugnet-secrets
type SecretsRouteServer struct {
	// Answers Unimplemented for every RPC of the contract without a method here
	routerv1connect.UnimplementedSecretsRouteServiceHandler
	s *Server
}

var _ routerv1connect.SecretsRouteServiceHandler = (*SecretsRouteServer)(nil)

// GetSecretLink answers what the page shows for a link, never consuming it
func (r *SecretsRouteServer) GetSecretLink(
	ctx context.Context, req *connect.Request[routerv1.GetSecretLinkRequest],
) (*connect.Response[routerv1.GetSecretLinkResponse], error) {
	s := r.s
	l, err := s.linkFor(ctx, req.Msg.GetLinkToken())
	if err == nil {
		err = liveLink(l)
	}
	if err != nil {
		return nil, s.status(err)
	}
	p, err := s.d.Store.PlacementByID(ctx, l.PlacementID)
	if err != nil {
		return nil, s.status(err)
	}
	if p.State != store.PlacementReady {
		return nil, s.status(errLinkUnknown)
	}
	return connect.NewResponse(&routerv1.GetSecretLinkResponse{
		PairingCode: l.PairingCode, SourceLabel: p.SourceLabel,
		SandboxCreatedAt: timestamppb.New(p.CreatedAt), ExpiresAt: timestamppb.New(l.ExpiresAt),
		Suggested: &routerv1.SecretSpec{
			Name: l.SuggestedName, Hosts: l.SuggestedHosts, HeaderNames: l.SuggestedHeaders,
		},
		SealingKeys: s.sealingKeys(p.ID),
	}), nil
}

// SubmitSecretLink stores the page's sealed value and uses the link, a repeat answered again
func (r *SecretsRouteServer) SubmitSecretLink(
	ctx context.Context, req *connect.Request[routerv1.SubmitSecretLinkRequest],
) (*connect.Response[routerv1.SubmitSecretLinkResponse], error) {
	s, m := r.s, req.Msg
	if !secrets.ValidKey(m.GetSubmissionId()) {
		return nil, invalid("submission_id must be 1 to 128 printable ASCII characters")
	}
	l, err := s.linkFor(ctx, m.GetLinkToken())
	if err != nil {
		return nil, s.status(err)
	}
	if l.UsedAt != nil && l.SubmissionID == m.GetSubmissionId() {
		return s.submitted(ctx, l)
	}
	if err := liveLink(l); err != nil {
		return nil, s.status(err)
	}
	p, err := s.d.Store.PlacementByID(ctx, l.PlacementID)
	if err != nil {
		return nil, s.status(err)
	}
	row, err := s.writeSecret(ctx, p, secretWrite{
		spec: m.GetSpec(), value: m.GetValue(), source: store.SourceLink,
		key: linkKeyPrefix + l.ID.String(), link: &l, submission: m.GetSubmissionId(),
	})
	if errors.Is(err, errLinkGone) {
		// Another submit used the link first, which may have been this one's retry
		if l, err = s.d.Store.SecretLinkByToken(ctx, l.TokenSHA256); err == nil &&
			l.SubmissionID == m.GetSubmissionId() {
			return s.submitted(ctx, l)
		}
		if err == nil {
			err = cmp.Or(liveLink(l), errLinkUnknown)
		}
	}
	if err != nil {
		return nil, s.status(err)
	}
	return connect.NewResponse(&routerv1.SubmitSecretLinkResponse{
		Secret: secrets.Info(s.awaitApplied(ctx, row)),
	}), nil
}

// submitted answers a used link's submit again with the secret it stored
func (s *Server) submitted(
	ctx context.Context, l store.SecretLink,
) (*connect.Response[routerv1.SubmitSecretLinkResponse], error) {
	row, err := s.d.Store.SecretByKey(ctx, l.PlacementID, linkKeyPrefix+l.ID.String())
	if errors.Is(err, store.ErrNotFound) {
		err = errLinkUnknown
	}
	if err != nil {
		return nil, s.status(err)
	}
	return connect.NewResponse(&routerv1.SubmitSecretLinkResponse{
		Secret: secrets.Info(s.awaitApplied(ctx, row)),
	}), nil
}

// GetBridgeSealingKey answers the sealing keys of the grant's placement
func (r *SecretsRouteServer) GetBridgeSealingKey(
	ctx context.Context, req *connect.Request[routerv1.GetBridgeSealingKeyRequest],
) (*connect.Response[routerv1.GetBridgeSealingKeyResponse], error) {
	s := r.s
	p, err := s.granted(ctx, req.Msg.GetPutGrant())
	if err == nil {
		err = s.secretsFor(p)
	}
	if err != nil {
		return nil, s.status(err)
	}
	return connect.NewResponse(&routerv1.GetBridgeSealingKeyResponse{
		SealingKeys: s.sealingKeys(p.ID),
	}), nil
}

// PutBridgeSecret stores a sealed secret the bridge sends, as PutSecret does
func (r *SecretsRouteServer) PutBridgeSecret(
	ctx context.Context, req *connect.Request[routerv1.PutBridgeSecretRequest],
) (*connect.Response[routerv1.PutBridgeSecretResponse], error) {
	s, m := r.s, req.Msg
	p, err := s.granted(ctx, m.GetPutGrant())
	if err != nil {
		return nil, s.status(err)
	}
	row, err := s.put(ctx, p, m.GetSpec(), m.GetValue(), store.SourceBridge,
		m.GetIdempotencyKey())
	if err != nil {
		return nil, s.status(err)
	}
	return connect.NewResponse(&routerv1.PutBridgeSecretResponse{
		Secret: secrets.Info(row), Replaced: row.Replaced(),
	}), nil
}

// linkFor finds a link by its token, used and expired ones included
func (s *Server) linkFor(ctx context.Context, token string) (store.SecretLink, error) {
	if token == "" {
		return store.SecretLink{}, errLinkUnknown
	}
	sum := sha256.Sum256([]byte(token))
	l, err := s.d.Store.SecretLinkByToken(ctx, sum[:])
	if errors.Is(err, store.ErrNotFound) {
		return store.SecretLink{}, errLinkUnknown
	}
	return l, err
}

// liveLink refuses a used, expired or locked link
func liveLink(l store.SecretLink) error {
	switch {
	case l.UsedAt != nil || l.Expired:
		return errLinkUnknown
	case l.FailedAttempts >= maxLinkFailures:
		return errLinkLocked
	}
	return nil
}

// granted answers the placement a put grant names, refusing an expired or forged one
func (s *Server) granted(ctx context.Context, grant string) (store.Placement, error) {
	id, err := s.d.Keys.CheckPutGrant(grant, time.Now())
	if errors.Is(err, dpkeys.ErrInvalid) || errors.Is(err, dpkeys.ErrExpired) {
		return store.Placement{}, errPutGrant
	}
	if err != nil {
		return store.Placement{}, err
	}
	p, err := s.d.Store.PlacementByID(ctx, id)
	if errors.Is(err, store.ErrNotFound) {
		return store.Placement{}, errPutGrant
	}
	return p, err
}
