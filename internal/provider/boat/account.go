package boat

import (
	"context"
	"errors"
	"log/slog"
	"net/http"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/boat/api"
)

// meReply holds the fields of GET /me that say whether the account may start sandboxes
type meReply struct {
	User struct {
		AccountStatus   string  `json:"accountStatus"`
		Suspended       bool    `json:"suspended"`
		Closed          bool    `json:"closed"`
		SuspendedReason *string `json:"suspendedReason"`
	} `json:"user"`
}

// CheckAccount reads GET /me and GET /limits, and GET /sandboxes where the key cannot read them
func (p *Provider) CheckAccount(ctx context.Context) (provider.Account, error) {
	unreadable := false
	me, status, err := p.readMe(ctx)
	switch {
	case status == http.StatusUnauthorized:
		return provider.Account{Block: provider.BlockAuth, Detail: "GET /me answered 401"}, nil
	case status == http.StatusForbidden:
		unreadable = true
	case err != nil:
		return provider.Account{}, err
	case me.User.AccountStatus == "suspended" || me.User.AccountStatus == "closed" ||
		me.User.Suspended || me.User.Closed:
		return provider.Account{
			Block: provider.BlockBanned, Detail: accountDetail(me),
		}, nil
	}
	reply, status, err := p.readLimitsStatus(ctx)
	switch {
	case status == http.StatusUnauthorized:
		return provider.Account{Block: provider.BlockAuth, Detail: "GET /limits answered 401"}, nil
	case status == http.StatusForbidden:
		unreadable = true
	case err != nil:
		return provider.Account{}, err
	case reply.Wallet != nil && reply.Wallet.BalanceDollars != nil &&
		*reply.Wallet.BalanceDollars < 0:
		return provider.Account{Block: provider.BlockUnfunded, Detail: "wallet below zero"}, nil
	case reply.BlockedReason != nil && *reply.BlockedReason != "":
		return provider.Account{
			Block: provider.BlockUnfunded, Detail: "blocked: " + *reply.BlockedReason,
		}, nil
	}
	if !unreadable {
		return provider.Account{}, nil
	}
	p.unreadableOnce.Do(func() {
		slog.Warn("boat account unreadable, the runtime key lacks account.read")
	})
	// A call the runtime key must be able to make tells whether the key works at all
	status, err = p.listStatus(ctx)
	switch {
	case status == http.StatusForbidden || status == http.StatusUnauthorized:
		return provider.Account{
			Block: provider.BlockAuth, Detail: "GET /sandboxes answered " + http.StatusText(status),
		}, nil
	case err != nil:
		return provider.Account{}, err
	}
	return provider.Account{}, nil
}

func accountDetail(me meReply) string {
	detail := "account " + me.User.AccountStatus
	if me.User.SuspendedReason != nil && *me.User.SuspendedReason != "" {
		detail += ": " + *me.User.SuspendedReason
	}
	return detail
}

// statusOf is the HTTP status of a Boat error answer, 0 for any other error
func statusOf(err error) int {
	if a, ok := errors.AsType[*answer](asInner(err)); ok {
		return a.status
	}
	return 0
}

// asInner returns the answer inside a provider error
func asInner(err error) error {
	if e, ok := provider.AsError(err); ok {
		return e.Err
	}
	return err
}

// readMe calls GET /me, status holding the answer's status when Boat refused the call
func (p *Provider) readMe(ctx context.Context) (meReply, int, error) {
	raw, err := p.send(ctx, noEffect, func(ctx context.Context) (*http.Response, error) {
		return p.api.Me(ctx)
	})
	if err != nil {
		return meReply{}, statusOf(err), err
	}
	var me meReply
	if err := decode(raw, &me); err != nil {
		return meReply{}, 0, internal(err)
	}
	return me, 0, nil
}

// readLimitsStatus reads /limits fresh, status holding the answer's status when Boat refused it
func (p *Provider) readLimitsStatus(ctx context.Context) (limitsReply, int, error) {
	reply, _, err := p.limits(ctx, pollEvery)
	if err != nil {
		return limitsReply{}, statusOf(err), err
	}
	return reply, 0, nil
}

// listStatus lists one sandbox, status holding the answer's status when Boat refused the call
func (p *Provider) listStatus(ctx context.Context) (int, error) {
	params := &api.SandboxesParams{Limit: new(1)}
	_, err := p.send(ctx, noEffect, func(ctx context.Context) (*http.Response, error) {
		return p.api.Sandboxes(ctx, params)
	})
	if err != nil {
		return statusOf(err), err
	}
	return 0, nil
}
