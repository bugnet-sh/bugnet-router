package health

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/riverqueue/river"

	"github.com/bugnet-sh/bugnet-router/internal/jobargs"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

// noteWindow is how long one account_check request holds back the next one for a provider
const noteWindow = 30 * time.Second

// NoteAccount asks for an account check when code says the account may be at fault, else nothing
func NoteAccount(
	ctx context.Context, rc *river.Client[pgx.Tx], name string, code provider.Code,
) {
	if provider.AccountBlock(code) == provider.BlockNone {
		return
	}
	RequestAccountCheck(ctx, rc, name)
}

// RequestAccountCheck asks for an account check at once, one request per provider every 30 s
func RequestAccountCheck(ctx context.Context, rc *river.Client[pgx.Tx], name string) {
	if rc == nil {
		return
	}
	_, err := rc.Insert(ctx, jobargs.AccountCheck{Provider: name, Now: true}, &river.InsertOpts{
		UniqueOpts: river.UniqueOpts{ByArgs: true, ByPeriod: noteWindow},
	})
	if err != nil {
		slog.Warn("cannot ask for an account check", "provider", name, "err", err)
	}
}

// NoteQuota asks for a limits poll of provider, since a create met the account's own cap
func NoteQuota(ctx context.Context, rc *river.Client[pgx.Tx], name string) {
	if rc == nil {
		return
	}
	if _, err := rc.Insert(ctx, jobargs.LimitsPoll{Provider: name}, nil); err != nil {
		slog.Warn("cannot ask for a limits poll", "provider", name, "err", err)
	}
}
