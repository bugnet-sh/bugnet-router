package main

import (
	"cmp"
	"flag"
	"fmt"
	"strconv"
	"strings"
	"time"

	"connectrpc.com/connect"
	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// wallet runs a wallet subcommand through OperatorService
func (c *cli) wallet(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("%w: wallet takes block, unblock, show or list", errUsage)
	}
	sub := map[string]func([]string) error{
		"block": c.walletBlock, "unblock": c.walletUnblock, "show": c.walletShow,
		"list": c.walletList,
	}[args[0]]
	if sub == nil {
		return fmt.Errorf("%w: wallet takes block, unblock, show or list", errUsage)
	}
	return sub(args[1:])
}

// payerArg parses fs with one payer before or after the flags
func payerArg(fs *flag.FlagSet, args []string) (string, error) {
	var payer string
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		payer, args = args[0], args[1:]
	}
	if err := parse(fs, args, true); err != nil {
		return "", err
	}
	switch {
	case payer == "" && fs.NArg() == 1:
		payer = fs.Arg(0)
	case fs.NArg() > 0 || payer == "":
		return "", fmt.Errorf("%w: wallet takes one payer address", errUsage)
	}
	return payer, nil
}

// actorOf is --actor, or $USER when it is not given
func (c *cli) actorOf(given string) (string, error) {
	who := cmp.Or(given, c.getenv("USER"))
	if who == "" {
		return "", fmt.Errorf("%w: --actor is required when USER is not set", errUsage)
	}
	return who, nil
}

// untilOf reads --until as an RFC 3339 time or a duration from now, nil when empty
func untilOf(raw string, now time.Time) (*timestamppb.Timestamp, error) {
	if raw == "" {
		return nil, nil
	}
	if t, err := time.Parse(time.RFC3339, raw); err == nil {
		return timestamppb.New(t), nil
	}
	if d, err := time.ParseDuration(raw); err == nil && d > 0 {
		return timestamppb.New(now.Add(d)), nil
	}
	return nil, fmt.Errorf("%w: --until must be an RFC 3339 time or a positive duration such "+
		"as 24h", errUsage)
}

func (c *cli) walletBlock(args []string) error {
	fs := newFlags("wallet block")
	reason := fs.String("reason", "", "why, kept as the block event's evidence")
	until := fs.String("until", "", "an RFC 3339 time or a duration from now, none for ever")
	stopLive := fs.Bool("stop-live", false, "end the wallet's live placements too")
	actor := fs.String("actor", "", "who blocks, $USER when empty")
	payer, err := payerArg(fs, args)
	if err != nil {
		return err
	}
	if err := required("reason", *reason); err != nil {
		return err
	}
	who, err := c.actorOf(*actor)
	if err != nil {
		return err
	}
	at, err := untilOf(*until, time.Now())
	if err != nil {
		return err
	}
	op, err := c.operator()
	if err != nil {
		return err
	}
	res, err := op.BlockWallet(c.ctx, connect.NewRequest(&routerv1.BlockWalletRequest{
		Payer: payer, Reason: *reason, Until: at, StopLive: *stopLive, Actor: who,
	}))
	if err != nil {
		return err
	}
	if err := c.printWallet(res.Msg.GetWallet()); err != nil {
		return err
	}
	for _, id := range res.Msg.GetEndingPlacementIds() {
		if err := c.print([][2]string{{"ending", id}}); err != nil {
			return err
		}
	}
	return nil
}

func (c *cli) walletUnblock(args []string) error {
	fs := newFlags("wallet unblock")
	note := fs.String("note", "", "why, kept as the unblock event's reason")
	actor := fs.String("actor", "", "who unblocks, $USER when empty")
	payer, err := payerArg(fs, args)
	if err != nil {
		return err
	}
	who, err := c.actorOf(*actor)
	if err != nil {
		return err
	}
	op, err := c.operator()
	if err != nil {
		return err
	}
	res, err := op.UnblockWallet(c.ctx, connect.NewRequest(&routerv1.UnblockWalletRequest{
		Payer: payer, Note: *note, Actor: who,
	}))
	if err != nil {
		return err
	}
	return c.printWallet(res.Msg.GetWallet())
}

func (c *cli) walletShow(args []string) error {
	payer, err := payerArg(newFlags("wallet show"), args)
	if err != nil {
		return err
	}
	op, err := c.operator()
	if err != nil {
		return err
	}
	res, err := op.GetWallet(c.ctx, connect.NewRequest(&routerv1.GetWalletRequest{
		Payer: payer,
	}))
	if err != nil {
		return err
	}
	if err := c.printWallet(res.Msg.GetWallet()); err != nil {
		return err
	}
	var b strings.Builder
	for _, p := range res.Msg.GetLive() {
		fmt.Fprintf(&b, "live %s state=%s provider=%s package_id=%s\n", p.GetPlacementId(),
			stateName(p.GetState()), p.GetProvider(), p.GetPackageId())
	}
	for _, e := range res.Msg.GetEvents() {
		fmt.Fprintf(&b, "event %s kind=%s actor=%s reason=%q\n", stamp(e.GetCreatedAt()),
			e.GetKind(), e.GetActor(), e.GetReason())
	}
	_, err = fmt.Fprint(c.stdout, b.String())
	return err
}

func (c *cli) walletList(args []string) error {
	fs := newFlags("wallet list")
	blocked := fs.Bool("blocked", false, "only wallets blocked now")
	limit := fs.Uint("limit", 0, "wallets a page, 100 when 0, at most 1000")
	page := fs.String("page", "", "the next_page_token of the page before")
	if err := parse(fs, args, false); err != nil {
		return err
	}
	op, err := c.operator()
	if err != nil {
		return err
	}
	res, err := op.ListWallets(c.ctx, connect.NewRequest(&routerv1.ListWalletsRequest{
		BlockedOnly: *blocked, Limit: uint32(min(*limit, 1<<31)), PageToken: *page,
	}))
	if err != nil {
		return err
	}
	var b strings.Builder
	for _, w := range res.Msg.GetWallets() {
		fmt.Fprintf(&b, "%s blocked=%t reason=%s live=%d strikes=%d\n", w.GetPayer(),
			w.GetBlocked(), w.GetBlockedReason(), w.GetLivePlacements(), w.GetStrikes())
	}
	if next := res.Msg.GetNextPageToken(); next != "" {
		b.WriteString("next_page_token=" + next + "\n")
	}
	_, err = fmt.Fprint(c.stdout, b.String())
	return err
}

func (c *cli) printWallet(w *routerv1.Wallet) error {
	kv := [][2]string{
		{"payer", w.GetPayer()},
		{"blocked", strconv.FormatBool(w.GetBlocked())},
	}
	if w.GetBlocked() {
		until := "lifted by an operator"
		if w.GetBlockedUntil() != nil {
			until = stamp(w.GetBlockedUntil())
		}
		kv = append(kv, [2]string{"blocked_at", stamp(w.GetBlockedAt())},
			[2]string{"blocked_until", until}, [2]string{"blocked_reason", w.GetBlockedReason()},
			[2]string{"blocked_by", w.GetBlockedBy()})
	}
	kv = append(kv,
		[2]string{"strikes", strconv.FormatUint(uint64(w.GetStrikes()), 10)},
		[2]string{"live_placements", strconv.FormatUint(uint64(w.GetLivePlacements()), 10)},
		[2]string{"first_seen", stamp(w.GetFirstSeen())},
		[2]string{"last_seen", stamp(w.GetLastSeen())})
	return c.print(kv)
}
