package main

import (
	"flag"
	"fmt"
	"net/url"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"

	"connectrpc.com/connect"
	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/bugnet-sh/bugnet-router/internal/preview"
)

const targetForms = "a placement id, a preview URL or id, a transfer URL or token, " +
	"or PROVIDER:SANDBOX_ID"

var sandboxProvider = regexp.MustCompile(`^[a-z][a-z0-9_-]*$`)

// abuse runs an abuse subcommand through OperatorService
func (c *cli) abuse(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("%w: abuse takes find or end", errUsage)
	}
	switch args[0] {
	case "find":
		return c.findPlacement(args[1:])
	case "end":
		return c.endPlacement("abuse end", routerv1.OperatorEndReason_OPERATOR_END_REASON_ABUSE,
			args[1:])
	}
	return fmt.Errorf("%w: abuse takes find or end", errUsage)
}

// placementCmd runs a placement subcommand through OperatorService
func (c *cli) placementCmd(args []string) error {
	if len(args) == 0 || args[0] != "end" {
		return fmt.Errorf("%w: placement takes end", errUsage)
	}
	return c.endPlacement("placement end",
		routerv1.OperatorEndReason_OPERATOR_END_REASON_OPERATOR, args[1:])
}

// targetArg parses fs with one target before or after the flags
func targetArg(fs *flag.FlagSet, args []string) (*routerv1.PlacementTarget, error) {
	var raw string
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		raw, args = args[0], args[1:]
	}
	if err := parse(fs, args, true); err != nil {
		return nil, err
	}
	switch {
	case raw == "" && fs.NArg() == 1:
		raw = fs.Arg(0)
	case fs.NArg() > 0 || raw == "":
		return nil, fmt.Errorf("%w: takes one target, %s", errUsage, targetForms)
	}
	return parseTarget(raw)
}

// parseTarget names what an abuse report holds: a placement id, a preview URL or id, a transfer
// URL or token, or PROVIDER:SANDBOX_ID
func parseTarget(raw string) (*routerv1.PlacementTarget, error) {
	raw = strings.TrimSpace(raw)
	if _, err := uuid.Parse(raw); err == nil {
		return &routerv1.PlacementTarget{
			Target: &routerv1.PlacementTarget_PlacementId{PlacementId: raw},
		}, nil
	}
	if preview.ValidID(raw) {
		return &routerv1.PlacementTarget{
			Target: &routerv1.PlacementTarget_PreviewId{PreviewId: raw},
		}, nil
	}
	if strings.HasPrefix(raw, "bnx_") {
		return transferTarget(raw), nil
	}
	if prov, sid, ok := strings.Cut(raw, ":"); ok && sandboxProvider.MatchString(prov) &&
		sid != "" && !strings.HasPrefix(sid, "//") {
		return &routerv1.PlacementTarget{Target: &routerv1.PlacementTarget_ProviderSandbox{
			ProviderSandbox: &routerv1.ProviderSandbox{Provider: prov, SandboxId: sid},
		}}, nil
	}
	if strings.Contains(raw, ".") {
		return urlTarget(raw)
	}
	return nil, fmt.Errorf("%w: %q is not %s", errUsage, raw, targetForms)
}

func transferTarget(raw string) *routerv1.PlacementTarget {
	return &routerv1.PlacementTarget{Target: &routerv1.PlacementTarget_Transfer{Transfer: raw}}
}

// urlTarget reads a link: a transfer URL holds a bnx_ token as its path, a preview URL names the
// preview in the first label of its host
func urlTarget(raw string) (*routerv1.PlacementTarget, error) {
	link := raw
	if !strings.Contains(link, "://") {
		link = "https://" + link
	}
	u, err := url.Parse(link)
	if err != nil || u.Hostname() == "" {
		return nil, fmt.Errorf("%w: %q is not %s", errUsage, raw, targetForms)
	}
	if strings.HasPrefix(path.Base(strings.TrimRight(u.Path, "/")), "bnx_") {
		return transferTarget(link), nil
	}
	label, _, _ := strings.Cut(u.Hostname(), ".")
	if preview.ValidID(label) {
		return &routerv1.PlacementTarget{
			Target: &routerv1.PlacementTarget_PreviewId{PreviewId: label},
		}, nil
	}
	return nil, fmt.Errorf("%w: %q is not a preview or transfer link", errUsage, raw)
}

func (c *cli) findPlacement(args []string) error {
	fs := newFlags("abuse find")
	tg, err := targetArg(fs, args)
	if err != nil {
		return err
	}
	op, err := c.operator()
	if err != nil {
		return err
	}
	res, err := op.FindPlacement(c.ctx, connect.NewRequest(&routerv1.FindPlacementRequest{
		Target: tg,
	}))
	if err != nil {
		return err
	}
	return c.printOperatorPlacement(res.Msg.GetPlacement(), nil)
}

func (c *cli) endPlacement(name string, reason routerv1.OperatorEndReason, args []string) error {
	fs := newFlags(name)
	note := fs.String("note", "", "why, kept as evidence")
	actor := fs.String("actor", "", "who ends it, $USER when empty")
	var refund *bool
	if reason == routerv1.OperatorEndReason_OPERATOR_END_REASON_OPERATOR {
		refund = fs.Bool("refund", false, "refund the unused time")
	}
	tg, err := targetArg(fs, args)
	if err != nil {
		return err
	}
	if err := required("note", *note); err != nil {
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
	res, err := op.EndPlacement(c.ctx, connect.NewRequest(&routerv1.EndPlacementRequest{
		Target: tg, Reason: reason, Refundable: refund != nil && *refund, Note: *note,
		Actor: who,
	}))
	if err != nil {
		return err
	}
	return c.printOperatorPlacement(res.Msg.GetPlacement(), res.Msg.GetBlockedPayers())
}

func (c *cli) printOperatorPlacement(p *routerv1.OperatorPlacement, blocked []string) error {
	kv := [][2]string{
		{"placement_id", p.GetPlacementId()},
		{"state", stateName(p.GetState())},
		{"provider", p.GetProvider()},
		{"provider_sandbox_id", p.GetProviderSandboxId()},
		{"package_id", p.GetPackageId()},
		{"payer", p.GetPayer()},
		{"extend_payers", strings.Join(p.GetExtendPayers(), ",")},
		{"source_label", p.GetSourceLabel()},
		{"termination_reason", p.GetTerminationReason()},
		{"created_at", stamp(p.GetCreatedAt())},
	}
	if p.GetDeadline() != nil {
		kv = append(kv, [2]string{"deadline", stamp(p.GetDeadline())})
	}
	if p.GetStartPath() != "" {
		kv = append(kv, [2]string{"start_path", p.GetStartPath()})
	}
	if p.GetHedged() {
		kv = append(kv, [2]string{"hedged", "true"})
	}
	if p.GetOrderTraceId() != "" {
		kv = append(kv, [2]string{"order_trace_id", p.GetOrderTraceId()})
	}
	if blocked != nil {
		kv = append(kv, [2]string{"blocked_payers", strings.Join(blocked, ",")})
	}
	return c.print(kv)
}

// sinceOf reads --since as an RFC 3339 time or a duration back from now, nil when empty
func sinceOf(raw string, now time.Time) (*timestamppb.Timestamp, error) {
	if raw == "" {
		return nil, nil
	}
	if t, err := time.Parse(time.RFC3339, raw); err == nil {
		return timestamppb.New(t), nil
	}
	if d, err := time.ParseDuration(raw); err == nil && d > 0 {
		return timestamppb.New(now.Add(-d)), nil
	}
	return nil, fmt.Errorf("%w: --since must be an RFC 3339 time or a positive duration such "+
		"as 24h", errUsage)
}

var signalClassNames = map[string]routerv1.SignalClass{
	"abuse":           routerv1.SignalClass_SIGNAL_CLASS_ABUSE,
	"control_lost":    routerv1.SignalClass_SIGNAL_CLASS_CONTROL_LOST,
	"provider_failed": routerv1.SignalClass_SIGNAL_CLASS_PROVIDER_FAILED,
	"account":         routerv1.SignalClass_SIGNAL_CLASS_ACCOUNT,
	"sandbox_exited":  routerv1.SignalClass_SIGNAL_CLASS_SANDBOX_EXITED,
}

func (c *cli) signals(args []string) error {
	fs := newFlags("signals")
	since := fs.String("since", "", "an RFC 3339 time or a duration back from now, such as 24h")
	prov := fs.String("provider", "", "only this provider")
	class := fs.String("class", "", "only abuse, control_lost, provider_failed, account or "+
		"sandbox_exited")
	held := fs.Bool("held", false, "only signals whose wallet actions the guard held")
	limit := fs.Uint("limit", 0, "signals to list, 100 when 0, at most 1000")
	if err := parse(fs, args, false); err != nil {
		return err
	}
	at, err := sinceOf(*since, time.Now())
	if err != nil {
		return err
	}
	req := &routerv1.ListSignalsRequest{
		Since: at, Provider: *prov, HeldOnly: *held, Limit: uint32(min(*limit, 1<<31)),
	}
	if *class != "" {
		cl, ok := signalClassNames[*class]
		if !ok {
			return fmt.Errorf("%w: --class must be abuse, control_lost, provider_failed, "+
				"account or sandbox_exited", errUsage)
		}
		req.SignalClass = cl
	}
	op, err := c.operator()
	if err != nil {
		return err
	}
	res, err := op.ListSignals(c.ctx, connect.NewRequest(req))
	if err != nil {
		return err
	}
	var b strings.Builder
	for _, s := range res.Msg.GetSignals() {
		fmt.Fprintf(&b, "%s class=%s provider=%s source=%s action=%s placement=%s observed=%s\n",
			stamp(s.GetCreatedAt()), strings.ToLower(strings.TrimPrefix(
				s.GetSignalClass().String(), "SIGNAL_CLASS_")), s.GetProvider(), s.GetSource(),
			s.GetAction(), s.GetPlacementId(), s.GetObservedJson())
	}
	_, err = fmt.Fprint(c.stdout, b.String())
	return err
}

func (c *cli) providers(args []string) error {
	if err := parse(newFlags("providers"), args, false); err != nil {
		return err
	}
	op, err := c.operator()
	if err != nil {
		return err
	}
	res, err := op.ListProviders(c.ctx, connect.NewRequest(&routerv1.ListProvidersRequest{}))
	if err != nil {
		return err
	}
	u := func(n uint32) string { return strconv.FormatUint(uint64(n), 10) }
	var b strings.Builder
	for _, p := range res.Msg.GetProviders() {
		polled := "never"
		if p.GetPolledAt() != nil {
			polled = stamp(p.GetPolledAt())
		}
		fmt.Fprintf(&b, "%s state=%s blocked=%s samples=%s success_lower_bound=%.3f "+
			"ready_p95_ms=%s in_use=%s external_in_use=%s max_concurrent=%s limit_source=%s "+
			"polled_at=%s foreign_sandboxes=%s image_version=%s image_accepted=%t\n",
			p.GetProvider(), p.GetState(),
			p.GetBlocked(), u(p.GetSamples()), p.GetSuccessLowerBound(), u(p.GetReadyP95Ms()),
			u(p.GetInUse()), u(p.GetExternalInUse()), u(p.GetMaxConcurrent()),
			p.GetLimitSource(), polled, u(p.GetForeignSandboxes()), p.GetImageVersion(),
			p.GetImageAccepted())
	}
	_, err = fmt.Fprint(c.stdout, b.String())
	return err
}
