package main

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"

	"connectrpc.com/connect"
	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
	"google.golang.org/genproto/googleapis/rpc/errdetails"

	"github.com/bugnet-sh/bugnet-router/internal/config"
)

// x402 runs x402 check, enable or list
func (c *cli) x402(args []string) error {
	if len(args) > 0 {
		switch args[0] {
		case "check":
			return c.x402Check(args[1:])
		case "enable":
			return c.x402Enable(args[1:])
		case "list":
			return c.x402List(args[1:])
		case "seller-check":
			return c.x402SellerCheck(args[1:])
		case "allowed-hosts":
			return c.x402AllowedHosts(args[1:])
		}
	}
	return fmt.Errorf("%w: x402 takes check, enable, list, seller-check or allowed-hosts",
		errUsage)
}

// x402Index is the reviewed index's part that allowed-hosts reads (XS 5.1)
type x402Index struct {
	V        int `json:"v"`
	Services []struct {
		Host string `json:"host"`
	} `json:"services"`
}

// x402AllowedHosts prints the router's allowed_shared_hosts line for a reviewed index: its hosts
// under one of the settings file's shared_hosting_suffixes, sorted and listed once (XS 6.16)
func (c *cli) x402AllowedHosts(args []string) error {
	fs := newFlags("x402 allowed-hosts")
	index := fs.String("index", "", "the reviewed index, x402-index.json")
	settings := fs.String("config", "", "the router's settings file")
	if err := parse(fs, args, false); err != nil {
		return err
	}
	for _, f := range [][2]string{{"index", *index}, {"config", *settings}} {
		if err := required(f[0], f[1]); err != nil {
			return err
		}
	}
	cfg, err := config.LoadFile(*settings)
	if err != nil {
		return err
	}
	raw, err := os.ReadFile(*index)
	if err != nil {
		return err
	}
	var idx x402Index
	if err := json.Unmarshal(raw, &idx); err != nil {
		return fmt.Errorf("%s: %w", *index, err)
	}
	if idx.V != 1 {
		return fmt.Errorf("%s: v must be 1", *index)
	}
	var hosts []string
	for _, s := range idx.Services {
		host := strings.TrimSuffix(strings.ToLower(s.Host), ".")
		for _, suffix := range cfg.X402.SharedHostingSuffixes {
			if host == suffix || strings.HasSuffix(host, "."+suffix) {
				hosts = append(hosts, host)
				break
			}
		}
	}
	slices.Sort(hosts)
	quotedHosts := make([]string, 0, len(hosts))
	for _, h := range slices.Compact(hosts) {
		quotedHosts = append(quotedHosts, strconv.Quote(h))
	}
	_, err = fmt.Fprintf(c.stdout, "allowed_shared_hosts = [%s]\n", strings.Join(quotedHosts, ", "))
	return err
}

// x402Check runs CheckX402, with no handle only the checks that need no placement
func (c *cli) x402Check(args []string) error {
	fs := newFlags("x402 check")
	h := fs.String("handle", "", "sandbox handle, empty for the checks that need no placement")
	var add, disable listFlag
	fs.Var(&add, "service", "method,host,prefix,max_price_micros,pay_to[,paid_path], repeatable")
	fs.Var(&disable, "disable", "service id to disable, repeatable")
	if err := parse(fs, args, false); err != nil {
		return err
	}
	req := &routerv1.CheckX402Request{Handle: *h, Disable: disable}
	for i, v := range add {
		spec, err := serviceFlag(v)
		if err != nil {
			return fmt.Errorf("%w: --service %d %w", errUsage, i+1, err)
		}
		req.Add = append(req.Add, spec)
	}
	api, err := c.client()
	if err != nil {
		return err
	}
	if _, err := api.CheckX402(c.ctx, connect.NewRequest(req)); err != nil {
		return refusal(err)
	}
	return c.print([][2]string{{"passed", "true"}})
}

// x402Enable runs EnableX402 and prints the placement's services, one a line
func (c *cli) x402Enable(args []string) error {
	fs := newFlags("x402 enable")
	h := fs.String("handle", "", "sandbox handle")
	wait := fs.Bool("wait", false, "wait for the added services to leave applying")
	reqID := fs.String("request-id", "", "request id, a fresh one when empty")
	var add, disable listFlag
	fs.Var(&add, "service", "method,host,prefix,max_price_micros,pay_to[,paid_path], repeatable")
	fs.Var(&disable, "disable", "service id to disable, repeatable")
	if err := parse(fs, args, false); err != nil {
		return err
	}
	if err := required("handle", *h); err != nil {
		return err
	}
	req := &routerv1.EnableX402Request{
		Handle: *h, Disable: disable, WaitActive: *wait, RequestId: *reqID,
	}
	if req.RequestId == "" {
		req.RequestId = "dev-" + rand.Text()
	}
	for i, v := range add {
		spec, err := serviceFlag(v)
		if err != nil {
			return fmt.Errorf("%w: --service %d %w", errUsage, i+1, err)
		}
		req.Add = append(req.Add, spec)
	}
	api, err := c.client()
	if err != nil {
		return err
	}
	res, err := api.EnableX402(c.ctx, connect.NewRequest(req))
	if err != nil {
		return refusal(err)
	}
	lines := make([]string, 0, len(res.Msg.GetServices()))
	for _, s := range res.Msg.GetServices() {
		lines = append(lines, serviceLine(s))
	}
	_, err = fmt.Fprintln(c.stdout, strings.Join(lines, "\n"))
	return err
}

// x402List prints the placement's x402 services, one a line, then its refusals
func (c *cli) x402List(args []string) error {
	fs := newFlags("x402 list")
	h := fs.String("handle", "", "sandbox handle")
	if err := parse(fs, args, false); err != nil {
		return err
	}
	if err := required("handle", *h); err != nil {
		return err
	}
	api, err := c.client()
	if err != nil {
		return err
	}
	res, err := api.ListX402(c.ctx, connect.NewRequest(&routerv1.ListX402Request{Handle: *h}))
	if err != nil {
		return refusal(err)
	}
	lines := []string{"enabled=" + strconv.FormatBool(res.Msg.GetEnabled())}
	for _, s := range res.Msg.GetServices() {
		lines = append(lines, serviceLine(s))
	}
	for _, r := range res.Msg.GetRefusals() {
		lines = append(lines, fmt.Sprintf("refusal=%s count=%d last_at=%s", r.GetReason(),
			r.GetCount(), stamp(r.GetLastAt())))
	}
	_, err = fmt.Fprintln(c.stdout, strings.Join(lines, "\n"))
	return err
}

// serviceFlag reads method,host,prefix,max_price_micros,pay_to[,paid_path], indexed without a path
func serviceFlag(v string) (*routerv1.X402ServiceSpec, error) {
	// The paid path is last, so it keeps any comma it holds
	parts := strings.SplitN(v, ",", 6)
	if len(parts) < 5 {
		return nil, errors.New("must be method,host,prefix,max_price_micros,pay_to[,paid_path]")
	}
	price, err := strconv.ParseInt(parts[3], 10, 64)
	if err != nil {
		return nil, errors.New("max_price_micros must be a whole number")
	}
	s := &routerv1.X402ServiceSpec{
		Method: parts[0], Host: parts[1], Prefix: parts[2], MaxPriceMicros: price,
		PayTo: parts[4], Indexed: len(parts) == 5,
	}
	if !s.Indexed {
		s.PaidPath = parts[5]
	}
	return s, nil
}

// serviceLine is one service as space-separated key=value pairs
func serviceLine(s *routerv1.X402ServiceInfo) string {
	spec := s.GetSpec()
	state := strings.ToLower(strings.TrimPrefix(s.GetState().String(), "X402_SERVICE_STATE_"))
	kv := []string{
		"service=" + s.GetServiceId(), "state=" + state, "method=" + spec.GetMethod(),
		"host=" + spec.GetHost(), "prefix=" + spec.GetPrefix(),
		"indexed=" + strconv.FormatBool(spec.GetIndexed()),
	}
	if !spec.GetIndexed() {
		kv = append(kv, "paid_path="+spec.GetPaidPath())
	}
	kv = append(kv, "max_price_micros="+strconv.FormatInt(spec.GetMaxPriceMicros(), 10),
		"pay_to="+spec.GetPayTo(), fmt.Sprintf("requests=%d", s.GetRequests()),
		fmt.Sprintf("paid=%d", s.GetPaid()), fmt.Sprintf("uncertain=%d", s.GetUncertain()))
	if s.GetFailureReason() != "" {
		kv = append(kv, "failure_reason="+s.GetFailureReason())
	}
	return strings.Join(kv, " ")
}

// refusal names a Connect error's ErrorInfo reason and metadata, sorted by key
func refusal(err error) error {
	var ce *connect.Error
	if !errors.As(err, &ce) {
		return err
	}
	for _, d := range ce.Details() {
		v, derr := d.Value()
		info, ok := v.(*errdetails.ErrorInfo)
		if derr != nil || !ok {
			continue
		}
		words := []string{ce.Code().String(), info.GetReason()}
		var meta []string
		for k, v := range info.GetMetadata() {
			meta = append(meta, k+"="+v)
		}
		slices.Sort(meta)
		return fmt.Errorf("%s: %s", strings.Join(append(words, meta...), " "), ce.Message())
	}
	return err
}
