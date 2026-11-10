package main

import (
	"strings"

	"connectrpc.com/connect"
	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
)

// reserve asks the router to hold a prepared sandbox for a quote, so create --quote Q can bind it
func (c *cli) reserve(args []string) error {
	fs := newFlags("reserve")
	quote := fs.String("quote", "", "quote id, 1 to 128 printable ASCII bytes")
	pkg := fs.String("package", "", "package id")
	ssh := fs.Bool("ssh", false, "the order will need SSH")
	preview := fs.Bool("preview", false, "the order will need previews")
	secrets := fs.Bool("secrets", false, "the order will need secrets")
	ip := fs.String("client-ip", "", "the client's address, for the per-address cap")
	if err := parse(fs, args, false); err != nil {
		return err
	}
	if err := required("quote", *quote); err != nil {
		return err
	}
	if err := required("package", *pkg); err != nil {
		return err
	}
	api, err := c.client()
	if err != nil {
		return err
	}
	res, err := api.Reserve(c.ctx, connect.NewRequest(&routerv1.ReserveRequest{
		QuoteId: *quote, PackageId: *pkg, ClientIp: *ip,
		Requirements: &routerv1.Requirements{Ssh: *ssh, Preview: *preview, Secrets: *secrets},
	}))
	if err != nil {
		return err
	}
	kv := [][2]string{{"outcome", enumName(res.Msg.GetOutcome().String(), "RESERVE_OUTCOME_")}}
	if res.Msg.GetOutcome() == routerv1.ReserveOutcome_RESERVE_OUTCOME_SKIPPED {
		kv = append(kv, [2]string{"skip", enumName(res.Msg.GetSkip().String(), "RESERVE_SKIP_")})
	} else {
		kv = append(kv, [2]string{"expires_at", stamp(res.Msg.GetExpiresAt())})
	}
	return c.print(kv)
}

// enumName is a proto enum's name without its prefix, in lower case
func enumName(name, prefix string) string {
	return strings.ToLower(strings.TrimPrefix(name, prefix))
}
