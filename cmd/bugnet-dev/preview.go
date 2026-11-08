package main

import (
	"cmp"
	"fmt"
	"strconv"
	"strings"

	"connectrpc.com/connect"
	routerv1 "github.com/oynozan/bugnet-proto/gen/go/bugnet/router/v1"
)

// preview prints the link to one port of a placement
func (c *cli) preview(args []string) error {
	fs := newFlags("preview")
	h := fs.String("handle", "", "sandbox handle")
	port := fs.String("port", "", "port inside the sandbox")
	if err := parse(fs, args, false); err != nil {
		return err
	}
	if err := required("handle", *h); err != nil {
		return err
	}
	n, err := strconv.ParseUint(*port, 10, 16)
	if err != nil || n == 0 {
		return fmt.Errorf("%w: --port must be a port from 1 to 65535", errUsage)
	}
	api, err := c.client()
	if err != nil {
		return err
	}
	res, err := api.CreatePreview(c.ctx, connect.NewRequest(&routerv1.CreatePreviewRequest{
		Handle: *h, Port: uint32(n),
	}))
	if err != nil {
		return err
	}
	m := res.Msg
	return c.print([][2]string{
		{"url", m.GetUrl()}, {"preview_id", m.GetPreviewId()},
		{"expires_at", stamp(m.GetExpiresAt())},
		{"listening", strconv.FormatBool(m.GetListening())},
	})
}

// revokePreview revokes a preview through OperatorService, the actor from $USER by default
func (c *cli) revokePreview(args []string) error {
	fs := newFlags("revoke-preview")
	reason := fs.String("reason", "", "why, kept in the router's log")
	actor := fs.String("actor", "", "who revokes, $USER when empty")
	// The id may come before the flags, which the flag package would stop at
	var id string
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		id, args = args[0], args[1:]
	}
	if err := parse(fs, args, true); err != nil {
		return err
	}
	switch {
	case id == "" && fs.NArg() == 1:
		id = fs.Arg(0)
	case fs.NArg() > 0 || id == "":
		return fmt.Errorf("%w: revoke-preview takes one preview id", errUsage)
	}
	if err := required("reason", *reason); err != nil {
		return err
	}
	who := cmp.Or(*actor, c.getenv("USER"))
	if who == "" {
		return fmt.Errorf("%w: --actor is required when USER is not set", errUsage)
	}
	op, err := c.operator()
	if err != nil {
		return err
	}
	if _, err := op.RevokePreview(c.ctx, connect.NewRequest(&routerv1.RevokePreviewRequest{
		PreviewId: id, Reason: *reason, Actor: who,
	})); err != nil {
		return err
	}
	return c.print([][2]string{{"preview_id", id}, {"revoked", "true"}})
}
