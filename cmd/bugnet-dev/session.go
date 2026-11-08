package main

import (
	"fmt"

	"connectrpc.com/connect"
	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
)

// session runs session hold and session rotate
func (c *cli) session(args []string) error {
	if len(args) > 0 {
		switch args[0] {
		case "hold":
			return c.sessionHold(args[1:])
		case "rotate":
			return c.sessionRotate(args[1:])
		}
	}
	return fmt.Errorf("%w: session takes hold or rotate", errUsage)
}

// sessionHold prints a held session record, which only OperatorService serves
func (c *cli) sessionHold(args []string) error {
	fs := newFlags("session hold")
	id := fs.String("placement", "", "placement id")
	if err := parse(fs, args, false); err != nil {
		return err
	}
	if err := required("placement", *id); err != nil {
		return err
	}
	op, err := c.operator()
	if err != nil {
		return err
	}
	res, err := op.GetSessionHold(c.ctx, connect.NewRequest(&routerv1.GetSessionHoldRequest{
		PlacementId: *id,
	}))
	if err != nil {
		return err
	}
	return c.print([][2]string{
		{"closed_by", res.Msg.GetClosedBy()}, {"hold_until", stamp(res.Msg.GetHoldUntil())},
		{"record", string(res.Msg.GetRecordJson())},
	})
}

// sessionRotate ends a placement's current session link and prints the new one
func (c *cli) sessionRotate(args []string) error {
	h, err := handleFlag("session rotate", args)
	if err != nil {
		return err
	}
	api, err := c.client()
	if err != nil {
		return err
	}
	got, err := api.GetPlacement(c.ctx, connect.NewRequest(&routerv1.GetPlacementRequest{
		Handle: h,
	}))
	if err != nil {
		return err
	}
	link := got.Msg.GetPlacement().GetSessionUrl()
	if link == "" {
		return fmt.Errorf("the placement has no session link")
	}
	res, err := api.RotateSessionLink(c.ctx, connect.NewRequest(
		&routerv1.RotateSessionLinkRequest{Handle: h, SessionUrl: link}))
	if err != nil {
		return err
	}
	return c.print([][2]string{{"session_url", res.Msg.GetSessionUrl()}})
}
