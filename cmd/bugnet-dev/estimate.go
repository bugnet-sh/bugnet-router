package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"

	"connectrpc.com/connect"
	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"

	"github.com/bugnet-sh/bugnet-router/internal/estimator"
	"github.com/bugnet-sh/bugnet-router/internal/estimator/rules"
)

// estimate runs the estimate subcommand, which replays a stored estimate
func (c *cli) estimate(args []string) error {
	fs := newFlags("estimate")
	replay := fs.String("replay", "", "the estimate id to replay")
	if err := parse(fs, args, false); err != nil {
		return err
	}
	if err := required("replay", *replay); err != nil {
		return err
	}
	op, err := c.operator()
	if err != nil {
		return err
	}
	res, err := op.GetEstimate(c.ctx, connect.NewRequest(&routerv1.GetEstimateRequest{
		EstimateId: *replay,
	}))
	if err != nil {
		return err
	}
	m := res.Msg
	if m.GetEstimatorVersion() != rules.Version {
		return fmt.Errorf("the estimate ran under %s and this build holds %s, replay it from "+
			"the git tag of %s", m.GetEstimatorVersion(), rules.Version, m.GetEstimatorVersion())
	}
	got, err := estimator.Replay(m.GetInputs(), m.GetCatalog(), m.GetOutput())
	if err != nil {
		return err
	}
	floor, rec, err := sameParts(m.GetOutput(), got)
	if err != nil {
		return err
	}
	if err := c.print([][2]string{
		{"estimate_id", m.GetEstimateId()}, {"estimator_version", m.GetEstimatorVersion()},
		{"inputs_sha256", m.GetInputsSha256()}, {"floor", matchWord(floor)},
		{"recommendation", matchWord(rec)},
	}); err != nil {
		return err
	}
	if !floor || !rec {
		return errors.New("the replay differs from the stored answer")
	}
	return nil
}

func matchWord(same bool) string {
	if same {
		return "identical"
	}
	return "differs"
}

// sameParts compares the floor and the recommendation of two stored outputs byte for byte
func sameParts(stored, replayed []byte) (floor, rec bool, err error) {
	var a, b map[string]json.RawMessage
	if err := json.Unmarshal(stored, &a); err != nil {
		return false, false, fmt.Errorf("decode the stored output: %w", err)
	}
	if err := json.Unmarshal(replayed, &b); err != nil {
		return false, false, fmt.Errorf("decode the replayed output: %w", err)
	}
	return bytes.Equal(a["floor"], b["floor"]), bytes.Equal(a["recommendation"],
		b["recommendation"]), nil
}
