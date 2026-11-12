package main

import (
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strconv"

	"connectrpc.com/connect"
	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"

	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/providers"
)

// transfer mints a transfer URL and prints it with the curl command that uses it
func (c *cli) transfer(args []string) error {
	fs := newFlags("transfer")
	h := fs.String("handle", "", "sandbox handle")
	up := fs.String("upload", "", "local file to send into the sandbox")
	down := fs.String("download", "", "sandbox path to fetch")
	to := fs.String("path", "", "sandbox path an upload lands at, ~/<file name> when empty")
	sum := fs.String("sha256", "", "an upload's expected SHA-256 in hex")
	if err := parse(fs, args, false); err != nil {
		return err
	}
	if err := required("handle", *h); err != nil {
		return err
	}
	req := &routerv1.CreateTransferRequest{Handle: *h}
	var curl string
	switch {
	case (*up == "") == (*down == ""):
		return fmt.Errorf("%w: give one of --upload FILE or --download PATH", errUsage)
	case *down != "":
		if *to != "" || *sum != "" {
			return fmt.Errorf("%w: --path and --sha256 go with --upload", errUsage)
		}
		req.Direction, req.Path = routerv1.TransferDirection_TRANSFER_DIRECTION_DOWNLOAD, *down
		curl = "curl -fSo " + shellWord(filepath.Base(*down)) + " "
	default:
		fi, err := os.Stat(*up)
		if err != nil {
			return err
		}
		if !fi.Mode().IsRegular() {
			return fmt.Errorf("%s is not a regular file", *up)
		}
		size := uint64(fi.Size())
		req.Direction, req.SizeBytes = routerv1.TransferDirection_TRANSFER_DIRECTION_UPLOAD, &size
		req.Path = *to
		if req.Path == "" {
			req.Path = "~/" + filepath.Base(*up)
		}
		if *sum != "" {
			b, err := hex.DecodeString(*sum)
			if err != nil || len(b) != 32 {
				return fmt.Errorf("%w: --sha256 must be 64 hex characters", errUsage)
			}
			req.Sha256 = b
		}
		curl = "curl -fsS -T " + shellWord(*up) + " "
	}
	api, err := c.client()
	if err != nil {
		return err
	}
	res, err := api.CreateTransfer(c.ctx, connect.NewRequest(req))
	if err != nil {
		return err
	}
	m := res.Msg
	kv := [][2]string{
		{"url", m.GetUrl()}, {"expires_at", stamp(m.GetExpiresAt())},
		{"size", strconv.FormatUint(m.GetSize(), 10)},
		{"max_bytes", strconv.FormatUint(m.GetMaxBytes(), 10)},
	}
	if m.GetModifiedAt() != nil {
		kv = append(kv, [2]string{"modified_at", stamp(m.GetModifiedAt())})
	}
	return c.print(append(kv, [2]string{"curl", curl + shellWord(m.GetUrl())}))
}

// bugnetLabels are the label keys Bugnet's router and live tests put on sandboxes
var bugnetLabels = []string{"bugnet", "bugnet-live", "bugnet-contract"}

// owned prints how many sandboxes the provider account holds under Bugnet's labels, and their ids
func (c *cli) owned(args []string) error {
	fs := newFlags("owned")
	if err := parse(fs, args, true); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("%w: owned takes one provider name", errUsage)
	}
	name := fs.Arg(0)
	cfg, err := c.config()
	if err != nil {
		return err
	}
	cfg.Providers = []string{name}
	cfg.ProviderKeys = map[string]string{}
	if env, ok := config.ProviderKeyEnv(name); ok {
		if cfg.ProviderKeys[name] = c.getenv(env); cfg.ProviderKeys[name] == "" {
			return fmt.Errorf("%s is not set", env)
		}
	}
	if cfg.ProviderSeeds, err = c.providerSeeds(name); err != nil {
		return err
	}
	reg, err := providers.Build(cfg, nil, nil)
	if err != nil {
		return err
	}
	prov, ok := reg.Get(name)
	if !ok {
		return errors.New("the provider was not built")
	}
	// Lium and HF read in the background until closed
	if closer, ok := prov.(io.Closer); ok {
		defer func() { _ = closer.Close() }()
	}
	all, err := prov.ListOwned(c.ctx, map[string]string{})
	if err != nil {
		return err
	}
	var ids []string
	for _, s := range all {
		for k := range s.Labels {
			if slices.Contains(bugnetLabels, k) {
				ids = append(ids, s.Ref.ID)
				break
			}
		}
	}
	slices.Sort(ids)
	kv := [][2]string{{"provider", name}, {"count", strconv.Itoa(len(ids))}}
	for _, id := range ids {
		kv = append(kv, [2]string{"id", id})
	}
	return c.print(kv)
}
