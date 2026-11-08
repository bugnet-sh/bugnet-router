package main

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strconv"
	"strings"

	"connectrpc.com/connect"
	routerv1 "github.com/bugnet-sh/bugnet-proto/gen/go/bugnet/router/v1"
	"golang.org/x/crypto/nacl/box"
)

// stdin is where secret reads its value, a variable so tests can stand in for it
var stdin io.Reader = os.Stdin

// maxSecretValue keeps the sealed box under the router's 70 KiB
const maxSecretValue = 64 << 10

// listFlag is a repeatable flag that keeps each value in order
type listFlag []string

func (l *listFlag) String() string { return strings.Join(*l, ",") }

func (l *listFlag) Set(v string) error {
	*l = append(*l, v)
	return nil
}

// sealedPayload is DP 10.2's bound record, the only plaintext the proxy opens
type sealedPayload struct {
	V         int      `json:"v"`
	Binding   string   `json:"binding"`
	Name      string   `json:"name"`
	Hosts     []string `json:"hosts"`
	Headers   []string `json:"headers"`
	FetchOnly bool     `json:"fetchOnly"`
	Value     string   `json:"value"`
}

// secret seals a value read from stdin to the router's newest key and puts it, as the bridge does
func (c *cli) secret(args []string) error {
	fs := newFlags("secret")
	h := fs.String("handle", "", "sandbox handle")
	name := fs.String("name", "", "variable name in the sandbox")
	var hosts, headers listFlag
	fs.Var(&hosts, "host", "host the secret may reach, repeatable")
	fs.Var(&headers, "header", "header name besides Authorization, repeatable")
	fetchOnly := fs.Bool("fetch-only", false, "only the router's repository fetch uses it")
	if err := parse(fs, args, false); err != nil {
		return err
	}
	for _, f := range []struct{ name, value string }{{"handle", *h}, {"name", *name}} {
		if err := required(f.name, f.value); err != nil {
			return err
		}
	}
	if len(hosts) == 0 {
		return fmt.Errorf("%w: at least one --host is required", errUsage)
	}
	value, err := readValue()
	if err != nil {
		return err
	}
	api, err := c.client()
	if err != nil {
		return err
	}
	keys, err := api.GetSecretSealingKey(c.ctx, connect.NewRequest(
		&routerv1.GetSecretSealingKeyRequest{Handle: *h}))
	if err != nil {
		return err
	}
	// The newest key comes first, and this dev tool trusts the router's list
	list := keys.Msg.GetSealingKeys()
	if len(list) == 0 || len(list[0].GetPublicKey()) != 32 {
		return errors.New("the router lists no sealing key")
	}
	spec := &routerv1.SecretSpec{
		Name: *name, Hosts: hosts, HeaderNames: headers, FetchOnly: *fetchOnly,
	}
	sealed, err := seal(list[0], spec, value)
	if err != nil {
		return err
	}
	res, err := api.PutSecret(c.ctx, connect.NewRequest(&routerv1.PutSecretRequest{
		Handle: *h, Spec: spec, IdempotencyKey: rand.Text(),
		Value: &routerv1.SealedValue{KeyId: list[0].GetKeyId(), Box: sealed},
	}))
	if err != nil {
		return err
	}
	info := res.Msg.GetSecret()
	kv := [][2]string{
		{"name", info.GetSpec().GetName()},
		{"state", strings.ToLower(strings.TrimPrefix(info.GetState().String(),
			"SECRET_STATE_"))},
		{"updated_at", stamp(info.GetUpdatedAt())},
	}
	if info.GetFailureReason() != "" {
		kv = append(kv, [2]string{"failure_reason", info.GetFailureReason()})
	}
	return c.print(append(kv, [2]string{"replaced", strconv.FormatBool(res.Msg.GetReplaced())}))
}

// readValue reads the value from stdin, without the one line ending a terminal adds
func readValue() (string, error) {
	raw, err := io.ReadAll(io.LimitReader(stdin, maxSecretValue+2))
	if err != nil {
		return "", fmt.Errorf("read the value from stdin: %w", err)
	}
	value := strings.TrimSuffix(strings.TrimSuffix(string(raw), "\n"), "\r")
	switch {
	case value == "":
		return "", fmt.Errorf("%w: the value is read from stdin, which was empty", errUsage)
	case len(value) > maxSecretValue:
		return "", fmt.Errorf("%w: the value is over 64 KiB", errUsage)
	}
	return value, nil
}

// seal binds the value to the placement and spec and seals it to key with crypto_box_seal
func seal(key *routerv1.SealingKey, spec *routerv1.SecretSpec, value string) ([]byte, error) {
	canonical := func(xs []string) []string {
		out := []string{}
		for _, x := range xs {
			out = append(out, strings.TrimSuffix(strings.ToLower(x), "."))
		}
		slices.Sort(out)
		return slices.Compact(out)
	}
	plain, err := json.Marshal(sealedPayload{
		V: 1, Binding: key.GetBinding(), Name: spec.GetName(),
		Hosts: canonical(spec.GetHosts()), Headers: canonical(spec.GetHeaderNames()),
		FetchOnly: spec.GetFetchOnly(), Value: value,
	})
	if err != nil {
		return nil, err
	}
	var pub [32]byte
	copy(pub[:], key.GetPublicKey())
	return box.SealAnonymous(nil, plain, &pub, rand.Reader)
}
