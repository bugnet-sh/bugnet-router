package leakscan

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net/url"
	"slices"
	"strings"

	"github.com/bugnet-sh/bugnet-router/internal/dpkeys"
)

// minNeedle is the shortest value that can be searched without matching by chance
const minNeedle = 8

// Needle is one secret value the scan searches for, known by a name that is safe to print
type Needle struct {
	Name  string
	Value []byte
}

// Needles is every value the scan searches for
type Needles []Needle

// EnvNames lists the environment variables whose values are needles
var EnvNames = []string{
	"BUGNET_INTERNAL_TOKEN", "BUGNET_HANDLE_KEY", "BUGNET_EVENT_SIGNING_KEY",
	"BUGNET_ORDER_PRIVATE_KEY", "BUGNET_DATA_PLANE_KEYS", "BUGNET_EDGE_TOKEN",
	"BUGNET_SECRETS_TOKEN", "BUGNET_SSH_TOKEN", "BUGNET_EGRESS_TOKEN", "BUGNET_SESSIONS_TOKEN",
	"BUGNET_EGRESS_FETCH_TOKEN", "BUGNET_HOOK_PATH_SECRET", "BUGNET_OPERATOR_TOKEN",
	"BUGNET_PAYMENTS_ROUTER_TOKEN", "BUGNET_GITHUB_TOKEN", "BUGNET_HF_TOKEN",
	"E2B_API_KEY", "BOAT_API_KEY", "BOXD_API_KEY", "KOYEB_API_TOKEN",
}

// webhookSuffix marks the webhook secrets, one for each provider that signs deliveries
const webhookSuffix = "_WEBHOOK_SECRET"

// FromEnv makes the needles of env, a list of NAME=value lines, and never quotes a value
//
// It adds the database URL's password alone, every data plane root, and every purpose key
// derived from every root by the router's own list of purposes.
func FromEnv(env []string) (Needles, error) {
	vars := map[string]string{}
	for _, line := range env {
		if name, v, ok := strings.Cut(line, "="); ok && v != "" {
			vars[name] = v
		}
	}
	var n Needles
	names := slices.Clone(EnvNames)
	for name := range vars {
		if strings.HasSuffix(name, webhookSuffix) {
			names = append(names, name)
		}
	}
	slices.Sort(names)
	for _, name := range slices.Compact(names) {
		v, ok := vars[name]
		if !ok {
			continue
		}
		if len(v) < minNeedle {
			return nil, fmt.Errorf("%s is shorter than %d bytes", name, minNeedle)
		}
		n = append(n, Needle{name, []byte(v)})
	}
	if v, ok := vars["BUGNET_DATABASE_URL"]; ok {
		n = append(n, Needle{"BUGNET_DATABASE_URL", []byte(v)})
		if u, err := url.Parse(v); err == nil && u.User != nil {
			if pw, ok := u.User.Password(); ok && pw != "" {
				if len(pw) < minNeedle {
					return nil, fmt.Errorf("the database password is shorter than %d bytes",
						minNeedle)
				}
				n = append(n, Needle{"BUGNET_DATABASE_URL password", []byte(pw)})
			}
		}
	}
	if v, ok := vars["BUGNET_DATA_PLANE_KEYS"]; ok {
		derived, err := Derived(v)
		if err != nil {
			return nil, err
		}
		n = append(n, derived...)
	}
	return n, nil
}

// Derived makes the needles of every root listed in a BUGNET_DATA_PLANE_KEYS value
func Derived(keys string) (Needles, error) {
	var n Needles
	for i, entry := range strings.Split(keys, ",") {
		id, raw, ok := strings.Cut(entry, ":")
		root, err := base64.StdEncoding.DecodeString(raw)
		if !ok || err != nil {
			return nil, fmt.Errorf("data plane key %d is not an id, a colon and base64", i+1)
		}
		n = append(n, Needle{"root " + id, root})
		for _, p := range dpkeys.Purposes {
			n = append(n, Needle{"derived " + p + " " + id, dpkeys.Derive(root, p)})
		}
	}
	return n, nil
}

// Encoding names, in the order a value that two encodings share is credited to the first
var encodings = []string{"raw", "base64", "base64url", "hex", "url"}

type pattern struct {
	rule string
	b    []byte
	// skip is the one path where the value belongs, empty for none
	skip string
}

// patterns lists every byte string that shows v in one encoding, each tagged with its rule
func patterns(kind, name string, v []byte, skip string) []pattern {
	var out []pattern
	seen := map[string]bool{}
	add := func(enc string, forms ...[]byte) {
		for _, b := range forms {
			if len(b) >= minNeedle && !seen[string(b)] {
				seen[string(b)] = true
				out = append(out, pattern{kind + " " + name + " " + enc, b, skip})
			}
		}
	}
	add("raw", v)
	add("base64", aligned(base64.RawStdEncoding, v)...)
	add("base64url", aligned(base64.RawURLEncoding, v)...)
	h := hex.EncodeToString(v)
	add("hex", []byte(h), []byte(strings.ToUpper(h)))
	add("url", []byte(url.QueryEscape(string(v))), []byte(url.PathEscape(string(v))))
	return out
}

// aligned encodes v as it appears after 0, 1 and 2 bytes of other data, keeping the full chars
func aligned(enc *base64.Encoding, v []byte) [][]byte {
	var out [][]byte
	for k := range 3 {
		s := enc.EncodeToString(append(make([]byte, k), v...))
		skip := (8*k + 5) / 6
		full := (8 * (k + len(v))) / 6
		if full > skip {
			out = append(out, []byte(s[skip:full]))
		}
	}
	return out
}

// hasValue reports whether n already holds v
func (n Needles) hasValue(v []byte) bool {
	return slices.ContainsFunc(n, func(x Needle) bool { return bytes.Equal(x.Value, v) })
}
