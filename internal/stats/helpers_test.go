package stats

import (
	"bytes"
	"encoding/json"
	"flag"
	"math/big"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/catalog"
	"github.com/bugnet-sh/bugnet-router/internal/config"
)

var update = flag.Bool("update", false, "rewrite the golden files")

// eq fails the test when got and want differ
func eq[T any](t *testing.T, got, want T) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

const alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"

// base58 encodes b as Solana writes keys and signatures
func base58(b []byte) string {
	n := new(big.Int).SetBytes(b)
	var out []byte
	radix, mod := big.NewInt(58), new(big.Int)
	for n.Sign() > 0 {
		n.DivMod(n, radix, mod)
		out = append([]byte{alphabet[mod.Int64()]}, out...)
	}
	for _, c := range b {
		if c != 0 {
			break
		}
		out = append([]byte{'1'}, out...)
	}
	return string(out)
}

// key is n bytes, each byte seed, so every test value is fixed
func key(n int, seed byte) string { return base58(bytes.Repeat([]byte{seed}, n)) }

// cfg loads a settings file of these top-level lines
func cfg(t *testing.T, lines ...string) *config.Config {
	t.Helper()
	path := filepath.Join(t.TempDir(), "settings.toml")
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	c, err := config.LoadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return &c
}

const catalogJSON = `{"packages": [
	{"id": "cpu-small-10m", "resources": {"vcpu": 2, "memory_mib": 2048, "disk_gib": 10},
		"minutes": 10, "price_micros": 50000},
	{"id": "cpu-small-10m-more", "resources": {"vcpu": 2, "memory_mib": 2048, "disk_gib": 10},
		"minutes": 10, "price_micros": 45000, "extension_of": "cpu-small-10m"},
	{"id": "gpu-l4-30m", "resources": {"vcpu": 8, "memory_mib": 32768, "disk_gib": 100,
		"gpu_count": 1, "gpu_memory_gib": 24, "gpu_model": "NVIDIA L4"},
		"minutes": 30, "price_micros": 1500000}
]}`

func testCatalog(t *testing.T) *catalog.Catalog {
	t.Helper()
	path := filepath.Join(t.TempDir(), "packages.json")
	if err := os.WriteFile(path, []byte(catalogJSON), 0o600); err != nil {
		t.Fatal(err)
	}
	c, err := catalog.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// golden compares v, encoded as served and then indented, with the file, or rewrites it
func golden(t *testing.T, v any, file string) {
	t.Helper()
	body, err := Encode(v)
	if err != nil {
		t.Fatal(err)
	}
	var pretty bytes.Buffer
	if err := json.Indent(&pretty, body, "", "    "); err != nil {
		t.Fatal(err)
	}
	pretty.WriteByte('\n')
	if *update {
		if err := os.WriteFile(file, pretty.Bytes(), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	if pretty.String() != string(want) {
		t.Fatalf("%s differs, got\n%s", file, pretty.String())
	}
}

func ptr[T any](v T) *T { return &v }

func date(d, h, m, s int) time.Time { return time.Date(2026, 11, d, h, m, s, 0, time.UTC) }
