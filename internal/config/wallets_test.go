package config

import (
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
)

const facilitatorAddr = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"

var walletDefaults = Wallets{
	MaxConcurrent: 5, StartsPerHour: 30, StartsBurst: 5, StartsPerDay: 200, RefusalsPerHour: 10,
	AnonymousMaxConcurrent: 20, OpsPerSecond: 20, OpsBurst: 40, RefusalBlockFor: time.Hour,
	StrikeThreshold: 2, StrikeWindow: 24 * time.Hour, StrikeBlockFor: 168 * time.Hour,
}

func TestWalletsDefaults(t *testing.T) {
	c, err := LoadFile(writeFile(t, ""))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, c.Wallets, walletDefaults)
	eq(t, c.ProviderOps, map[string]int{"boat": 50})
	eq(t, Wallets{}.OrDefaults(), walletDefaults)
	eq(t, Wallets{UsePayer: true, MaxConcurrent: 1}.OrDefaults().MaxConcurrent, 1)
}

func TestWalletsSettings(t *testing.T) {
	c, err := LoadFile(writeFile(t, `[wallets]
use_payer = true
require_payer = true
max_concurrent = 1
starts_per_hour = 2
starts_burst = 3
starts_per_day = 4
anonymous_max_concurrent = 5
ops_per_second = 6
ops_burst = 7
refusals_per_hour = 8
refusal_block_for = "2h"
facilitator_addresses = ["`+facilitatorAddr+`"]
emit_events = true
strike_threshold = 3
strike_window = "1h"
strike_block_for = "1m"

[e2b]
ops_per_second = 7

[boat]
ops_per_second = 0
`))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, c.Wallets, Wallets{
		UsePayer: true, RequirePayer: true, MaxConcurrent: 1, StartsPerHour: 2, StartsBurst: 3,
		StartsPerDay: 4, AnonymousMaxConcurrent: 5, OpsPerSecond: 6, OpsBurst: 7,
		RefusalsPerHour: 8, RefusalBlockFor: 2 * time.Hour,
		FacilitatorAddresses: []string{facilitatorAddr}, EmitEvents: true, StrikeThreshold: 3,
		StrikeWindow: time.Hour, StrikeBlockFor: time.Minute,
	})
	eq(t, c.ProviderOps, map[string]int{"e2b": 7})
}

func TestWalletsBounds(t *testing.T) {
	cases := []struct{ toml, want string }{
		{"[wallets]\nmax_concurrent = 0", "wallets.max_concurrent must be 1 to 1000000"},
		{"[wallets]\nstarts_burst = -1", "wallets.starts_burst must be 1 to 1000000"},
		{"[wallets]\nops_per_second = 1000001", "wallets.ops_per_second must be 1 to 1000000"},
		{"[wallets]\nrefusal_block_for = \"0s\"",
			"wallets.refusal_block_for must be a positive Go duration such as 1h"},
		{"[wallets]\nstrike_window = \"1 day\"",
			"wallets.strike_window must be a positive Go duration such as 1h"},
		{"[wallets]\nfacilitator_addresses = [\"" + facilitatorAddr + "\", \"0x12\"]",
			"wallets.facilitator_addresses entry 2 must be a canonical base58 Solana address"},
		{"[wallets]\nfacilitator_addresses = [\"" + facilitatorAddr + "\", \"" +
			facilitatorAddr + "\"]", "wallets.facilitator_addresses entry 2 repeats entry 1"},
		{"[boat]\nops_per_second = -1", "boat.ops_per_second must be 0 to 1000000"},
		{"[wallets]\nuse_payer = 1", "line 4: wallets.use_payer must be true or false"},
		{"[wallets]\nbytes_per_day = \"50GiB\"", "line 4: wallets.bytes_per_day is not a setting"},
		{"[wallets]\nrequire_payer = true",
			"wallets.require_payer = true needs wallets.use_payer = true"},
	}
	for _, c := range cases {
		msg := problem(t, c.toml+"\n")
		if !strings.HasPrefix(msg, c.want) {
			t.Errorf("%s gave %q, want %q", c.toml, msg, c.want)
		}
		if strings.Contains(msg, "0x12") {
			t.Errorf("%s: the error quotes the entry: %s", c.toml, msg)
		}
	}
}

func TestExampleTomlHoldsWalletsDefaults(t *testing.T) {
	example := loadExample(t)
	eq(t, example.Wallets, walletDefaults)
	eq(t, example.ProviderOps, map[string]int{"boat": 50})
	doc, err := outline([]byte(committed(t, "example.toml")))
	if err != nil {
		t.Fatal(err)
	}
	var written []string
	ops := map[string]bool{}
	for _, s := range doc {
		if len(s.path) == 2 && s.path[0] == "wallets" {
			written = append(written, s.path[1])
		}
		if len(s.path) == 2 && s.path[1] == "ops_per_second" && s.path[0] != "wallets" {
			ops[s.path[0]] = true
		}
	}
	var keys []string
	schema := reflect.TypeFor[walletsFile]()
	for i := range schema.NumField() {
		keys = append(keys, schema.Field(i).Tag.Get("toml"))
	}
	slices.Sort(written)
	slices.Sort(keys)
	eq(t, written, keys)
	eq(t, ops, map[string]bool{"e2b": true, "boxd": true, "koyeb": true, "boat": true})
}

func TestDevTomlTurnsThePayerOn(t *testing.T) {
	c := loadDev(t)
	eq(t, c.Wallets.UsePayer, true)
	eq(t, c.Wallets.EmitEvents, true)
	eq(t, len(c.Wallets.FacilitatorAddresses), 1)
	eq(t, c.Wallets.AnonymousMaxConcurrent > 20, true)
}

func loadDev(t *testing.T) Config {
	t.Helper()
	line := `order_token_public_key = "` + pubB64(t) + `"`
	c, err := LoadFile(writeFile(t, line+"\n\n"+committed(t, "dev.toml")))
	if err != nil {
		t.Fatal(err)
	}
	return c
}
