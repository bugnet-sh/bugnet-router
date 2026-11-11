package config

import (
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
)

var limitsPollDefaults = LimitsPoll{
	Every: time.Minute, BoatEvery: 15 * time.Second, AccountEvery: 5 * time.Minute,
	AccountEveryBlocked: time.Minute, ReserveSlots: 1, StaleAfter: 5,
}

func TestLimitsPollDefaults(t *testing.T) {
	c, err := LoadFile(writeFile(t, ""))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, c.LimitsPoll, limitsPollDefaults)
	eq(t, DefaultLimitsPoll(), limitsPollDefaults)
	// E2B's cap and create rate are settings, since no key can read them
	eq(t, c.MaxConcurrent, map[string]int{"e2b": 20, "lium": 10, "hf": 10})
	eq(t, c.CreatesPerSecond, map[string]int{"e2b": 1, "lium": 1, "hf": 1})
	eq(t, c.E2B.TeamID, "")
	eq(t, c.Koyeb.OrganizationID, "")
}

func TestLimitsPollSettings(t *testing.T) {
	c, err := LoadFile(writeFile(t, `[limits_poll]
every = "2m"
boat_every = "20s"
account_every = "10m"
account_every_blocked = "30s"
reserve_slots = 0
stale_after = 3

[e2b]
team_id = "team-1"
max_concurrent = 100
creates_per_second = 5

[boxd]
max_concurrent = 12
creates_per_second = 2

[koyeb]
organization_id = "org-1"
max_concurrent = 7

[boat]
max_concurrent = 9
creates_per_second = 3
`))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, c.LimitsPoll, LimitsPoll{
		Every: 2 * time.Minute, BoatEvery: 20 * time.Second, AccountEvery: 10 * time.Minute,
		AccountEveryBlocked: 30 * time.Second, ReserveSlots: 0, StaleAfter: 3,
	})
	eq(t, c.MaxConcurrent, map[string]int{
		"e2b": 100, "boxd": 12, "koyeb": 7, "boat": 9, "lium": 10, "hf": 10,
	})
	eq(t, c.CreatesPerSecond, map[string]int{"e2b": 5, "boxd": 2, "boat": 3, "lium": 1, "hf": 1})
	eq(t, c.E2B.TeamID, "team-1")
	eq(t, c.Koyeb.OrganizationID, "org-1")
}

func TestLimitsPollRefusals(t *testing.T) {
	for _, tc := range []struct{ toml, want string }{
		{"[limits_poll]\nevery = \"soon\"",
			"limits_poll.every must be a Go duration from 1s to 24h such as 60s"},
		{"[limits_poll]\nboat_every = \"0s\"", "limits_poll.boat_every must be a Go duration"},
		{"[limits_poll]\naccount_every = \"48h\"", "limits_poll.account_every must be"},
		{"[limits_poll]\naccount_every_blocked = \"-1m\"",
			"limits_poll.account_every_blocked must be"},
		{"[limits_poll]\nreserve_slots = -1", "limits_poll.reserve_slots must be 0 to 1000"},
		{"[limits_poll]\nstale_after = 0", "limits_poll.stale_after must be 1 to 1000"},
		{"[limits_poll]\nsurprise = 1", "surprise"},
		{"[e2b]\nmax_concurrent = -1", "e2b.max_concurrent must be 0 to"},
		{"[koyeb]\ncreates_per_second = 2000000", "koyeb.creates_per_second must be 0 to"},
		{"[boat]\nmax_concurrent = -5", "boat.max_concurrent must be 0 to"},
	} {
		msg := problem(t, tc.toml+"\n")
		if !strings.Contains(msg, tc.want) {
			t.Errorf("%s gave %q, want %q", tc.toml, msg, tc.want)
		}
	}
}

func TestExampleTomlHoldsLimitsPollDefaults(t *testing.T) {
	example := loadExample(t)
	eq(t, example.LimitsPoll, limitsPollDefaults)
	eq(t, example.MaxConcurrent, map[string]int{"e2b": 20, "lium": 10, "hf": 10})
	eq(t, example.CreatesPerSecond, map[string]int{"e2b": 1, "lium": 1, "hf": 1})
	doc, err := outline([]byte(committed(t, "example.toml")))
	if err != nil {
		t.Fatal(err)
	}
	var written, keys []string
	for _, s := range doc {
		if len(s.path) == 2 && s.path[0] == "limits_poll" {
			written = append(written, s.path[1])
		}
	}
	schema := reflect.TypeFor[limitsPollFile]()
	for i := range schema.NumField() {
		keys = append(keys, schema.Field(i).Tag.Get("toml"))
	}
	slices.Sort(written)
	slices.Sort(keys)
	eq(t, written, keys)
}
