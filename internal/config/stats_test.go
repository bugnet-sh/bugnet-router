package config

import (
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
)

var statsDefaults = Stats{
	PaymentsURL: "http://127.0.0.1:8447", PaymentsTimeout: 5 * time.Second,
	RowCache: 5 * time.Second, SessionsCache: 256, MaxQueries: 4, PerSourcePerMinute: 60,
	PerSourceBurst: 20, MaxSources: 10000,
}

func TestStatsDefaults(t *testing.T) {
	c, err := LoadFile(writeFile(t, ""))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, c.Stats, statsDefaults)
	eq(t, c.Stats.Enabled(), false)
}

func TestStatsSettings(t *testing.T) {
	c, err := LoadFile(writeFile(t, `[stats]
listen_addr = "127.0.0.1:8082"
payments_url = "http://[::1]:9447"
payments_timeout = "2s"
row_cache = "1s"
sessions_cache = 10
max_queries = 2
per_source_per_minute = 30
per_source_burst = 5
max_sources = 100
`))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, c.Stats, Stats{
		ListenAddr: "127.0.0.1:8082", PaymentsURL: "http://[::1]:9447",
		PaymentsTimeout: 2 * time.Second, RowCache: time.Second, SessionsCache: 10, MaxQueries: 2,
		PerSourcePerMinute: 30, PerSourceBurst: 5, MaxSources: 100,
	})
	eq(t, c.Stats.Enabled(), true)
}

func TestStatsSettingsStrict(t *testing.T) {
	listen := "stats.listen_addr must be a loopback address and port such as 127.0.0.1:8082, " +
		"or empty"
	payments := "stats.payments_url must be http:// to a loopback address and port, such as " +
		"http://127.0.0.1:8447"
	on := "[stats]\nlisten_addr = \"127.0.0.1:8082\"\n"
	cases := []struct{ toml, want string }{
		{"[stats]\nlisten_addr = \"0.0.0.0:8082\"", listen},
		{"[stats]\nlisten_addr = \"203.0.113.9:8082\"", listen},
		{"[stats]\nlisten_addr = \"localhost:8082\"", listen},
		{"[stats]\nlisten_addr = \":8082\"", listen},
		{"[stats]\nlisten_addr = \"127.0.0.1\"", listen},
		{on + "payments_url = \"https://127.0.0.1:8447\"", payments},
		{on + "payments_url = \"http://203.0.113.9:8447\"", payments},
		{on + "payments_url = \"http://localhost:8447\"", payments},
		{on + "payments_url = \"http://127.0.0.1\"", payments},
		{on + "payments_url = \"http://u:p@127.0.0.1:8447\"", payments},
		{on + "payments_url = \"http://127.0.0.1:8447/x\"", payments},
		{on + "payments_url = \"http://127.0.0.1:8447?a=1\"", payments},
		{"[stats]\npayments_timeout = \"0s\"",
			"stats.payments_timeout must be a Go duration from 1ms to 15s"},
		{"[stats]\npayments_timeout = \"20s\"",
			"stats.payments_timeout must be a Go duration from 1ms to 15s"},
		{"[stats]\nrow_cache = \"-1s\"", "stats.row_cache must be a Go duration from 1ms to 1m"},
		{"[stats]\nsessions_cache = 0", "stats.sessions_cache must be 1 to 100000"},
		{"[stats]\nmax_queries = 65", "stats.max_queries must be 1 to 64"},
		{"[stats]\nper_source_per_minute = 0", "stats.per_source_per_minute must be 1 to 1000000"},
		{"[stats]\nper_source_burst = -1", "stats.per_source_burst must be 1 to 1000000"},
		{"[stats]\nmax_sources = 1000001", "stats.max_sources must be 1 to 1000000"},
		{"[stats]\nlisten = \"127.0.0.1:8082\"", "line 4: stats.listen is not a setting"},
	}
	for _, c := range cases {
		msg := problem(t, c.toml+"\n")
		if !strings.HasPrefix(msg, c.want) {
			t.Errorf("%s gave %q, want %q", c.toml, msg, c.want)
		}
		for _, quoted := range []string{"203.0.113.9", "u:p@"} {
			if strings.Contains(msg, quoted) {
				t.Errorf("%s: the error quotes the value: %s", c.toml, msg)
			}
		}
	}
	// A payments_url nothing reads is not checked while the feed is off
	if msg := problem(t, "[stats]\npayments_url = \"http://203.0.113.9:8447\"\n"); msg != "" {
		t.Errorf("an unused payments_url refused: %s", msg)
	}
	for _, ok := range []string{"127.0.0.1:0", "127.0.0.2:8082", "[::1]:8082"} {
		if msg := problem(t, "[stats]\nlisten_addr = \""+ok+"\"\n"); msg != "" {
			t.Errorf("%s refused: %s", ok, msg)
		}
	}
}

func TestExampleTomlHoldsStatsDefaults(t *testing.T) {
	eq(t, loadExample(t).Stats, statsDefaults)
	doc, err := outline([]byte(committed(t, "example.toml")))
	if err != nil {
		t.Fatal(err)
	}
	var written, keys []string
	for _, s := range doc {
		if len(s.path) == 2 && s.path[0] == "stats" {
			written = append(written, s.path[1])
		}
	}
	schema := reflect.TypeFor[statsFile]()
	for i := range schema.NumField() {
		keys = append(keys, schema.Field(i).Tag.Get("toml"))
	}
	slices.Sort(written)
	slices.Sort(keys)
	eq(t, written, keys)
}

func TestDevTomlKeepsTheFeedOff(t *testing.T) {
	c := loadDev(t)
	eq(t, c.Stats.ListenAddr, "")
	if !strings.Contains(committed(t, "dev.toml"), "[stats]\nlisten_addr = \"\"") {
		t.Error(`config/dev.toml lacks [stats] listen_addr = ""`)
	}
}
