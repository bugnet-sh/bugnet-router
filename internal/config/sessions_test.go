package config

import (
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
)

var sessionDefaults = Sessions{
	Keep: 48 * time.Hour, AbuseHold: 720 * time.Hour, MaxEvents: 5000, JobOutput: 64 << 10,
	PlacementOutput: 4 << 20, DiskBudget: 8 << 30, MCPTools: mcpTools,
	Live: SessionsLive{
		Enabled: true, MaxReaders: 256, PlacementBytes: 256 << 20, BytesPerSecond: 1 << 20,
		Replay: 256 << 10,
	},
}

func TestSessionsDefaults(t *testing.T) {
	c, err := LoadFile(writeFile(t, ""))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, c.Sessions, sessionDefaults)
	eq(t, len(c.Sessions.MCPTools), 24)
	for _, tool := range []string{"bugnet_x402_find", "bugnet_x402_budget", "bugnet_exec"} {
		eq(t, slices.Contains(c.Sessions.MCPTools, tool), true)
	}
}

func TestSessionsSettings(t *testing.T) {
	c, err := LoadFile(writeFile(t, `[sessions]
base_url = "https://sessions.example.com/"
keep = "1h"
abuse_hold = "24h"
max_events = 10
job_output = "1KiB"
placement_output = "1MiB"
disk_budget = "1GiB"
mcp_tools = ["bugnet_exec"]

[sessions.live]
enabled = false
max_readers = 1
placement_bytes = "1MiB"
bytes_per_second = "1KiB"
replay = "1"
`))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, c.Sessions, Sessions{
		BaseURL: "https://sessions.example.com", Keep: time.Hour, AbuseHold: 24 * time.Hour,
		MaxEvents: 10, JobOutput: 1 << 10, PlacementOutput: 1 << 20, DiskBudget: 1 << 30,
		MCPTools: []string{"bugnet_exec"},
		Live: SessionsLive{MaxReaders: 1, PlacementBytes: 1 << 20, BytesPerSecond: 1 << 10,
			Replay: 1},
	})
}

func TestSessionsBaseURL(t *testing.T) {
	for raw, want := range map[string]string{
		"https://sessions.example.com":      "https://sessions.example.com",
		"https://sessions.example.com:8448": "https://sessions.example.com:8448",
		"http://127.0.0.1:8448":             "http://127.0.0.1:8448",
		"http://localhost:8448/":            "http://localhost:8448",
	} {
		c, err := LoadFile(writeFile(t, "[sessions]\nbase_url = \""+raw+"\"\n"))
		if err != nil {
			t.Fatalf("%s: %v", raw, err)
		}
		eq(t, c.Sessions.BaseURL, want)
	}
	for _, raw := range []string{
		"sessions.example.com", "http://sessions.example.com", "https://sessions.example.com/s",
		"https://sessions.example.com?a=1", "https://u:p@sessions.example.com", "ftp://x.com",
		"https://", "https://sessions.example.com#k",
	} {
		msg := problem(t, "[sessions]\nbase_url = \""+raw+"\"\n")
		if !strings.HasPrefix(msg, "sessions.base_url must be an origin") {
			t.Errorf("%s gave %q", raw, msg)
		}
	}
}

func TestSessionsBounds(t *testing.T) {
	size := func(key string) string { return "sessions." + key + " must be 1 byte to 1 TiB" }
	cases := []struct{ table, key, value, want string }{
		{"sessions", "keep", `"30m"`, "sessions.keep must be a Go duration of at least 1h"},
		{"sessions", "keep", `"forever"`, "sessions.keep must be a Go duration of at least 1h"},
		{"sessions", "abuse_hold", `"0s"`,
			"sessions.abuse_hold must be a Go duration of at least 1h"},
		{"sessions", "max_events", "0", "sessions.max_events must be 1 to 100000"},
		{"sessions", "max_events", "100001", "sessions.max_events must be 1 to 100000"},
		{"sessions", "job_output", `"0"`, size("job_output")},
		{"sessions", "placement_output", `"4MB"`, size("placement_output")},
		{"sessions", "disk_budget", `"-1GiB"`, size("disk_budget")},
		{"sessions", "job_output", `"8MiB"`,
			"sessions.job_output must be at most sessions.placement_output"},
		{"sessions", "mcp_tools", "[]", "sessions.mcp_tools must list 1 to 64 tool names"},
		{"sessions", "mcp_tools", `["bugnet_exec", "Bugnet_Exec"]`,
			"sessions.mcp_tools entry 2 must be bugnet_ and 1 to 48"},
		{"sessions", "mcp_tools", `["bugnet_exec", "bugnet_exec"]`,
			"sessions.mcp_tools entry 2 repeats entry 1"},
		{"sessions.live", "max_readers", "0", "sessions.live.max_readers must be 1 to 4096"},
		{"sessions.live", "max_readers", "4097", "sessions.live.max_readers must be 1 to 4096"},
		{"sessions.live", "placement_bytes", `"0"`, size("live.placement_bytes")},
		{"sessions.live", "bytes_per_second", `"1TB"`, size("live.bytes_per_second")},
		{"sessions.live", "replay", `"0MiB"`, size("live.replay")},
	}
	for _, c := range cases {
		msg := problem(t, "["+c.table+"]\n"+c.key+" = "+c.value+"\n")
		if !strings.HasPrefix(msg, c.want) {
			t.Errorf("%s.%s = %s gave %q, want %q", c.table, c.key, c.value, msg, c.want)
		}
	}
	msg := problem(t, "[sessions]\nmcp_tools = [\"bugnet_exec\", \"sk-secret-value\"]\n")
	eq(t, strings.Contains(msg, "sk-secret-value"), false)
}

func TestSessionsTokenIsASecret(t *testing.T) {
	msg := problem(t, "[sessions]\ntoken = \"x\"\n")
	eq(t, msg, "line 4: sessions.token is a secret, set BUGNET_SESSIONS_TOKEN in the "+
		"environment instead")
}

func TestExampleTomlHoldsSessionDefaults(t *testing.T) {
	empty, err := LoadFile(writeFile(t, ""))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, loadExample(t).Sessions, empty.Sessions)
	doc, err := outline([]byte(committed(t, "example.toml")))
	if err != nil {
		t.Fatal(err)
	}
	var written, live []string
	for _, s := range doc {
		switch {
		case len(s.path) == 2 && s.path[0] == "sessions" && !s.header:
			written = append(written, s.path[1])
		case len(s.path) == 3 && s.path[1] == "live":
			live = append(live, s.path[2])
		}
	}
	keys := func(schema reflect.Type) []string {
		var out []string
		for i := range schema.NumField() {
			if tag := schema.Field(i).Tag.Get("toml"); tag != "live" {
				out = append(out, tag)
			}
		}
		slices.Sort(out)
		return out
	}
	slices.Sort(written)
	slices.Sort(live)
	eq(t, written, keys(reflect.TypeFor[sessionsFile]()))
	eq(t, live, keys(reflect.TypeFor[sessionsLiveFile]()))
}

func TestDevTomlNamesTheDevSessionsHost(t *testing.T) {
	c, err := LoadFile("../../config/dev.toml")
	if err != nil {
		t.Fatal(err)
	}
	eq(t, c.Sessions.BaseURL, "https://sessions.dev.bugnet.sh")
}
