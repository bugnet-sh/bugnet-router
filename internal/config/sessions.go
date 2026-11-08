package config

import (
	"cmp"
	"fmt"
	"net"
	"net/url"
	"regexp"
	"slices"
	"time"
)

// Sessions holds the session page's settings
type Sessions struct {
	// BaseURL is the page's origin, and no placement carries a link while it is empty
	BaseURL string
	// Keep is how long a record's page lives after its placement ends
	Keep time.Duration
	// AbuseHold is how long an abuse or operator end keeps the record for operators
	AbuseHold time.Duration
	// MaxEvents bounds one record's events, placement, payment and allowance rows aside
	MaxEvents int
	// JobOutput is the newest output the copy keeps of each output of a job
	JobOutput int64
	// PlacementOutput is the output one record keeps, earlier jobs keeping 8 KiB past it
	PlacementOutput int64
	// DiskBudget is what the three session tables may take on disk together
	DiskBudget int64
	// MCPTools are the gateway's tool names an mcp event may carry
	MCPTools []string
	Live     SessionsLive
}

// SessionsLive holds the live view's settings
type SessionsLive struct {
	// Enabled false refuses WatchSessionJob and ends every live stream
	Enabled bool
	// MaxReaders bounds the live streams one router-api process serves at once
	MaxReaders int
	// PlacementBytes bounds what the live view sends of one placement's output
	PlacementBytes int64
	// BytesPerSecond paces each live stream
	BytesPerSecond int64
	// Replay is how much of a job's newest output a first connection sees
	Replay int64
}

type sessionsFile struct {
	BaseURL         string           `toml:"base_url"`
	Keep            string           `toml:"keep"`
	AbuseHold       string           `toml:"abuse_hold"`
	MaxEvents       int64            `toml:"max_events"`
	JobOutput       string           `toml:"job_output"`
	PlacementOutput string           `toml:"placement_output"`
	DiskBudget      string           `toml:"disk_budget"`
	MCPTools        []string         `toml:"mcp_tools"`
	Live            sessionsLiveFile `toml:"live"`
}

type sessionsLiveFile struct {
	Enabled        bool   `toml:"enabled"`
	MaxReaders     int64  `toml:"max_readers"`
	PlacementBytes string `toml:"placement_bytes"`
	BytesPerSecond string `toml:"bytes_per_second"`
	Replay         string `toml:"replay"`
}

const (
	// maxSessionEvents keeps one record far below what its page can show
	maxSessionEvents = 100_000
	// maxLiveReaders matches the live client's blocking pool
	maxLiveReaders = 4096
	maxMCPTools    = 64
)

// mcpTools are the gateway's tool names, the free ones included, as bugnet-gateway registers them
var mcpTools = []string{
	"bugnet_assess", "bugnet_quote", "bugnet_redeem", "bugnet_start", "bugnet_status",
	"bugnet_extend", "bugnet_stop", "bugnet_exec", "bugnet_job", "bugnet_cancel_job",
	"bugnet_read_file", "bugnet_write_file", "bugnet_list_files", "bugnet_preview",
	"bugnet_transfer", "bugnet_ssh", "bugnet_ssh_remove", "bugnet_secret_link",
	"bugnet_secret_put_grant", "bugnet_secrets", "bugnet_secret_delete", "bugnet_fetch_source",
	"bugnet_x402_find", "bugnet_x402_budget",
}

var toolName = regexp.MustCompile(`^bugnet_[a-z0-9_]{1,48}$`)

func defaultSessions() sessionsFile {
	return sessionsFile{
		Keep: "48h", AbuseHold: "720h", MaxEvents: 5000, JobOutput: "64KiB",
		PlacementOutput: "4MiB", DiskBudget: "8GiB",
		// A clone, since go-toml decodes a list into the slice it finds
		MCPTools: slices.Clone(mcpTools),
		Live: sessionsLiveFile{
			Enabled: true, MaxReaders: 256, PlacementBytes: "256MiB", BytesPerSecond: "1MiB",
			Replay: "256KiB",
		},
	}
}

// read checks the sessions table against its bounds, d holding the defaults
func (f sessionsFile) read(d sessionsFile) (Sessions, error) {
	base, err := sessionsBase(f.BaseURL)
	if err != nil {
		return Sessions{}, err
	}
	s := Sessions{BaseURL: base, Live: SessionsLive{Enabled: f.Live.Enabled}}
	for _, t := range []struct {
		key, raw, def string
		into          *time.Duration
	}{
		{"sessions.keep", f.Keep, d.Keep, &s.Keep},
		{"sessions.abuse_hold", f.AbuseHold, d.AbuseHold, &s.AbuseHold},
	} {
		v, err := time.ParseDuration(cmp.Or(t.raw, t.def))
		if err != nil || v < time.Hour {
			return Sessions{}, fmt.Errorf("%s must be a Go duration of at least 1h such as 48h",
				t.key)
		}
		*t.into = v
	}
	if f.MaxEvents < 1 || f.MaxEvents > maxSessionEvents {
		return Sessions{}, fmt.Errorf("sessions.max_events must be 1 to %d", maxSessionEvents)
	}
	s.MaxEvents = int(f.MaxEvents)
	if f.Live.MaxReaders < 1 || f.Live.MaxReaders > maxLiveReaders {
		return Sessions{}, fmt.Errorf("sessions.live.max_readers must be 1 to %d",
			maxLiveReaders)
	}
	s.Live.MaxReaders = int(f.Live.MaxReaders)
	for _, b := range []struct {
		key, raw, def string
		into          *int64
	}{
		{"sessions.job_output", f.JobOutput, d.JobOutput, &s.JobOutput},
		{"sessions.placement_output", f.PlacementOutput, d.PlacementOutput, &s.PlacementOutput},
		{"sessions.disk_budget", f.DiskBudget, d.DiskBudget, &s.DiskBudget},
		{"sessions.live.placement_bytes", f.Live.PlacementBytes, d.Live.PlacementBytes,
			&s.Live.PlacementBytes},
		{"sessions.live.bytes_per_second", f.Live.BytesPerSecond, d.Live.BytesPerSecond,
			&s.Live.BytesPerSecond},
		{"sessions.live.replay", f.Live.Replay, d.Live.Replay, &s.Live.Replay},
	} {
		n, err := byteSize(b.key, cmp.Or(b.raw, b.def))
		if err != nil {
			return Sessions{}, err
		}
		*b.into = n
	}
	if s.JobOutput > s.PlacementOutput {
		return Sessions{}, fmt.Errorf("sessions.job_output must be at most " +
			"sessions.placement_output")
	}
	if s.MCPTools, err = toolList(f.MCPTools); err != nil {
		return Sessions{}, err
	}
	return s, nil
}

// sessionsBase checks the page's origin, https with a host and nothing after it
func sessionsBase(raw string) (string, error) {
	if raw == "" {
		return "", nil
	}
	const want = "sessions.base_url must be an origin such as https://sessions.example.com, " +
		"with no path, query or user, or http to a loopback host"
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || u.User != nil || (u.Path != "" && u.Path != "/") ||
		u.RawQuery != "" || u.Fragment != "" || u.Opaque != "" {
		return "", fmt.Errorf("%s", want)
	}
	host := u.Hostname()
	switch {
	case u.Scheme == "https":
	case u.Scheme == "http" && loopback(host):
	default:
		return "", fmt.Errorf("%s", want)
	}
	return u.Scheme + "://" + u.Host, nil
}

func loopback(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// toolList checks the mcp tool names, each once
func toolList(names []string) ([]string, error) {
	if len(names) == 0 || len(names) > maxMCPTools {
		return nil, fmt.Errorf("sessions.mcp_tools must list 1 to %d tool names", maxMCPTools)
	}
	for i, name := range names {
		if !toolName.MatchString(name) {
			// An entry is never quoted, since a secret could have been pasted there
			return nil, fmt.Errorf("sessions.mcp_tools entry %d must be bugnet_ and 1 to 48 "+
				"lowercase letters, digits or underscores", i+1)
		}
		if first := slices.Index(names[:i], name); first >= 0 {
			return nil, fmt.Errorf("sessions.mcp_tools entry %d repeats entry %d", i+1, first+1)
		}
	}
	return slices.Clone(names), nil
}
