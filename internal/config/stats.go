package config

import (
	"cmp"
	"errors"
	"fmt"
	"net/netip"
	"net/url"
	"time"
)

// Stats holds the public stats feed's settings
type Stats struct {
	// ListenAddr is router-api's loopback listener for the feed, which is off when empty
	ListenAddr string
	// PaymentsURL is Payments' internal API, which serves its totals to the snapshot job
	PaymentsURL     string
	PaymentsTimeout time.Duration
	// RowCache is how long router-api serves the snapshot row before reading it again
	RowCache time.Duration
	// SessionsCache is how many session pages one router-api keeps
	SessionsCache int
	// MaxQueries is how many session page queries one router-api runs at once
	MaxQueries         int
	PerSourcePerMinute int
	PerSourceBurst     int
	// MaxSources is how many per-source buckets one router-api keeps
	MaxSources int
}

// Enabled reports whether router-api serves the feed and router-worker builds its snapshot
func (s Stats) Enabled() bool { return s.ListenAddr != "" }

type statsFile struct {
	ListenAddr         string `toml:"listen_addr"`
	PaymentsURL        string `toml:"payments_url"`
	PaymentsTimeout    string `toml:"payments_timeout"`
	RowCache           string `toml:"row_cache"`
	SessionsCache      int64  `toml:"sessions_cache"`
	MaxQueries         int64  `toml:"max_queries"`
	PerSourcePerMinute int64  `toml:"per_source_per_minute"`
	PerSourceBurst     int64  `toml:"per_source_burst"`
	MaxSources         int64  `toml:"max_sources"`
}

func defaultStats() statsFile {
	return statsFile{
		PaymentsURL: "http://127.0.0.1:8447", PaymentsTimeout: "5s", RowCache: "5s",
		SessionsCache: 256, MaxQueries: 4, PerSourcePerMinute: 60, PerSourceBurst: 20,
		MaxSources: 10000,
	}
}

// read checks the stats table against its bounds, d holding the defaults
func (f statsFile) read(d statsFile) (Stats, error) {
	s := Stats{ListenAddr: f.ListenAddr, PaymentsURL: cmp.Or(f.PaymentsURL, d.PaymentsURL)}
	if s.ListenAddr != "" && !loopbackPort(s.ListenAddr) {
		// The value is never quoted, since a secret could have been pasted there
		return Stats{}, errors.New("stats.listen_addr must be a loopback address and port " +
			"such as 127.0.0.1:8082, or empty")
	}
	if s.ListenAddr != "" && !loopbackHTTP(s.PaymentsURL) {
		return Stats{}, errors.New("stats.payments_url must be http:// to a loopback address " +
			"and port, such as http://127.0.0.1:8447")
	}
	for _, p := range []struct {
		key, raw, def string
		most          time.Duration
		into          *time.Duration
	}{
		{"payments_timeout", f.PaymentsTimeout, d.PaymentsTimeout, 15 * time.Second,
			&s.PaymentsTimeout},
		{"row_cache", f.RowCache, d.RowCache, time.Minute, &s.RowCache},
	} {
		v, err := time.ParseDuration(cmp.Or(p.raw, p.def))
		if err != nil || v < time.Millisecond || v > p.most {
			return Stats{}, fmt.Errorf("stats.%s must be a Go duration from 1ms to %s", p.key,
				shortDuration(p.most))
		}
		*p.into = v
	}
	for _, n := range []struct {
		key  string
		raw  int64
		most int64
		into *int
	}{
		{"sessions_cache", f.SessionsCache, 100_000, &s.SessionsCache},
		{"max_queries", f.MaxQueries, 64, &s.MaxQueries},
		{"per_source_per_minute", f.PerSourcePerMinute, maxWalletLimit, &s.PerSourcePerMinute},
		{"per_source_burst", f.PerSourceBurst, maxWalletLimit, &s.PerSourceBurst},
		{"max_sources", f.MaxSources, maxWalletLimit, &s.MaxSources},
	} {
		if n.raw < 1 || n.raw > n.most {
			return Stats{}, fmt.Errorf("stats.%s must be 1 to %d", n.key, n.most)
		}
		*n.into = int(n.raw)
	}
	return s, nil
}

// loopbackPort reports whether addr is a loopback IP address and a port
func loopbackPort(addr string) bool {
	ap, err := netip.ParseAddrPort(addr)
	return err == nil && ap.Addr().IsLoopback()
}

// loopbackHTTP reports whether raw is a plain http:// origin on a loopback address and port
func loopbackHTTP(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "http" || u.User != nil || u.RawQuery != "" ||
		u.Fragment != "" || u.Opaque != "" || (u.Path != "" && u.Path != "/") {
		return false
	}
	return loopbackPort(u.Host)
}

// shortDuration writes d as the error messages name it, 15s rather than 15s0ms
func shortDuration(d time.Duration) string {
	if d%time.Minute == 0 {
		return fmt.Sprintf("%dm", d/time.Minute)
	}
	return d.String()
}
