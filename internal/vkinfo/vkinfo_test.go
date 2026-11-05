package vkinfo_test

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/valkey-io/valkey-go"

	"github.com/oynozan/bugnet-router/internal/testenv"
	"github.com/oynozan/bugnet-router/internal/vkinfo"
)

const sample = "# Memory\r\n" +
	"used_memory:2317312\r\n" +
	"used_memory_human:2.21M\r\n" +
	"used_memory_peak:2400000\r\n" +
	"maxmemory:4294967296\r\n" +
	"maxmemory_human:4.00G\r\n" +
	"maxmemory_policy:noeviction\r\n" +
	"mem_allocator:jemalloc-5.3.0\r\n"

func TestParseReadsUsedMaxAndPolicy(t *testing.T) {
	m, err := vkinfo.Parse(sample)
	want := vkinfo.Memory{Used: 2317312, Max: 4294967296, Policy: "noeviction"}
	if err != nil || m != want {
		t.Fatalf("Parse = %+v, %v, want %+v", m, err, want)
	}
}

// without returns sample less the line of field
func without(field string) string {
	var kept []string
	for line := range strings.Lines(sample) {
		if !strings.HasPrefix(line, field+":") {
			kept = append(kept, line)
		}
	}
	return strings.Join(kept, "")
}

func TestParseNamesAMissingFieldAndKeepsTheRest(t *testing.T) {
	for field, want := range map[string]vkinfo.Memory{
		"used_memory":      {Max: 4294967296, Policy: "noeviction"},
		"maxmemory":        {Used: 2317312, Policy: "noeviction"},
		"maxmemory_policy": {Used: 2317312, Max: 4294967296},
	} {
		m, err := vkinfo.Parse(without(field))
		if err == nil || !strings.Contains(err.Error(), field) || m != want {
			t.Errorf("Parse without %s = %+v, %v, want %+v and an error naming it",
				field, m, err, want)
		}
	}
}

func TestParseRefusesANonNumber(t *testing.T) {
	text := strings.Replace(sample, "maxmemory:4294967296", "maxmemory:lots", 1)
	m, err := vkinfo.Parse(text)
	want := vkinfo.Memory{Used: 2317312, Policy: "noeviction"}
	if err == nil || !strings.Contains(err.Error(), "maxmemory") || m != want {
		t.Fatalf("Parse with a non-number maxmemory = %+v, %v, want %+v and an error naming it",
			m, err, want)
	}
}

func TestReadAsksValkey(t *testing.T) {
	vk, _ := testenv.Valkey(t)
	m, err := vkinfo.Read(t.Context(), vk)
	if err != nil || m.Used <= 0 || m.Max < 0 || m.Policy == "" {
		t.Fatalf("Read = %+v, %v, want the local Valkey's memory and policy", m, err)
	}
}

func fixed(m vkinfo.Memory, err error) vkinfo.Reader {
	return func(context.Context, valkey.Client) (vkinfo.Memory, error) { return m, err }
}

// parsed is a reader that always answers what Parse makes of info
func parsed(info string) vkinfo.Reader {
	return func(context.Context, valkey.Client) (vkinfo.Memory, error) { return vkinfo.Parse(info) }
}

func TestCheckStart(t *testing.T) {
	for _, c := range []struct {
		name    string
		read    vkinfo.Reader
		refusal string
		warning string
	}{
		{"noeviction with maxmemory", fixed(vkinfo.Memory{Used: 1, Max: 2, Policy: "noeviction"},
			nil), "", ""},
		{"evicting", fixed(vkinfo.Memory{Max: 2, Policy: "allkeys-lru"}, nil),
			"valkey: maxmemory-policy is allkeys-lru, the router needs noeviction", ""},
		{"no maxmemory", fixed(vkinfo.Memory{Used: 1, Policy: "noeviction"}, nil), "",
			"valkey has no maxmemory, so /monitor cannot alert on memory"},
		{"unreadable", fixed(vkinfo.Memory{}, errors.New("no maxmemory_policy")), "",
			"no maxmemory_policy"},
		{"evicting without maxmemory", parsed(strings.Replace(without("maxmemory"),
			"noeviction", "allkeys-lru", 1)),
			"valkey: maxmemory-policy is allkeys-lru, the router needs noeviction", ""},
		{"noeviction without used_memory", parsed(without("used_memory")), "", "no used_memory"},
	} {
		t.Run(c.name, func(t *testing.T) {
			var logs bytes.Buffer
			log := slog.New(slog.NewTextHandler(&logs, nil))
			var refusal string
			if err := vkinfo.CheckStart(t.Context(), log, nil, c.read); err != nil {
				refusal = err.Error()
			}
			if refusal != c.refusal {
				t.Fatalf("CheckStart refused with %q, want %q", refusal, c.refusal)
			}
			warned := strings.Contains(logs.String(), "level=WARN")
			if warned != (c.warning != "") || !strings.Contains(logs.String(), c.warning) {
				t.Fatalf("CheckStart logged %q, want a warning with %q", logs.String(), c.warning)
			}
		})
	}
}
