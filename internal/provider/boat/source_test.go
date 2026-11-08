package boat

import (
	"context"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

type sourceCase struct {
	ip       any
	ssh      any
	bits     int
	families []string
}

// readSource makes a sandbox that reports c and reads its egress source
func readSource(t *testing.T, c sourceCase, state string) (provider.EgressSource, bool, error) {
	t.Helper()
	f := newFake(t)
	p, err := New(Options{
		APIKey: fakeKey, BaseURL: f.srv.URL + "/api/v1", EgressIPv6Bits: c.bits,
		EgressFamilies: c.families,
	})
	if err != nil {
		t.Fatal(err)
	}
	size, _ := p.SizeFor(provider.Resources{})
	ref, err := p.Create(context.Background(), provider.CreateRequest{
		IdempotencyKey: "k", Size: size, Lifetime: time.Hour, Labels: map[string]string{"bugnet": "1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	f.setNetwork(ref.ID, c.ip, c.ssh)
	if state != "" {
		f.setState(ref.ID, state)
	}
	return p.EgressSource(context.Background(), ref)
}

var allFamilies = []string{FamilyIPv6, FamilyIPv4}

func TestBoatEgressSource(t *testing.T) {
	src, ok, err := readSource(t, sourceCase{ip: "2001:db8::5", bits: 128, families: allFamilies}, "")
	eq(t, err == nil && ok, true)
	eq(t, src.Prefix.String(), "2001:db8::5/128")
	eq(t, src.Bind.String(), "2001:db8::5")
	src, _, _ = readSource(t, sourceCase{ip: "2001:db8::5", bits: 64, families: allFamilies}, "")
	eq(t, src.Prefix.String(), "2001:db8::/64")
	eq(t, src.Bind.String(), "2001:db8::5")
	src, ok, _ = readSource(t, sourceCase{
		ip: "::ffff:203.0.113.10", bits: 128, families: allFamilies}, "")
	eq(t, ok && src.Prefix.String() == "203.0.113.10/32", true)
	eq(t, src.Bind.String(), "203.0.113.10")
	_, ok, _ = readSource(t, sourceCase{
		ip: "203.0.113.10", ssh: "ssh.boat.dev:2201", bits: 128, families: allFamilies}, "")
	eq(t, ok, false)
}

func TestBoatEgressSourceRefusals(t *testing.T) {
	for _, ip := range []any{nil, "", "not-an-ip", "fe80::1%eth0", "10.0.0.5", "100.64.0.1",
		"fd00::1", "127.0.0.1", "::1", 7} {
		_, ok, err := readSource(t, sourceCase{ip: ip, bits: 128, families: allFamilies}, "")
		eq(t, err, nil)
		eq(t, ok, false)
	}
}

func TestBoatEgressSourceFamilies(t *testing.T) {
	_, ok, _ := readSource(t, sourceCase{ip: "2001:db8::5", families: []string{FamilyIPv4}}, "")
	eq(t, ok, false)
	_, ok, _ = readSource(t, sourceCase{ip: "203.0.113.10", families: []string{FamilyIPv6}}, "")
	eq(t, ok, false)
	_, ok, _ = readSource(t, sourceCase{ip: "203.0.113.10", families: []string{FamilyIPv4}}, "")
	eq(t, ok, true)
	_, ok, _ = readSource(t, sourceCase{ip: "2001:db8::5"}, "")
	eq(t, ok, false)
}

func TestBoatEgressSourceStates(t *testing.T) {
	for _, state := range []string{"archived", "cancelled", "archiving", "error"} {
		_, ok, err := readSource(t, sourceCase{ip: "2001:db8::5", families: allFamilies}, state)
		if state != "cancelled" && err != nil {
			t.Fatalf("%s: %v", state, err)
		}
		eq(t, ok, false)
	}
}

func TestBoatEgressSourceNotFound(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, fakeKey)
	_, ok, err := p.EgressSource(context.Background(), provider.Ref{Provider: "boat", ID: "sbx0000000"})
	e, isErr := provider.AsError(err)
	eq(t, ok, false)
	eq(t, isErr && e.Code == provider.CodeNotFound, true)
}

func TestBoatEgressOptionsRefused(t *testing.T) {
	for _, o := range []Options{
		{EgressIPv6Bits: 63}, {EgressIPv6Bits: 129}, {EgressFamilies: []string{"ipx"}},
	} {
		o.APIKey = fakeKey
		_, err := New(o)
		eq(t, err != nil, true)
	}
}
