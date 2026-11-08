package store_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/bugnet-sh/bugnet-router/internal/store"
)

func TestSSHKeysCapAndIdempotence(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	p := newPlacement(t, s, "o-keys")
	for _, fp := range []string{"SHA256:a", "SHA256:b"} {
		if err := s.AddSSHKey(ctx, p.ID, fp, []byte(fp), 2); err != nil {
			t.Fatalf("add %s: %v", fp, err)
		}
	}
	if err := s.AddSSHKey(ctx, p.ID, "SHA256:a", []byte("SHA256:a"), 2); err != nil {
		t.Fatalf("the same key at the cap: %v", err)
	}
	if err := s.AddSSHKey(ctx, p.ID, "SHA256:c", nil, 2); !errors.Is(err,
		store.ErrTooManyKeys) {
		t.Fatalf("a third key of two: %v", err)
	}
	removed, err := s.RemoveSSHKey(ctx, p.ID, "SHA256:a")
	if err != nil || !removed {
		t.Fatalf("remove: %v, %v", removed, err)
	}
	if removed, err := s.RemoveSSHKey(ctx, p.ID, "SHA256:a"); err != nil || removed {
		t.Fatalf("a second remove: %v, %v", removed, err)
	}
	live, err := s.LiveSSHKeys(ctx, []store.SSHKeyRef{
		{Placement: p.ID, Fingerprint: "SHA256:a"}, {Placement: p.ID, Fingerprint: "SHA256:b"},
		{Placement: uuid.New(), Fingerprint: "SHA256:b"},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := map[store.SSHKeyRef]bool{{Placement: p.ID, Fingerprint: "SHA256:b"}: true}
	if len(live) != 1 || !live[store.SSHKeyRef{Placement: p.ID, Fingerprint: "SHA256:b"}] {
		t.Fatalf("live keys %v, want %v", live, want)
	}
	if err := s.AddSSHKey(ctx, p.ID, "SHA256:c", []byte("c"), 2); err != nil {
		t.Fatalf("a key after a removal freed its place: %v", err)
	}
	if err := s.AddSSHKey(ctx, p.ID, "SHA256:a", []byte("a"), 3); err != nil {
		t.Fatalf("a removed key added again: %v", err)
	}
	k, err := s.LiveSSHKey(ctx, p.ID, "SHA256:a")
	if err != nil || string(k.PublicKey) != "a" || k.RemovedAt != nil {
		t.Fatalf("the key added again: %+v, %v", k, err)
	}
}

func TestRacingSSHKeysKeepTheCap(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	p := newPlacement(t, s, "o-keyrace")
	var wg sync.WaitGroup
	errs := make(chan error, 10)
	for i := range 10 {
		wg.Go(func() {
			fp := "SHA256:" + string(rune('a'+i))
			errs <- s.AddSSHKey(ctx, p.ID, fp, []byte(fp), 4)
		})
	}
	wg.Wait()
	close(errs)
	added := 0
	for err := range errs {
		switch {
		case err == nil:
			added++
		case !errors.Is(err, store.ErrTooManyKeys):
			t.Fatal(err)
		}
	}
	if added != 4 {
		t.Fatalf("%d keys added under a cap of 4", added)
	}
}

func TestSSHAliasIsUniqueAndSetOnce(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	a := newPlacement(t, s, "o-alias-a")
	b := newPlacement(t, s, "o-alias-b")
	if ok, err := s.SetSSHAlias(ctx, a.ID, "bn-aaaaaaaaaa"); err != nil || !ok {
		t.Fatalf("first alias: %v, %v", ok, err)
	}
	if ok, err := s.SetSSHAlias(ctx, b.ID, "bn-aaaaaaaaaa"); err != nil || ok {
		t.Fatalf("a taken alias: %v, %v", ok, err)
	}
	if ok, err := s.SetSSHAlias(ctx, a.ID, "bn-bbbbbbbbbb"); err != nil || ok {
		t.Fatalf("a second alias: %v, %v", ok, err)
	}
	got, err := s.PlacementBySSHAlias(ctx, "bn-aaaaaaaaaa")
	if err != nil || got.ID != a.ID || got.SSHAlias != "bn-aaaaaaaaaa" {
		t.Fatalf("by alias: %+v, %v", got, err)
	}
	if _, err := s.PlacementBySSHAlias(ctx, "bn-cccccccccc"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("an unknown alias: %v", err)
	}
}

func TestSSHHostsAndGatewayKeys(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	p := newPlacement(t, s, "o-hosts")
	if _, err := s.SSHHost(ctx, p.ID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("no host yet: %v", err)
	}
	h := store.SSHHost{
		PlacementID: p.ID, Transport: store.SSHTransportGate, SSHDPort: 22, SSHUser: "root",
		HostKey: "ssh-ed25519 AAAA",
	}
	if err := s.PutSSHHost(ctx, h); err != nil {
		t.Fatal(err)
	}
	h.SSHDPort = 2022
	if err := s.PutSSHHost(ctx, h); err != nil {
		t.Fatal(err)
	}
	got, err := s.SSHHost(ctx, p.ID)
	if err != nil || got.SSHDPort != 2022 || got.SSHUser != "root" || got.PreparedAt.IsZero() {
		t.Fatalf("host: %+v, %v", got, err)
	}
	for range 2 {
		if err := s.AddGatewayKey(ctx, p.ID, "SHA256:gw"); err != nil {
			t.Fatal(err)
		}
	}
	if ok, err := s.HasGatewayKey(ctx, p.ID, "SHA256:gw"); err != nil || !ok {
		t.Fatalf("gateway key: %v, %v", ok, err)
	}
	if err := s.DeleteGatewayKeys(ctx, p.ID); err != nil {
		t.Fatal(err)
	}
	if ok, err := s.HasGatewayKey(ctx, p.ID, "SHA256:gw"); err != nil || ok {
		t.Fatalf("gateway key after the delete: %v, %v", ok, err)
	}
}
