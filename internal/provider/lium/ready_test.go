package lium

import (
	"context"
	"maps"
	"strconv"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

// rentedPod creates a pod on the fake through Create, which rents node-a
func rentedPod(t *testing.T, f *fakeLium, p *Provider) (provider.Ref, fakePod) {
	t.Helper()
	ref := mustCreate(t, p, createReq(freshKey()))
	pod, ok := f.pod(ref.ID)
	if !ok {
		t.Fatal("no pod")
	}
	return ref, pod
}

func TestLiumReadyMeta(t *testing.T) {
	f, p := liveFake(t)
	var _ provider.ReadyReporter = p
	ref, pod := rentedPod(t, f, p)
	// The pod runs after a few reads, with no SSH port known for the first of them
	go func() {
		time.Sleep(20 * time.Millisecond)
		f.setPod(pod.id, func(fp *fakePod) { fp.status, fp.sshPort = "RUNNING", 0 })
		time.Sleep(20 * time.Millisecond)
		f.setPod(pod.id, func(fp *fakePod) { fp.sshPort = 40022 })
	}()
	before := time.Now().Truncate(time.Second)
	meta, err := p.ReadyMeta(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	readyAt, err := time.Parse(time.RFC3339, meta["ready_at"])
	if err != nil || readyAt.Before(before) || readyAt.After(time.Now()) {
		t.Fatalf("ready_at %q", meta["ready_at"])
	}
	delete(meta, "ready_at")
	// The host key is the one the pod's sshd showed on the first connection
	eq(t, meta[provider.MetaHostKey], f.sshd(pod.id).hostPublicKey())
	delete(meta, provider.MetaHostKey)
	eq(t, meta, map[string]string{
		"ssh_host": "203.0.113.7", "ssh_port": "40022", "node": "node-a",
		"hourly_micros": "250000",
	})
	if n := len(f.requestsTo("GET", "/pods/"+pod.id)); n < 3 {
		t.Fatalf("%d reads of the pod", n)
	}
	// It sets no removal, the deadline being fixed only after it returns
	eq(t, len(f.requestsTo("POST", "/pods/"+pod.id+"/schedule-removal")), 0)
	if err := p.WaitReady(context.Background(), ref); err != nil {
		t.Fatal(err)
	}

	// The host key hook pins what it reads at the address it is given
	var dialed string
	p.hostKey = func(_ context.Context, r provider.Ref, addr string) (string, error) {
		eq(t, r.ID, ref.ID)
		dialed = addr
		return "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIPodHostKey", nil
	}
	meta, err = p.ReadyMeta(context.Background(), ref)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, dialed, "203.0.113.7:40022")
	eq(t, meta[provider.MetaHostKey], "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIPodHostKey")
}

func TestLiumReadyMetaEndsOnDeadPods(t *testing.T) {
	for status, code := range map[string]provider.Code{
		"FAILED": provider.CodeCapacity, "CREATION_FAILED": provider.CodeCapacity,
		"BROKEN": provider.CodeCapacity, "REBOOT_FAILED": provider.CodeCapacity,
		"STOPPED": provider.CodeUnfunded, "STOP_PENDING": provider.CodeUnfunded,
	} {
		t.Run(status, func(t *testing.T) {
			f, p := liveFake(t)
			ref, pod := rentedPod(t, f, p)
			f.setPod(pod.id, func(fp *fakePod) { fp.status = status })
			_, err := p.ReadyMeta(context.Background(), ref)
			wantCode(t, err, code, true)
		})
	}
	t.Run("deleting", func(t *testing.T) {
		f, p := liveFake(t)
		ref, pod := rentedPod(t, f, p)
		f.setPod(pod.id, func(fp *fakePod) { fp.status = "DELETING" })
		_, err := p.ReadyMeta(context.Background(), ref)
		wantCode(t, err, provider.CodeNotFound, false)
	})
	t.Run("destroyed", func(t *testing.T) {
		f, p := liveFake(t)
		ref, _ := rentedPod(t, f, p)
		if err := p.Destroy(context.Background(), ref); err != nil {
			t.Fatal(err)
		}
		wantCode(t, p.WaitReady(context.Background(), ref), provider.CodeNotFound, false)
	})
	t.Run("an unknown status", func(t *testing.T) {
		f, p := liveFake(t)
		ref, pod := rentedPod(t, f, p)
		f.setPod(pod.id, func(fp *fakePod) { fp.status = "HIBERNATED" })
		_, err := p.ReadyMeta(context.Background(), ref)
		wantCode(t, err, provider.CodeInternal, false)
	})
	t.Run("never running", func(t *testing.T) {
		f, p := liveFake(t)
		ref, _ := rentedPod(t, f, p)
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		if _, err := p.ReadyMeta(ctx, ref); err == nil {
			t.Fatal("a pod that never runs became ready")
		}
	})
	t.Run("a ref of another provider", func(t *testing.T) {
		_, p := liveFake(t)
		err := p.WaitReady(context.Background(), provider.Ref{Provider: "hf", ID: "x"})
		wantCode(t, err, provider.CodeNotFound, false)
	})
}

func TestLiumNodeMoveAtDeploy(t *testing.T) {
	type move struct {
		change func(*fakePod)
		opts   func(*Options)
		// pass says the node is one the size may run on
		pass bool
	}
	elsewhere := func(change func(*fakePod)) func(*fakePod) {
		return func(fp *fakePod) {
			fp.node, fp.machine, fp.tier, fp.country = "node-moved", "NVIDIA GeForce RTX 3090",
				"secure", "FR"
			fp.nodeGPUs, fp.gpus, fp.price = 1, 1, 0.30
			if change != nil {
				change(fp)
			}
		}
	}
	noOthers := func(o *Options) { o.OtherCountries = false }
	for name, m := range map[string]move{
		"EU, Secure and whole": {change: elsewhere(nil), pass: true},
		"a variant machine name the listing classed": {change: elsewhere(func(fp *fakePod) {
			fp.machine = "NVIDIA GeForce RTX 3090 Founders"
		}), pass: true},
		"outside the EU, other countries allowed": {change: elsewhere(func(fp *fakePod) {
			fp.country = "US"
		}), pass: true},
		"outside the EU, other countries refused": {change: elsewhere(func(fp *fakePod) {
			fp.country = "US"
		}), opts: noOthers},
		"a blocked country": {change: elsewhere(func(fp *fakePod) { fp.country = "RU" })},
		"no country":        {change: elsewhere(func(fp *fakePod) { fp.country = "" })},
		"Spot":              {change: elsewhere(func(fp *fakePod) { fp.tier = "spot" })},
		"split with another renter": {change: elsewhere(func(fp *fakePod) {
			fp.nodeGPUs = 2
		})},
		"a node of two GPUs held whole": {change: elsewhere(func(fp *fakePod) {
			fp.nodeGPUs, fp.gpus = 2, 2
		})},
		"another GPU type": {change: elsewhere(func(fp *fakePod) {
			fp.machine = "NVIDIA GeForce RTX 4090"
		})},
		"a machine name never listed": {change: elsewhere(func(fp *fakePod) {
			fp.machine = "NVIDIA GeForce RTX 3090 Unknown"
		})},
		"over the cap": {change: elsewhere(func(fp *fakePod) { fp.price = 0.36 })},
	} {
		t.Run(name, func(t *testing.T) {
			opts := []func(*Options){}
			if m.opts != nil {
				opts = append(opts, m.opts)
			}
			f, p := liveFake(t, opts...)
			// The listing once showed a variant machine name under the 3090 class
			v := euNode("node-variant")
			v.machine, v.free = "NVIDIA GeForce RTX 3090 Founders", 0
			f.mu.Lock()
			f.nodes = append(f.nodes, v)
			f.mu.Unlock()
			relist(t, p)
			ref, pod := rentedPod(t, f, p)
			f.setPod(pod.id, func(fp *fakePod) { fp.status = "RUNNING" })
			f.setPod(pod.id, m.change)
			meta, err := p.ReadyMeta(context.Background(), ref)
			if m.pass {
				if err != nil {
					t.Fatal(err)
				}
				eq(t, meta["node"], "node-moved")
				moved, _ := f.pod(pod.id)
				eq(t, meta["hourly_micros"], strconv.FormatInt(micros(moved.price), 10))
				eq(t, len(f.requestsTo("DELETE", "/pods/"+pod.id)), 0)
				return
			}
			wantCode(t, err, provider.CodeCapacity, true)
			deletes := f.requestsTo("DELETE", "/pods/"+pod.id)
			if len(deletes) != 1 || deletes[0].query.Get("expected_name") != pod.name {
				t.Fatalf("deletes %+v", deletes)
			}
		})
	}
	t.Run("a ref of no size sold", func(t *testing.T) {
		f, p := liveFake(t)
		ref, pod := rentedPod(t, f, p)
		ref.Meta["size"] = "lium-t4"
		f.setPod(pod.id, func(fp *fakePod) { fp.status = "RUNNING" })
		f.setPod(pod.id, elsewhere(nil))
		_, err := p.ReadyMeta(context.Background(), ref)
		wantCode(t, err, provider.CodeCapacity, true)
	})
}

func TestLiumNetguard(t *testing.T) {
	for _, addr := range []string{
		"10.0.0.5", "127.0.0.1", "169.254.10.1", "192.168.1.9", "100.64.0.1", "::1", "fd00::1",
		"fe80::1", "::ffff:10.0.0.5", "0.0.0.0", "", "pod.example.com",
	} {
		t.Run(addr, func(t *testing.T) {
			f, p := liveFake(t)
			ref, pod := rentedPod(t, f, p)
			f.setPod(pod.id, func(fp *fakePod) { fp.status, fp.ip = "RUNNING", addr })
			_, err := p.ReadyMeta(context.Background(), ref)
			wantCode(t, err, provider.CodeCapacity, true)
			if _, ok := f.pod(pod.id); ok {
				t.Fatal("a pod at a refused address was kept")
			}
		})
	}
	t.Run("a public address", func(t *testing.T) {
		f, p := liveFake(t)
		ref, pod := rentedPod(t, f, p)
		f.setPod(pod.id, func(fp *fakePod) { fp.status, fp.ip = "RUNNING", "2001:db9::7" })
		meta, err := p.ReadyMeta(context.Background(), ref)
		if err != nil {
			t.Fatal(err)
		}
		eq(t, meta["ssh_host"], "2001:db9::7")
	})
	for _, addr := range []string{
		"10.0.0.5", "127.0.0.1", "169.254.10.1", "192.168.1.9", "::1", "fd00::1", "fe80::1",
		"::ffff:10.0.0.5",
	} {
		t.Run("AuthorizeNativeSSH at "+addr, func(t *testing.T) {
			f, p := liveFake(t)
			ref, pod := readyRef(t, f, p)
			// Lium now reports the pod at a refused address
			f.setPod(pod.id, func(fp *fakePod) { fp.ip = addr })
			_, err := p.AuthorizeNativeSSH(context.Background(), ref, gatewayKey(t))
			wantCode(t, err, provider.CodeInternal, false)
			// The ref keeps a refused address while Lium reports a public one
			f.setPod(pod.id, func(fp *fakePod) { fp.ip = "203.0.113.7" })
			bad := provider.Ref{Provider: ref.Provider, ID: ref.ID, Meta: maps.Clone(ref.Meta)}
			bad.Meta["ssh_host"] = addr
			_, err = p.AuthorizeNativeSSH(context.Background(), bad, gatewayKey(t))
			wantCode(t, err, provider.CodeInternal, false)
			wantNoKeys(t, f)
		})
	}
}
