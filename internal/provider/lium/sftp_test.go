package lium

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

// hostileWindow is what an SSH server may send past what the adapter read, x/crypto's window
const hostileWindow = 2 << 20

// hostilePod is a ready pod whose sftp-server answers as hostileFS does
func hostilePod(t *testing.T) (*fakeLium, *Provider, provider.Ref, *fakeSSHD) {
	t.Helper()
	f, p := liveFake(t)
	ref, pod := readyRef(t, f, p)
	sshd := f.sshd(pod.id)
	sshd.setHostile(t)
	return f, p, ref, sshd
}

// underDeadline runs call under a deadline of d and fails unless it ends within a second of it
func underDeadline(t *testing.T, d time.Duration, call func(ctx context.Context) error) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	start := time.Now()
	err := call(ctx)
	if took := time.Since(start); took > d+time.Second {
		t.Fatalf("the call took %v under a deadline of %v", took, d)
	}
	return err
}

func TestLiumSFTPHostile(t *testing.T) {
	t.Run("EndlessDirectory", func(t *testing.T) {
		_, p, ref, sshd := hostilePod(t)
		into := provider.NewListing(1000)
		err := underDeadline(t, 30*time.Second, func(ctx context.Context) error {
			return p.ListFiles(ctx, ref, hostileEndless, 1, into)
		})
		if err != nil || len(into.Entries) != 1000 || !into.Truncated {
			t.Fatalf("gave %d entries truncated %v, %v", len(into.Entries), into.Truncated, err)
		}
		if sent := sshd.sftpOut.Load(); sent > listCap(1000)+hostileWindow {
			t.Fatalf("the pod sent %d bytes past a cap of %d", sent, listCap(1000))
		}
	})
	t.Run("EndlessDots", func(t *testing.T) {
		// Dot entries never fill the listing, so only the byte cap ends it
		_, p, ref, sshd := hostilePod(t)
		into := provider.NewListing(10)
		start := time.Now()
		err := underDeadline(t, 30*time.Second, func(ctx context.Context) error {
			return p.ListFiles(ctx, ref, hostileDots, 3, into)
		})
		if err != nil || len(into.Entries) != 0 || !into.Truncated {
			t.Fatalf("gave %d entries truncated %v, %v", len(into.Entries), into.Truncated, err)
		}
		if took := time.Since(start); took > 5*time.Second {
			t.Fatalf("the cap took %v to end the listing", took)
		}
		if sent := sshd.sftpOut.Load(); sent > listCap(10)+hostileWindow {
			t.Fatalf("the pod sent %d bytes past a cap of %d", sent, listCap(10))
		}
	})
	t.Run("NamePastNameMax", func(t *testing.T) {
		_, p, ref, _ := hostilePod(t)
		err := p.ListFiles(context.Background(), ref, hostileLongName, 1,
			provider.NewListing(10))
		wantCode(t, err, provider.CodeInternal, false)
	})
	t.Run("HugeSize", func(t *testing.T) {
		_, p, ref, _ := hostilePod(t)
		ctx := context.Background()
		for _, name := range []string{hostileHuge, hostileWrapped} {
			_, err := p.StatFile(ctx, ref, name)
			wantCode(t, err, provider.CodeInternal, false)
			_, _, err = p.ReadFile(ctx, ref, name, 0, 1<<20)
			wantCode(t, err, provider.CodeInternal, false)
			_, err = p.OpenReader(ctx, ref, name, 0)
			wantCode(t, err, provider.CodeInternal, false)
		}
	})
	t.Run("StalledCalls", func(t *testing.T) {
		_, p, ref, _ := hostilePod(t)
		const d = 300 * time.Millisecond
		err := underDeadline(t, d, func(ctx context.Context) error {
			_, _, err := p.ReadFile(ctx, ref, hostileStall, 0, 10)
			return err
		})
		wantCode(t, err, provider.CodeInternal, false)
		err = underDeadline(t, d, func(ctx context.Context) error {
			return p.ListFiles(ctx, ref, hostileStallDir, 1, provider.NewListing(10))
		})
		wantCode(t, err, provider.CodeInternal, false)
		err = underDeadline(t, d, func(ctx context.Context) error {
			r, err := p.OpenReader(ctx, ref, hostileStall, 0)
			if err != nil {
				t.Fatalf("OpenReader of a file whose reads stall: %v", err)
			}
			defer func() { _ = r.Close() }()
			_, err = io.ReadAll(r)
			return err
		})
		if err == nil {
			t.Fatal("a stalled read ended with no error")
		}
	})
	t.Run("StalledReadWithNoDeadline", func(t *testing.T) {
		// The operation limit bounds each read of a caller that set no deadline
		_, p, ref, _ := hostilePod(t)
		p.ssh.op = 300 * time.Millisecond
		r, err := p.OpenReader(context.Background(), ref, hostileStall, 0)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = r.Close() }()
		start := time.Now()
		_, err = r.Read(make([]byte, 10))
		wantCode(t, err, provider.CodeInternal, false)
		if took := time.Since(start); took > p.ssh.op+time.Second {
			t.Fatalf("the read took %v under a limit of %v", took, p.ssh.op)
		}
	})
	t.Run("NoSpace", func(t *testing.T) {
		// ENOSPC in the status's words, and OpenSSH's bare failure on a full disk
		_, p, ref, _ := hostilePod(t)
		ctx := context.Background()
		for _, dir := range []string{hostileNoSpace, hostileFull} {
			err := p.WriteFile(ctx, ref, dir+"/f", []byte("data"), 0o644)
			wantCode(t, err, provider.CodeNoSpace, false)
			w, err := p.OpenWriter(ctx, ref, dir+"/g", provider.WriteSpec{Size: 4, Mode: 0o644})
			if err != nil {
				t.Fatal(err)
			}
			_, err = w.Write([]byte("data"))
			wantCode(t, err, provider.CodeNoSpace, false)
			_ = w.Abort()
		}
	})
}
