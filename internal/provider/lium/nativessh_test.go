package lium

import (
	"context"
	"errors"
	"io/fs"
	"maps"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/sshkeys"
)

// gatewayKey is an authorized_keys line as the saga hands the gateway's key over
func gatewayKey(t *testing.T) string {
	t.Helper()
	line, err := sshkeys.Throwaway()
	if err != nil {
		t.Fatal(err)
	}
	return line
}

// authorizedKeys is the pod's authorized_keys file on the fake
func authorizedKeys(f *fakeLium) string {
	return filepath.Join(f.sh.home, ".ssh", "authorized_keys")
}

// wantNoKeys fails the test when the pod's authorized_keys file exists
func wantNoKeys(t *testing.T, f *fakeLium) {
	t.Helper()
	if _, err := os.Stat(authorizedKeys(f)); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("authorized_keys was written: %v", err)
	}
}

func TestLiumAuthorizeNativeSSH(t *testing.T) {
	ctx := context.Background()
	t.Run("appends the key once and answers the pod's sshd", func(t *testing.T) {
		f, p := liveFake(t)
		var _ provider.NativeSSHer = p
		ref, pod := readyRef(t, f, p)
		keys := authorizedKeys(f)
		if err := os.MkdirAll(filepath.Dir(keys), 0o700); err != nil {
			t.Fatal(err)
		}
		// Lium's line for the router's key, with no newline at its end
		router := p.podPublicKey(ref.Meta["pod_name"])
		if err := os.WriteFile(keys, []byte(router), 0o600); err != nil {
			t.Fatal(err)
		}
		line := gatewayKey(t)
		want := provider.NativeSSH{
			Address: net.JoinHostPort("203.0.113.7", strconv.Itoa(pod.sshPort)), User: "root",
			HostKey: f.sshd(pod.id).hostPublicKey(),
		}
		for range 2 {
			got, err := p.AuthorizeNativeSSH(ctx, ref, line)
			if err != nil {
				t.Fatal(err)
			}
			eq(t, got, want)
			raw, err := os.ReadFile(keys)
			if err != nil {
				t.Fatal(err)
			}
			eq(t, string(raw), router+"\n"+line+"\n")
		}
		// A second gateway key gets its own line
		second := gatewayKey(t)
		if _, err := p.AuthorizeNativeSSH(ctx, ref, second); err != nil {
			t.Fatal(err)
		}
		raw, _ := os.ReadFile(keys)
		eq(t, string(raw), router+"\n"+line+"\n"+second+"\n")
	})
	t.Run("makes the file only its owner reads", func(t *testing.T) {
		f, p := liveFake(t)
		ref, _ := readyRef(t, f, p)
		line := gatewayKey(t)
		if _, err := p.AuthorizeNativeSSH(ctx, ref, line); err != nil {
			t.Fatal(err)
		}
		raw, err := os.ReadFile(authorizedKeys(f))
		if err != nil {
			t.Fatal(err)
		}
		eq(t, string(raw), line+"\n")
		for path, mode := range map[string]fs.FileMode{
			authorizedKeys(f): 0o600, filepath.Dir(authorizedKeys(f)): 0o700,
		} {
			info, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			eq(t, info.Mode().Perm(), mode)
		}
	})
	t.Run("a ref without its SSH entries reads them from Lium", func(t *testing.T) {
		f, p := liveFake(t)
		ref, pod := readyRef(t, f, p)
		bare := provider.Ref{Provider: ref.Provider, ID: ref.ID, Meta: maps.Clone(ref.Meta)}
		for _, k := range []string{"ssh_host", "ssh_port", provider.MetaHostKey, "pod_name"} {
			delete(bare.Meta, k)
		}
		got, err := p.AuthorizeNativeSSH(ctx, bare, gatewayKey(t))
		if err != nil {
			t.Fatal(err)
		}
		eq(t, got, provider.NativeSSH{
			Address: net.JoinHostPort("203.0.113.7", strconv.Itoa(pod.sshPort)), User: "root",
			HostKey: f.sshd(pod.id).hostPublicKey(),
		})
	})
	t.Run("refuses a line that is not one key", func(t *testing.T) {
		f, p := liveFake(t)
		ref, _ := readyRef(t, f, p)
		line := gatewayKey(t)
		for _, bad := range []string{
			"", "not a key", line + "\n" + gatewayKey(t), line + "\n", line + "\r",
			line + "\x00", "\n" + line, line + " " + strings.Repeat("a", 9000),
		} {
			_, err := p.AuthorizeNativeSSH(ctx, ref, bad)
			wantCode(t, err, provider.CodeInvalid, false)
		}
		wantNoKeys(t, f)
	})
	t.Run("a pod Lium no longer has is not found", func(t *testing.T) {
		f, p := liveFake(t)
		ref, pod := readyRef(t, f, p)
		f.mu.Lock()
		delete(f.pods, pod.id)
		f.mu.Unlock()
		_, err := p.AuthorizeNativeSSH(ctx, ref, gatewayKey(t))
		wantCode(t, err, provider.CodeNotFound, false)
		wantNoKeys(t, f)
		_, err = p.AuthorizeNativeSSH(ctx, provider.Ref{Provider: "boat", ID: ref.ID},
			gatewayKey(t))
		wantCode(t, err, provider.CodeNotFound, false)
	})
	t.Run("a command that fails is an internal error", func(t *testing.T) {
		f, p := liveFake(t)
		ref, _ := readyRef(t, f, p)
		// A directory where the file should be makes every write fail
		if err := os.MkdirAll(authorizedKeys(f), 0o700); err != nil {
			t.Fatal(err)
		}
		_, err := p.AuthorizeNativeSSH(ctx, ref, gatewayKey(t))
		wantCode(t, err, provider.CodeInternal, false)
		if !strings.Contains(err.Error(), "authorized_keys") {
			t.Fatalf("error %v names no authorized_keys", err)
		}
	})
}
