package hf

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

// standIn is the fake sbx-server a baked Job runs, printing what it started with
const standIn = "#!/bin/sh\n" +
	`echo "sbx-server ran port=$SBX_PORT sha=${SBX_SERVER_SHA256-unset} ` +
	`url=${SBX_SERVER_URL-unset} token=${#SBX_TOKEN}"` + "\n"

func TestHFBakedBoot(t *testing.T) {
	sum := sha256.Sum256([]byte(standIn))
	pinned := hex.EncodeToString(sum[:])
	cases := []struct {
		name string
		// file is what sits at the baked path, nothing when empty
		file string
		mode os.FileMode
		ran  bool
	}{
		{"the pinned build runs as PID 1", standIn, 0o755, true},
		{"a missing binary ends the Job", "", 0, false},
		{"a changed binary ends the Job", standIn + "echo changed\n", 0o755, false},
		{"a binary without its exec bit ends the Job", standIn, 0o644, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := newFake(t)
			f.boot = true
			opts := testOptions()
			opts.SBXServerBaked, opts.SBXServerSHA256 = true, pinned
			p := f.provider(t, opts)
			p.sbxPath = filepath.Join(f.sbx.bin, "sbx-server")
			if c.file != "" {
				if err := os.WriteFile(p.sbxPath, []byte(c.file), 0o600); err != nil {
					t.Fatal(err)
				}
				if err := os.Chmod(p.sbxPath, c.mode); err != nil {
					t.Fatal(err)
				}
			}
			ref, j := created(t, f, p)
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			err := p.WaitReady(ctx, ref)
			out := f.bootOutput()
			if c.ran {
				if err != nil {
					t.Fatalf("%v, the boot printed %q", err, out)
				}
				// The digest is unset before the exec, and the token reaches the server alone
				if !strings.Contains(out, "sbx-server ran port=49983 sha=unset url=unset "+
					"token=64\n") {
					t.Fatalf("the boot printed %q", out)
				}
				eq(t, f.stageOf(j.id), "RUNNING")
				return
			}
			e, ok := provider.AsError(err)
			if !ok || e.Code != provider.CodeCapacity || !e.CanFailOver ||
				!strings.Contains(err.Error(), "ERROR") {
				t.Fatalf("got %v, want a capacity error naming ERROR that fails over", err)
			}
			if strings.Contains(out, "sbx-server ran") {
				t.Fatalf("the boot ran a binary its digest refuses, it printed %q", out)
			}
			eq(t, f.stageOf(j.id), "ERROR")
			eq(t, len(sbxRequests(f)), 0)
		})
	}
}
