package imagebuild

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestBuildSucceedsAndNamesEachSize(t *testing.T) {
	f := newFakeProvider("boxd")
	arts, err := Build(t.Context(), f, f, baseSpec())
	if err != nil {
		t.Fatal(err)
	}
	eq(t, len(arts), 2)
	eq(t, arts[0].Ref, "bugnet-dev-2026-11-1-2x2")
	eq(t, arts[1].Ref, "bugnet-dev-2026-11-1-4x8")
	eq(t, arts[0].Manifest.Version, "2026.11.1")
	made, gone := f.builders()
	eq(t, made, 2)
	eq(t, gone, 2)
	for _, req := range f.creates {
		if !req.Builder || len(req.Labels) != 0 || req.Lifetime != time.Hour {
			t.Errorf("builder request %+v", req)
		}
	}
}

func TestBuildBoatMakesOneArtifactForEverySize(t *testing.T) {
	f := newFakeProvider("boat")
	f.user = "user"
	arts, err := Build(t.Context(), f, f, baseSpec())
	if err != nil {
		t.Fatal(err)
	}
	eq(t, len(arts), 1)
	eq(t, arts[0].Ref, "bugnet-dev-2026-11-1")
}

func TestBuildFromOCIFinishesTheImage(t *testing.T) {
	f := newFakeProvider("boxd")
	spec := baseSpec()
	spec.FromOCI, spec.Payload = "ghcr.io/bugnet-sh/base@sha256:abc", nil
	if _, err := Build(t.Context(), f, f, spec); err != nil {
		t.Fatal(err)
	}
	eq(t, f.creates[0].Image, "oci:ghcr.io/bugnet-sh/base@sha256:abc")
}

func TestBuildDestroysBuilderOnEveryPath(t *testing.T) {
	cases := map[string]func(f *fakeProvider, cancel context.CancelFunc){
		"setup failure": func(f *fakeProvider, _ context.CancelFunc) {
			f.failBuild = func(string) bool { return true }
		},
		"save failure": func(f *fakeProvider, _ context.CancelFunc) {
			f.failSave = func(string) bool { return true }
		},
		"scan finding": func(f *fakeProvider, _ context.CancelFunc) {
			f.leakFor = func(string) bool { return true }
		},
		"cancelled context": func(f *fakeProvider, cancel context.CancelFunc) {
			f.onExec = func(argv []string) {
				if strings.Contains(strings.Join(argv, " "), "setup-image.sh build") {
					cancel()
				}
			}
		},
	}
	for name, arrange := range cases {
		t.Run(name, func(t *testing.T) {
			f := newFakeProvider("boxd")
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			arrange(f, cancel)
			_, err := Build(ctx, f, f, baseSpec())
			if name != "cancelled context" && err == nil {
				t.Fatal("the build passed")
			}
			made, gone := f.builders()
			if made == 0 || made != gone {
				t.Errorf("%d builders made, %d destroyed", made, gone)
			}
			for _, live := range f.destroyCtxLive {
				if !live {
					t.Error("a builder was destroyed with the build's own cancelled context")
				}
			}
		})
	}
}

func TestBuildFindingDeletesSavedArtifact(t *testing.T) {
	f := newFakeProvider("boxd")
	f.leakFor = func(size string) bool { return size == "4x8" }
	_, err := Build(t.Context(), f, f, baseSpec())
	var fe *FindingsError
	if !errors.As(err, &fe) {
		t.Fatalf("Build gave %v, want a findings error", err)
	}
	eq(t, len(f.saved), 1)
	eq(t, strings.Join(f.deleted, ","), "bugnet-dev-2026-11-1-2x2")
	if strings.Contains(err.Error(), testSecret) {
		t.Error("the error printed a secret")
	}
}

func TestBuildRefusesLargePayload(t *testing.T) {
	f := newFakeProvider("boxd")
	spec := baseSpec()
	spec.Payload = make([]byte, MaxPayload+1)
	if _, err := Build(t.Context(), f, f, spec); !errors.Is(err, ErrPayloadSize) {
		t.Fatalf("Build gave %v, want ErrPayloadSize", err)
	}
	if len(f.creates) != 0 {
		t.Error("a builder was made for a payload that is too large")
	}
}

func TestBuildNamesTheSetupLineNotTheOutput(t *testing.T) {
	f := newFakeProvider("boxd")
	f.failBuild = func(string) bool { return true }
	_, err := Build(t.Context(), f, f, baseSpec())
	if err == nil || !strings.Contains(err.Error(), "setup-image: step4 failed") {
		t.Fatalf("Build gave %v, want the setup-image line", err)
	}
}
