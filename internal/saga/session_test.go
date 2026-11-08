package saga_test

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/riverqueue/river"

	"github.com/bugnet-sh/bugnet-router/internal/jobargs"
	"github.com/bugnet-sh/bugnet-router/internal/placement"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/local"
	"github.com/bugnet-sh/bugnet-router/internal/saga"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

// sessionEnded fails the test unless the placement's record ended and its one copy is queued
func (e *env) sessionEnded(t *testing.T, id uuid.UUID) {
	t.Helper()
	rec, err := e.store.SessionRecordByPlacement(t.Context(), id)
	if err != nil {
		t.Fatal(err)
	}
	if rec.EndedAt == nil || rec.ExpiresAt == nil || rec.CopyState != store.CopyPending {
		t.Fatalf("record %+v, want ended with its copy pending", rec)
	}
	res, err := e.river.JobList(t.Context(),
		river.NewJobListParams().Kinds(jobargs.SessionCopy{}.Kind()))
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, j := range res.Jobs {
		var args jobargs.SessionCopy
		if err := json.Unmarshal(j.EncodedArgs, &args); err != nil {
			t.Fatal(err)
		}
		if args.PlacementID == id {
			n++
			if j.Queue != jobargs.QueueDataplane {
				t.Fatalf("session_copy on %s", j.Queue)
			}
		}
	}
	if n != 1 {
		t.Fatalf("%d session_copy jobs for %s, want 1", n, id)
	}
}

func TestFailedPlacementEndsItsSessionRecord(t *testing.T) {
	t.Run("no candidate", func(t *testing.T) {
		e := newEnv(t, newLocal(t, local.Options{Name: "a"}))
		p, _, err := e.saga.Create(t.Context(), e.startToken(t, "order-1"),
			placement.Requirements{GPU: true})
		if !errors.Is(err, placement.ErrNoCandidate) {
			t.Fatalf("err %v, want ErrNoCandidate", err)
		}
		e.sessionEnded(t, p.ID)
	})
	t.Run("every attempt failed", func(t *testing.T) {
		e := newEnv(t, newLocal(t, local.Options{Name: "a",
			CreateHook: failWith(provider.CodeCapacity, true)}))
		p, _ := e.place(t, "order-1")
		if p.State != store.PlacementFailed {
			t.Fatalf("placement %+v, want failed", p)
		}
		e.sessionEnded(t, p.ID)
	})
	t.Run("stuck", func(t *testing.T) {
		e := newEnv(t, newLocal(t, local.Options{Name: "a"}))
		p, _, err := e.store.CreatePlacement(t.Context(), store.NewPlacement{
			HandleSHA256: []byte("stuck-handle"), OrderID: "order-1", PackageID: smallPkg,
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := e.store.CreateSessionRecord(t.Context(), p.ID); err != nil {
			t.Fatal(err)
		}
		if err := e.saga.FailStuck(t.Context(), p.ID, saga.ReasonPlacingStalled); err != nil {
			t.Fatal(err)
		}
		e.saga.Wait()
		e.sessionEnded(t, p.ID)
	})
}
