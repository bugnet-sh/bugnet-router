package transfer_test

import (
	"crypto/sha256"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/valkey-io/valkey-go"

	"github.com/bugnet-sh/bugnet-router/internal/testenv"
	"github.com/bugnet-sh/bugnet-router/internal/transfer"
)

var limits = transfer.Limits{MaxTokens: 16, MaxActive: 2, MaxClaims: 5, TTL: 40 * time.Minute}

func newStore(t *testing.T) *transfer.Store {
	t.Helper()
	vk, prefix := testenv.Valkey(t)
	return transfer.NewStore(vk, prefix, limits)
}

func token(n int) []byte {
	sum := sha256.Sum256([]byte("bnx_" + strconv.Itoa(n)))
	return sum[:]
}

func record(p uuid.UUID, n int, now time.Time) transfer.Record {
	return transfer.Record{
		ID: transfer.ID(token(n)), Placement: p, Direction: upload, Path: "/root/f", Size: 10,
		Mode: 0o640, SHA256: make([]byte, 32), ExpiresAt: now.Add(10 * time.Minute),
		StagedPath: "/home/boxd/.bugnet/xfer/x/f",
	}
}

func put(t *testing.T, s *transfer.Store, r transfer.Record, now time.Time) {
	t.Helper()
	if _, created, err := s.Put(t.Context(), r, now); err != nil || !created {
		t.Fatalf("put %s: %v, created %v", r.ID, err, created)
	}
}

func claim(
	t *testing.T, s *transfer.Store, n int, now time.Time,
) (transfer.Record, string, error) {
	t.Helper()
	return s.Claim(t.Context(), token(n), "edge-1", now)
}

func TestPutKeepsTheFirstRecordAndCapsOpenTokens(t *testing.T) {
	s := newStore(t)
	now := time.Now().Truncate(time.Millisecond)
	p := uuid.New()
	r := record(p, 0, now)
	put(t, s, r, now)
	again := r
	again.Path = "/root/other"
	got, created, err := s.Put(t.Context(), again, now)
	eq(t, err, nil)
	eq(t, created, false)
	eq(t, got.Path, "/root/f")
	r.State = transfer.Issued
	r.ExpiresAt = r.ExpiresAt.UTC()
	eq(t, got, r)

	for n := 1; n < 16; n++ {
		put(t, s, record(p, n, now), now)
	}
	_, _, err = s.Put(t.Context(), record(p, 16, now), now)
	is(t, err, transfer.ErrTooMany)
	put(t, s, record(uuid.New(), 16, now), now)
	later := now.Add(11 * time.Minute)
	put(t, s, record(p, 17, later), later)
}

func TestClaimStates(t *testing.T) {
	s := newStore(t)
	now := time.Now()
	p := uuid.New()
	_, _, err := claim(t, s, 0, now)
	is(t, err, transfer.ErrUnknown)
	put(t, s, record(p, 0, now), now)
	r, lease, err := claim(t, s, 0, now)
	if err != nil || lease == "" || r.State != transfer.Active || r.Claims != 1 {
		t.Fatalf("first claim: %+v, %q, %v", r, lease, err)
	}
	_, _, err = claim(t, s, 0, now.Add(time.Second))
	is(t, err, transfer.ErrInUse)
	if err := s.Done(t.Context(), r, lease, 10); err != nil {
		t.Fatal(err)
	}
	_, _, err = claim(t, s, 0, now.Add(time.Second))
	is(t, err, transfer.ErrUsed)

	put(t, s, record(p, 1, now), now)
	_, _, err = claim(t, s, 1, now.Add(10*time.Minute))
	is(t, err, transfer.ErrExpired)
}

func TestSixthClaimIsSpent(t *testing.T) {
	s := newStore(t)
	now := time.Now()
	put(t, s, record(uuid.New(), 0, now), now)
	for i := range 5 {
		r, lease, err := claim(t, s, 0, now)
		if err != nil {
			t.Fatalf("claim %d: %v", i+1, err)
		}
		if err := s.Release(t.Context(), r, lease, 1); err != nil {
			t.Fatal(err)
		}
	}
	_, _, err := claim(t, s, 0, now)
	is(t, err, transfer.ErrClaimsSpent)
}

func TestByteBudgetSpendsTheURL(t *testing.T) {
	s := newStore(t)
	now := time.Now()
	put(t, s, record(uuid.New(), 0, now), now)
	r, lease, err := claim(t, s, 0, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Release(t.Context(), r, lease, 2*10+transfer.ByteSlack); err != nil {
		t.Fatal(err)
	}
	_, _, err = claim(t, s, 0, now)
	is(t, err, transfer.ErrClaimsSpent)
}

func TestTwoActiveTransfersPerPlacement(t *testing.T) {
	s := newStore(t)
	now := time.Now()
	p := uuid.New()
	for n := range 3 {
		put(t, s, record(p, n, now), now)
	}
	_, _, err := claim(t, s, 0, now)
	eq(t, err, nil)
	r1, lease1, err := claim(t, s, 1, now)
	eq(t, err, nil)
	_, _, err = claim(t, s, 2, now)
	is(t, err, transfer.ErrTooMany)
	if err := s.Release(t.Context(), r1, lease1, 0); err != nil {
		t.Fatal(err)
	}
	_, _, err = claim(t, s, 2, now)
	eq(t, err, nil)
}

func TestLeaseLapseReturnsToIssued(t *testing.T) {
	s := newStore(t)
	now := time.Now()
	p := uuid.New()
	put(t, s, record(p, 0, now), now)
	put(t, s, record(p, 1, now), now)
	put(t, s, record(p, 2, now), now)
	r, lease, err := claim(t, s, 0, now)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = claim(t, s, 1, now)
	eq(t, err, nil)
	renewed, err := s.Renew(t.Context(), r, lease, 4, now.Add(20*time.Second))
	eq(t, err, nil)
	eq(t, renewed.BytesSent, int64(4))
	_, _, err = claim(t, s, 0, now.Add(49*time.Second))
	is(t, err, transfer.ErrInUse)

	lapsed := now.Add(51 * time.Second)
	again, lease2, err := claim(t, s, 0, lapsed)
	if err != nil || again.Claims != 2 || lease2 == lease {
		t.Fatalf("claim after the lease lapsed: %+v, %v", again, err)
	}
	_, _, err = claim(t, s, 2, lapsed)
	eq(t, err, nil)
	_, err = s.Renew(t.Context(), r, lease, 1, lapsed)
	is(t, err, transfer.ErrInUse)
	is(t, s.Done(t.Context(), r, lease, 1), transfer.ErrInUse)
}

func TestRecordFieldsCarryTheSpecNames(t *testing.T) { // DP 8.3.1
	vk, prefix := testenv.Valkey(t)
	s := transfer.NewStore(vk, prefix, limits)
	now := time.Now()
	r := record(uuid.New(), 0, now)
	put(t, s, r, now)
	m, err := vk.Do(t.Context(), vk.B().Hgetall().Key(prefix+"xfer:"+r.ID).Build()).AsStrMap()
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{
		"placement_id", "direction", "path", "size", "mode", "sha256", "etag", "expires_at",
		"state", "lease", "claims", "bytes_sent", "staged_path",
	} {
		if _, ok := m[name]; !ok {
			t.Errorf("the record has no %s field, it has %v", name, m)
		}
	}
	eq(t, m["placement_id"], r.Placement.String())
	eq(t, m["staged_path"], r.StagedPath)
}

// expiresAt is when Valkey drops the record of id, in Unix milliseconds
func expiresAt(t *testing.T, vk valkey.Client, prefix, id string) int64 {
	t.Helper()
	at, err := vk.Do(t.Context(), vk.B().Pexpiretime().Key(prefix+"xfer:"+id).Build()).AsInt64()
	if err != nil {
		t.Fatal(err)
	}
	return at
}

func TestAClaimKeepsTheRecordUntilItsLeaseEnds(t *testing.T) {
	vk, prefix := testenv.Valkey(t)
	short := limits
	short.TTL = time.Second
	s := transfer.NewStore(vk, prefix, short)
	now := time.Now()
	r := record(uuid.New(), 0, now)
	put(t, s, r, now)
	r, lease, err := claim(t, s, 0, now)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, expiresAt(t, vk, prefix, r.ID) >= now.Add(transfer.LeaseLength).UnixMilli(), true)
	later := now.Add(20 * time.Second)
	if _, err := s.Renew(t.Context(), r, lease, 0, later); err != nil {
		t.Fatal(err)
	}
	eq(t, expiresAt(t, vk, prefix, r.ID) >= later.Add(transfer.LeaseLength).UnixMilli(), true)
}

func TestAResumeNearTheRecordsEndKeepsItsRecord(t *testing.T) {
	vk, prefix := testenv.Valkey(t)
	short := limits
	short.TTL, short.Lease = 500*time.Millisecond, 400*time.Millisecond
	s := transfer.NewStore(vk, prefix, short)
	now := time.Now()
	r := record(uuid.New(), 0, now)
	r.Direction, r.ExpiresAt = download, now.Add(100*time.Millisecond)
	put(t, s, r, now)
	first, lease, err := claim(t, s, 0, now)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Release(t.Context(), first, lease, 1); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	resumed, lease, err := claim(t, s, 0, time.Now())
	if err != nil {
		t.Fatalf("a resume after the window: %v", err)
	}
	time.Sleep(350 * time.Millisecond)
	if _, err := s.Renew(t.Context(), resumed, lease, 1, time.Now()); err != nil {
		t.Fatalf("a renewal past the record's first end: %v", err)
	}
}

func TestCheckAnswersAsAClaimWould(t *testing.T) {
	s := newStore(t)
	now := time.Now()
	r := record(uuid.New(), 0, now)
	put(t, s, r, now)
	got, err := s.Get(t.Context(), r.ID)
	eq(t, err, nil)
	eq(t, s.Check(got, now), nil)
	is(t, s.Check(got, now.Add(10*time.Minute)), transfer.ErrExpired)
	claimed, lease, err := claim(t, s, 0, now)
	if err != nil {
		t.Fatal(err)
	}
	got, _ = s.Get(t.Context(), r.ID)
	eq(t, got.LeaseUntil.Equal(now.Add(transfer.LeaseLength).Truncate(time.Millisecond)), true)
	is(t, s.Check(got, now.Add(time.Second)), transfer.ErrInUse)
	eq(t, s.Check(got, now.Add(transfer.LeaseLength+time.Second)), nil)
	if err := s.Release(t.Context(), claimed, lease, 2*10+transfer.ByteSlack); err != nil {
		t.Fatal(err)
	}
	got, _ = s.Get(t.Context(), r.ID)
	is(t, s.Check(got, now), transfer.ErrClaimsSpent)
	got.State = transfer.Done
	is(t, s.Check(got, now), transfer.ErrUsed)
}
