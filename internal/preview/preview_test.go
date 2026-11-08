package preview_test

import (
	"reflect"
	"regexp"
	"testing"

	"github.com/bugnet-sh/bugnet-router/internal/dpkeys"
	"github.com/bugnet-sh/bugnet-router/internal/preview"
)

// eq fails the test when got and want differ
func eq[T any](t *testing.T, got, want T) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func keys(t *testing.T) dpkeys.Keys {
	t.Helper()
	k, err := dpkeys.Parse(dpkeys.NewEnv())
	if err != nil {
		t.Fatal(err)
	}
	return k
}

func TestNewIDIs16LowercaseBase32Characters(t *testing.T) {
	shape := regexp.MustCompile(`^[a-z2-7]{16}$`)
	seen := map[string]bool{}
	for range 1000 {
		id := preview.NewID()
		eq(t, shape.MatchString(id), true)
		eq(t, preview.ValidID(id), true)
		eq(t, seen[id], false)
		seen[id] = true
	}
}

func TestValidIDRefusesOtherShapes(t *testing.T) {
	for _, id := range []string{
		"", "abcdefghijklmnop2", "abcdefghijklmno", "ABCDEFGHIJKLMNOP", "abcdefghijklmn01",
		"abcdefghijklmn-7", "abcdefghijklmnop.preview", "abcdefghijklmné",
	} {
		if preview.ValidID(id) {
			t.Errorf("ValidID(%q) is true", id)
		}
	}
	eq(t, preview.ValidID("abcdefghijklmn27"), true)
}

func TestURLCarriesTheTokenInItsQuery(t *testing.T) {
	eq(t, preview.URL("abcdefghijklmn27", "preview.bugnet.sh", "bnp_x-y_z"),
		"https://abcdefghijklmn27.preview.bugnet.sh/?bugnet_token=bnp_x-y_z")
}

func TestCheckTellsCurrentFromEarlierGenerations(t *testing.T) {
	k := keys(t)
	id := preview.NewID()
	tok := func(gen uint32) string {
		t.Helper()
		s, err := k.PreviewToken(k.Current().ID, id, gen)
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
	eq(t, preview.Check(k, id, 0, tok(0)), preview.Current)
	eq(t, preview.Check(k, id, 1, tok(0)), preview.Earlier)
	eq(t, preview.Check(k, id, 1, tok(1)), preview.Current)
	eq(t, preview.Check(k, id, 0, tok(1)), preview.Invalid)
	eq(t, preview.Check(k, id, 3, tok(1)), preview.Earlier)
	eq(t, preview.Check(k, preview.NewID(), 0, tok(0)), preview.Invalid)
	eq(t, preview.Check(k, id, 0, ""), preview.Invalid)
	eq(t, preview.Check(k, id, 0, tok(0)+"x"), preview.Invalid)
	eq(t, preview.Check(keys(t), id, 0, tok(0)), preview.Invalid)
}
