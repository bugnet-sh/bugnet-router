package config

import (
	"reflect"
	"slices"
	"strings"
	"testing"
)

func TestAbuseGPUDefaults(t *testing.T) {
	c, err := LoadFile(writeFile(t, ""))
	if err != nil {
		t.Fatal(err)
	}
	for name, a := range map[string]Abuse{
		"empty file": c.Abuse, "DefaultAbuse": DefaultAbuse(), "OrDefaults": Abuse{}.OrDefaults(),
	} {
		t.Run(name, func(t *testing.T) {
			eq(t, a.LiumDisputeBlock, "provider_failed")
			eq(t, len(a.HFPlatformWords), 0)
		})
	}
}

func TestAbuseLiumDisputeBlock(t *testing.T) {
	for _, class := range []string{"abuse", "control_lost", "provider_failed"} {
		c, err := LoadFile(writeFile(t, "[abuse]\nlium_dispute_block = \""+class+"\"\n"))
		if err != nil {
			t.Fatalf("%s: %v", class, err)
		}
		eq(t, c.Abuse.LiumDisputeBlock, class)
	}
	for _, class := range []string{"sandbox_exited", "account", "ban"} {
		msg := problem(t, "[abuse]\nlium_dispute_block = \""+class+"\"\n")
		want := "abuse.lium_dispute_block must be one of abuse, control_lost, provider_failed"
		if !strings.Contains(msg, want) {
			t.Errorf("%s gave %q, want %q", class, msg, want)
		}
	}
}

func TestAbuseHFPlatformWords(t *testing.T) {
	c, err := LoadFile(writeFile(t,
		"[abuse]\nhf_platform_words = [\"Node Failure\", \" evicted \"]\n"))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, c.Abuse.HFPlatformWords, []string{"node failure", "evicted"})
	msg := problem(t, "[abuse]\nhf_platform_words = [\"node\", \" \"]\n")
	if want := "abuse.hf_platform_words must hold no empty word"; !strings.Contains(msg, want) {
		t.Errorf("an empty word gave %q, want %q", msg, want)
	}
}

func TestExampleTomlHoldsEveryAbuseKey(t *testing.T) {
	eq(t, loadExample(t).Abuse, DefaultAbuse())
	doc, err := outline([]byte(committed(t, "example.toml")))
	if err != nil {
		t.Fatal(err)
	}
	var written, keys []string
	for _, s := range doc {
		if len(s.path) == 2 && s.path[0] == "abuse" {
			written = append(written, s.path[1])
		}
	}
	schema := reflect.TypeFor[abuseFile]()
	for i := range schema.NumField() {
		keys = append(keys, schema.Field(i).Tag.Get("toml"))
	}
	slices.Sort(written)
	slices.Sort(keys)
	eq(t, written, keys)
}
