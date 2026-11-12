package leakscan

import (
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"testing"
)

func eq[T any](t *testing.T, got, want T) {
	t.Helper()
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func scan(t *testing.T, archive io.Reader, n Needles, a Allowlist) []Finding {
	t.Helper()
	f, err := Scan(archive, n, a)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func rules(f []Finding) []string {
	var out []string
	for _, x := range f {
		out = append(out, x.Rule+" "+x.Path)
	}
	return out
}

func TestEveryNeedleInEveryEncodingOnce(t *testing.T) {
	needles := values()
	f := scan(t, fixtureWithEachNeedleInEachEncoding(t), needles, Base(false))
	eq(t, len(f), len(needles)*5)
	count := map[string]int{}
	for _, x := range f {
		count[x.Rule]++
	}
	for rule, n := range count {
		if n != 1 {
			t.Errorf("%s found %d times", rule, n)
		}
	}
}

func TestEveryEncodingOfAValueInsideOtherData(t *testing.T) {
	v := []byte("\xfb\xffneedle-inside-other-bytes")
	n := Needles{{"N", v}}
	for k := range 3 {
		blob := append(bytes.Repeat([]byte{'x'}, k), v...)
		blob = append(blob, 'y', 'z')
		text := base64.StdEncoding.EncodeToString(blob)
		f := scan(t, newFixture(t).file("home/blob", text).archive(), n, Base(false))
		if len(f) != 1 || f[0].Rule != "needle N base64" {
			t.Errorf("alignment %d gave %v", k, f)
		}
	}
}

func TestFindingsCarryOffsetAndPid(t *testing.T) {
	v := "needle-value-in-environ"
	n := Needles{{"N", []byte(v)}}
	f := scan(t, newFixture(t).environ(42, "A=b", "C="+v).archive(), n, Base(false))
	eq(t, len(f), 1)
	eq(t, f[0], Finding{Rule: "needle N raw", Path: "proc/42/environ", Pid: 42, Offset: 6})
}

func TestDerivedKeysAreNeedles(t *testing.T) {
	keys := "k1:" + base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{7}, 32))
	n, err := FromEnv([]string{"BUGNET_DATA_PLANE_KEYS=" + keys})
	if err != nil {
		t.Fatal(err)
	}
	var gate, placeholder []byte
	for _, x := range n {
		switch x.Name {
		case "derived gate k1":
			gate = x.Value
		case "derived placeholder k1":
			placeholder = x.Value
		}
	}
	f := scan(t, newFixture(t).
		file("tmp/a", string(gate)).
		file("tmp/b", base64.StdEncoding.EncodeToString(placeholder)).archive(), n, Base(false))
	eq(t, rules(f), []string{"needle derived gate k1 raw tmp/a",
		"needle derived placeholder k1 base64 tmp/b"})
}

func TestFromEnvListsEveryPurposeOfEveryRoot(t *testing.T) {
	root := func(b byte) string {
		return base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{b}, 32))
	}
	n, err := FromEnv([]string{
		"BUGNET_DATA_PLANE_KEYS=k2:" + root(2) + ",k1:" + root(1),
		"BUGNET_DATABASE_URL=postgres://u:p%40ssword9@h/db",
		"BUGNET_INTERNAL_TOKEN=internal-token-value",
		"E2B_WEBHOOK_SECRET=webhook-secret-value",
		"HOME=/root",
	})
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, x := range n {
		names = append(names, x.Name)
	}
	for _, want := range []string{"derived gate k1", "derived ref k2", "derived sessions k2",
		"root k1", "BUGNET_DATABASE_URL", "BUGNET_DATABASE_URL password",
		"BUGNET_INTERNAL_TOKEN", "E2B_WEBHOOK_SECRET"} {
		if !slices.Contains(names, want) {
			t.Errorf("no needle %q in %v", want, names)
		}
	}
	for _, x := range n {
		if x.Name == "BUGNET_DATABASE_URL password" {
			eq(t, string(x.Value), "p@ssword9")
		}
	}
	_, err = FromEnv([]string{"BUGNET_HANDLE_KEY=short"})
	if err == nil || strings.Contains(err.Error(), "short\n") {
		t.Fatalf("a short value gave %v", err)
	}
}

func TestNoPathAllowsANeedle(t *testing.T) {
	v := "internal-token-value-abc"
	n := Needles{{"BUGNET_INTERNAL_TOKEN", []byte(v)}}
	a := For("boat", true)
	a.Allow = append(a.Allow, Allow{Shape: Anywhere}, Allow{Shape: Anywhere, Path: "tmp/"})
	f := scan(t, newFixture(t).
		file("tmp/bugnet/jobs/x", `{"env":"`, v, `"}`).
		file(GateKeyPath, v).archive(), n, a)
	eq(t, rules(f), []string{"needle BUGNET_INTERNAL_TOKEN raw etc/bugnet/gate.key",
		"needle BUGNET_INTERNAL_TOKEN raw tmp/bugnet/jobs/x"})
}

const gateKey = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFG"

func TestGateKeyOutsideItsFile(t *testing.T) {
	f := scan(t, newFixture(t).file(GateKeyPath, gateKey, "\n").
		file("tmp/bugnet/jobs/y", "x=", gateKey).archive(), nil, Base(false))
	eq(t, rules(f), []string{"own gate key raw tmp/bugnet/jobs/y"})
}

func TestOtherSandboxValueFound(t *testing.T) {
	other := Needles{{"other gate key", []byte(gateKey)}}
	f := scan(t, newFixture(t).file(GateKeyPath, "another-gate-key-for-this-box-xyz").
		file("etc/stray", base64.URLEncoding.EncodeToString([]byte(gateKey))).archive(),
		other, Base(false))
	eq(t, rules(f), []string{"needle other gate key base64 etc/stray"})
}

func TestOwnValuesReturned(t *testing.T) {
	res, err := ScanFull(newFixture(t).file(GateKeyPath, gateKey, "\n").archive(), nil, Base(true))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, len(res.Own), 1)
	eq(t, string(res.Own[0].Value), gateKey)
	eq(t, res.Own[0].Name, "gate key")
}

func TestAllowedShapesAtTheirPaths(t *testing.T) {
	pem := "-----BEGIN OPENSSH PRIVATE KEY-----\nabc\n-----END OPENSSH PRIVATE KEY-----\n"
	build := func(env ...string) *bytes.Reader {
		return newFixture(t).
			file(GateKeyPath, gateKey).
			file(TunnelPath, "bnt_", strings.Repeat("a", 30)).
			file(HostKeyPath, pem).
			file("etc/bugnet/ssh/authorized_keys", "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIabc u").
			file("usr/local/share/ca-certificates/bugnet.crt", "-----BEGIN CERTIFICATE-----\n").
			file("etc/bugnet/secrets.env", "TOKEN=bnsec_", strings.Repeat("b", 30), "\n").
			environ(7, env...).archive()
	}
	tok := strings.Repeat("c", 30)
	eq(t, rules(scan(t, build("ASCII_TOKEN=boat_"+tok, "BOAT_ID=sbx_"+tok), nil,
		For("boat", true))), []string(nil))
	eq(t, rules(scan(t, build("SANDBOX_SECRET=hf_"+tok, "BUGNET_LABELS=sk-"+tok), nil,
		For("koyeb", true))), []string(nil))
	eq(t, rules(scan(t, build("E2B_SANDBOX_ID=ghp_"+tok), nil, For("e2b", true))),
		[]string(nil))
}

func TestAllowedShapesNeedTheirPlace(t *testing.T) {
	tunnel := "bnt_" + strings.Repeat("a", 30)
	pem := "-----BEGIN OPENSSH PRIVATE KEY-----"
	f := scan(t, newFixture(t).
		file(TunnelPath, tunnel).
		file("etc/elsewhere", tunnel).
		file("root/.ssh/id", pem).
		file("etc/profile", "bnsec_", strings.Repeat("b", 30)).
		environ(3, "ASCII_TOKEN=boat_"+strings.Repeat("c", 30)).archive(), nil, Base(false))
	eq(t, rules(f), []string{"shape boat proc/3/environ", "shape placeholder etc/profile",
		"shape private-key root/.ssh/id", "shape tunnel etc/bugnet/egress.token",
		"shape tunnel etc/elsewhere"})
}

func TestEnvAllowanceKeepsToItsVariable(t *testing.T) {
	a := For("boat", false)
	f := scan(t, newFixture(t).
		environ(3, "OTHER=boat_"+strings.Repeat("c", 30),
			"ASCII_TOKEN=boat_"+strings.Repeat("c", 30)).
		file("etc/units", "Environment=BOAT_ID=sbx_", strings.Repeat("d", 24), " X=1\n").
		archive(), nil, a)
	eq(t, rules(f), []string{"shape boat proc/3/environ"})
}

func TestEveryShapeOnce(t *testing.T) {
	samples := map[string]string{
		"e2b": "e2b_" + strings.Repeat("a1", 20), "boxd": "bxd_" + strings.Repeat("A", 43),
		"boat": "boat_" + strings.Repeat("b", 24), "box": "box_" + strings.Repeat("b", 24),
		"webhook": "whsec_" + strings.Repeat("f", 64), "handle": "sbx_" + strings.Repeat("h", 24),
		"order":        "v1." + strings.Repeat("o", 20) + "." + strings.Repeat("p", 20),
		"private-key":  "-----BEGIN RSA PRIVATE KEY-----",
		"github":       "ghp_" + strings.Repeat("g", 30),
		"github-pat":   "github_pat_" + strings.Repeat("g", 30),
		"hugging-face": "hf_" + strings.Repeat("h", 30), "openai": "sk-" + strings.Repeat("s", 30),
		"aws":     "AKIA" + strings.Repeat("A", 16),
		"preview": "bnp_" + strings.Repeat("p", 30), "transfer": "bnx_" + strings.Repeat("p", 30),
		"link": "bnsl_" + strings.Repeat("p", 30), "put": "bnsp_" + strings.Repeat("p", 30),
		"tunnel":      "bnt_" + strings.Repeat("p", 30),
		"placeholder": "bnsec_" + strings.Repeat("p", 30),
	}
	eq(t, len(samples), len(Shapes))
	for name, s := range samples {
		f := scan(t, newFixture(t).file("etc/x", "see ", s, " here").archive(), nil, Base(false))
		eq(t, rules(f), []string{"shape " + name + " etc/x"})
	}
}

func TestOrdinaryWordsAreNotShapes(t *testing.T) {
	f := scan(t, newFixture(t).file("etc/x",
		"pip install hf_transfer\nsk-learn-tools\nversion v1.2.3\n",
		"task-abcdefghijklmnopqrstuvwxyz\n",
		"AKIAshort\nbnsec_short\n").archive(), nil, Base(false))
	eq(t, rules(f), []string(nil))
}

func TestMetadataAnswerFails(t *testing.T) {
	f := scan(t, newFixture(t).file("metadata/imds-v4", "200\n").archive(), nil, Base(false))
	eq(t, rules(f), []string{"metadata answered metadata/imds-v4"})
}

func TestNamesListedForReview(t *testing.T) {
	res, err := ScanFull(newFixture(t).
		environ(5, "NEW_PROVIDER_TOKEN=plain", "HOME=/root", "ASCII_TOKEN=x").archive(), nil,
		For("boat", false))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, len(res.Findings), 0)
	eq(t, res.Names, []string{"NEW_PROVIDER_TOKEN"})
}

type zeros struct{}

func (zeros) Read(p []byte) (int, error) { clear(p); return len(p), nil }

func TestArchiveAtTheLimitFails(t *testing.T) {
	_, err := Scan(io.LimitReader(zeros{}, MaxArchive), nil, Base(false))
	if !errors.Is(err, ErrTooLarge) {
		t.Fatalf("a 64 MiB archive gave %v", err)
	}
	_, err = Scan(io.LimitReader(zeros{}, MaxArchive+1), nil, Base(false))
	if !errors.Is(err, ErrTooLarge) {
		t.Fatalf("a larger archive gave %v", err)
	}
}

// failOnce fails one read at offset at with err and serves the rest of r after it
type failOnce struct {
	r      *bytes.Reader
	at     int64
	err    error
	failed bool
}

func (f *failOnce) Read(p []byte) (int, error) {
	pos := f.r.Size() - int64(f.r.Len())
	if !f.failed && pos == f.at {
		f.failed = true
		return 0, f.err
	}
	if !f.failed {
		p = p[:min(int64(len(p)), f.at-pos)]
	}
	return f.r.Read(p)
}

func TestAFailedReadThatWrapsEOFFails(t *testing.T) {
	// a provider read whose connection closed before any answer wraps io.EOF
	lost := fmt.Errorf("read of the archive: %w", io.EOF)
	archive := newFixture(t).file("etc/first", "x").file("etc/stray", gateKey).archive()
	// The second header starts after the first header and its file's padded block
	r := &failOnce{r: archive, at: 2 * 512, err: lost}
	f, err := Scan(r, Needles{{"gate key", []byte(gateKey)}}, Base(false))
	if !errors.Is(err, lost) {
		t.Fatalf("a read failing at a header with %v gave %v, %v, want that failure", lost, f,
			err)
	}
}

func TestValuesNeverReachOutput(t *testing.T) {
	needles := values()
	res, err := ScanFull(fixtureWithEachNeedleInEachEncoding(t), needles, Base(false))
	if err != nil {
		t.Fatal(err)
	}
	var out strings.Builder
	t.Log("findings:", len(res.Findings))
	for _, f := range res.Findings {
		fmt.Fprintf(&out, "%v %+v %s\n", f, f, f)
	}
	for _, n := range needles {
		for enc, text := range inEachEncoding(n.Value) {
			if strings.Contains(out.String(), text) {
				t.Errorf("the output holds %s of %s", enc, n.Name)
			}
		}
	}
}

// secrets.env, profile.d, a job's env and an SSH session's env each hold it
func TestPlaceholderPlaces(t *testing.T) {
	value, ph := "bnleak_0123456789abcdef", "bnsec_abcdefghijklmnopqrstuvwxyz"
	good := []Place{{"secrets.env", []byte("T=" + ph)}, {"profile.d", []byte("export T=" + ph)},
		{"job env", []byte("T=" + ph)}, {"ssh env", []byte("T=" + ph)}}
	clean := []Place{{"/etc/environment", []byte("PATH=/usr/bin\nLANG=C\n")}}
	eq(t, CheckPlaces(good, clean, ph, value), []string(nil))
	bad := []Place{{"secrets.env", []byte("T=" + value)}, {"ssh env", []byte("T=")}}
	eq(t, CheckPlaces(bad, clean, ph, value), []string{"secrets.env lacks the placeholder",
		"secrets.env holds the value", "ssh env lacks the placeholder"})
	for _, dirty := range []string{"T=" + ph, "T=" + value, "BUGNET_X=1", "BUGNET_LABELS=a"} {
		got := CheckPlaces(good, []Place{{"/etc/environment", []byte(dirty)}}, ph, value)
		if len(got) == 0 {
			t.Errorf("/etc/environment with %q passed", dirty[:2])
		}
		for _, line := range got {
			if strings.Contains(line, value) || strings.Contains(line, ph) {
				t.Errorf("a report quotes a value: %q", line)
			}
		}
	}
}

func TestCollectScriptShape(t *testing.T) {
	for _, want := range []string{"/proc/[0-9]*", "ps -eo pid,user,args",
		"systemctl show -p Environment",
		"169.254.169.254", "fd00:ec2::254", "-size -1025k", "-c 67108864", "var/lib/cloud"} {
		if !strings.Contains(Collect, want) {
			t.Errorf("collect.sh lacks %q", want)
		}
	}
	if strings.Contains(Collect, "BUGNET_") || strings.Contains(Collect, "_KEY") {
		t.Error("collect.sh names a secret")
	}
	eq(t, ArchivePath, "/tmp/leak/archive.tar")
}
