package live

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/bugnet-sh/bugnet-run/frame"

	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/image"
	"github.com/bugnet-sh/bugnet-router/internal/imagebuild"
	"github.com/bugnet-sh/bugnet-router/internal/leakscan"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/boxd"
	"github.com/bugnet-sh/bugnet-router/internal/provider/livetest"
	"github.com/bugnet-sh/bugnet-router/internal/providers"
	"github.com/bugnet-sh/bugnet-router/internal/saga"
)

// imageRows are CP 19.15.3's rows, IMG1 to IMG10 on every provider and one set for each of them
var imageRows []row

// TestLiveImage runs the base image rows against the artifacts the build record of the current
// version names, through scripts/live-test.sh --suite image. It starts no router stack.
func TestLiveImage(t *testing.T) {
	runRows(t, imageRows)
}

func init() {
	all := func(id string, run func(*testing.T, *imageEnv)) row {
		return row{id: id, noStack: true, run: imageRow(run)}
	}
	only := func(id string, run func(*testing.T, *imageEnv), names ...string) row {
		return row{id: id, providers: names, noStack: true, run: imageRow(run)}
	}
	imageRows = append(imageRows,
		all("IMG1", rowImageFresh), all("IMG2", rowImageRunner), all("IMG3", rowImageCA),
		all("IMG4", rowImageNoStockSSHD), all("IMG5", rowImageRandomness),
		all("IMG6", rowImageScan), all("IMG7", rowImageStartTimes), all("IMG8", rowImageDisk),
		all("IMG9", rowImageNoApt), all("IMG10", rowImageLocale),
		only("IMG-E2B1", rowE2BFinish, "e2b"), only("IMG-E2B2", rowE2BStartTime, "e2b"),
		only("IMG-BOXD1", rowBoxdOCI, "boxd"), only("IMG-BOXD2", rowBoxdSnapshot, "boxd"),
		only("IMG-BOXD3", rowBoxdArchive, "boxd"), only("IMG-BOXD4", rowBoxdReseed, "boxd"),
		only("IMG-KOYEB1", rowKoyebImage, "koyeb"), only("IMG-KOYEB2", rowKoyebPull, "koyeb"),
		only("IMG-BOAT1", rowBoatFrom, "boat"), only("IMG-BOAT2", rowBoatScopes, "boat"),
		only("IMG-BOAT3", rowBoatTools, "boat"), only("IMG-BOAT4", rowBoatHydration, "boat"),
	)
}

func TestImageRowsAndTheirProviders(t *testing.T) {
	var ids []string
	for _, r := range imageRows {
		ids = append(ids, r.id)
		eq(t, r.noStack, true)
	}
	for i := 1; i <= 10; i++ {
		eq(t, slices.Contains(ids, fmt.Sprintf("IMG%d", i)), true)
	}
	for _, r := range imageRows {
		switch {
		case strings.HasPrefix(r.id, "IMG-E2B"):
			eq(t, r.providers, []string{"e2b"})
		case strings.HasPrefix(r.id, "IMG-BOXD"):
			eq(t, r.providers, []string{"boxd"})
		case strings.HasPrefix(r.id, "IMG-KOYEB"):
			eq(t, r.providers, []string{"koyeb"})
		case strings.HasPrefix(r.id, "IMG-BOAT"):
			eq(t, r.providers, []string{"boat"})
		}
	}
	eq(t, len(imageRows), 22)
}

/* The build record and the rows' environment */

// buildRecord is deploy/image/builds/<channel>-<version>.json as scripts/image.sh keeps it
type buildRecord struct {
	Version string `json:"version"`
	Channel string `json:"channel"`
	OCI     struct {
		Ref string `json:"ref"`
	} `json:"oci"`
	E2B struct {
		Templates []struct {
			Name string `json:"name"`
			Size string `json:"size"`
			ID   string `json:"id"`
		} `json:"templates"`
		Tools map[string]string `json:"tools"`
	} `json:"e2b"`
	Boxd struct {
		Refs  map[string]string `json:"refs"`
		Tools map[string]string `json:"tools"`
	} `json:"boxd"`
	Koyeb struct {
		Ref   string            `json:"ref"`
		Tools map[string]string `json:"tools"`
	} `json:"koyeb"`
	Boat struct {
		Ref   string            `json:"ref"`
		Tools map[string]string `json:"tools"`
	} `json:"boat"`
}

// imageEnv is what one image row needs, a provider on its own image beside one on ours
type imageEnv struct {
	name    string
	cfg     config.Config
	ours    provider.Provider
	own     provider.Provider
	rec     buildRecord
	sizes   []provider.Size
	needles leakscan.Needles
	boot    imagebuild.BootFunc
}

// imageRow runs a row on the provider its test name ends with
func imageRow(run func(*testing.T, *imageEnv)) func(*testing.T, *stack) {
	return func(t *testing.T, _ *stack) {
		run(t, newImageEnv(t, path.Base(t.Name())))
	}
}

func newImageEnv(t *testing.T, name string) *imageEnv {
	t.Helper()
	cfg := livetest.Settings(t)
	root := repo(t)
	raw, err := os.ReadFile(filepath.Join(root, "deploy", "image", "VERSION"))
	if err != nil {
		t.Fatal(err)
	}
	version := strings.TrimSpace(string(raw))
	recPath := filepath.Join(root, "deploy", "image", "builds",
		cfg.Images.Channel+"-"+version+".json")
	raw, err = os.ReadFile(recPath)
	if err != nil {
		t.Fatalf("the build record of %s is missing, run scripts/image.sh build %s: %v",
			version, name, err)
	}
	e := &imageEnv{name: name, cfg: cfg}
	if err := json.Unmarshal(raw, &e.rec); err != nil {
		t.Fatalf("%s: %v", recPath, err)
	}
	e.ours, e.own = liveImageAdapter(t, cfg, name, true), liveImageAdapter(t, cfg, name, false)
	sz, ok := e.ours.(provider.Sizer)
	if !ok {
		t.Fatalf("%s lists no sizes", name)
	}
	e.sizes = sz.Sizes()
	if e.needles, err = leakscan.FromEnv(os.Environ()); err != nil {
		t.Fatal(err)
	}
	cfg.Secrets.Providers = nil
	boot, err := saga.LoadBootSettings(slog.New(slog.NewTextHandler(io.Discard, nil)), cfg)
	if err != nil {
		t.Fatal(err)
	}
	e.boot = saga.CheckBoot(boot, saga.SSHSettingsOf(cfg))
	return e
}

// liveImageAdapter builds the provider with its runtime key, on its own image unless ours
func liveImageAdapter(t *testing.T, cfg config.Config, name string, ours bool) provider.Provider {
	t.Helper()
	env, ok := config.ProviderKeyEnv(name)
	if !ok || os.Getenv(env) == "" {
		t.Fatalf("%s is not set, so the %s image rows cannot run", env, name)
	}
	cfg.Providers = []string{name}
	cfg.ProviderKeys = map[string]string{name: os.Getenv(env)}
	if !ours {
		cfg.Images = config.Images{Channel: cfg.Images.Channel}
	}
	reg, err := providers.Build(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	p, _ := reg.Get(name)
	return p
}

// artifact is the ref a create of size starts from
func (e *imageEnv) artifact(t *testing.T, size string) string {
	t.Helper()
	var ref string
	switch e.name {
	case "e2b":
		for _, tpl := range e.rec.E2B.Templates {
			if tpl.Size == size {
				ref = tpl.ID
			}
		}
	case "boxd":
		ref = e.rec.Boxd.Refs[size]
	case "koyeb":
		ref = e.rec.Koyeb.Ref
	case "boat":
		ref = e.rec.Boat.Ref
	}
	if ref == "" {
		t.Fatalf("the build record names no %s artifact for size %s", e.name, size)
	}
	return ref
}

// smallest is the smallest size the record names an artifact for
func (e *imageEnv) smallest(t *testing.T) provider.Size {
	t.Helper()
	for _, s := range e.sizes {
		switch e.name {
		case "e2b":
			for _, tpl := range e.rec.E2B.Templates {
				if tpl.Size == s.Name {
					return s
				}
			}
		case "boxd":
			if e.rec.Boxd.Refs[s.Name] != "" {
				return s
			}
		default:
			return s
		}
	}
	t.Fatalf("the build record names no %s artifact for any size", e.name)
	return provider.Size{}
}

// tools is the record's tools for the provider
func (e *imageEnv) tools() map[string]string {
	switch e.name {
	case "e2b":
		return e.rec.E2B.Tools
	case "boxd":
		return e.rec.Boxd.Tools
	case "koyeb":
		return e.rec.Koyeb.Tools
	}
	return e.rec.Boat.Tools
}

// start creates a sandbox of size from image on p and waits for it, destroying it at the end
func start(
	t *testing.T, p provider.Provider, size provider.Size, imageRef string,
) (provider.Ref, time.Duration) {
	t.Helper()
	began := time.Now()
	ref, err := p.Create(t.Context(), provider.CreateRequest{
		Size: size, Image: imageRef, Lifetime: 20 * time.Minute,
		Labels: map[string]string{"bugnet_image_check": "1"},
	})
	if err != nil {
		t.Fatalf("create from %q: %v", imageRef, err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 30*time.Second)
		defer cancel()
		if err := p.Destroy(ctx, ref); err != nil {
			t.Errorf("destroy %s: %v", ref.ID, err)
		}
	})
	if err := p.WaitReady(t.Context(), ref); err != nil {
		t.Fatalf("wait ready: %v", err)
	}
	return ref, time.Since(began)
}

// sandbox starts a sandbox of the smallest size from our artifact
func (e *imageEnv) sandbox(t *testing.T) (provider.Ref, time.Duration) {
	t.Helper()
	size := e.smallest(t)
	return start(t, e.ours, size, e.artifact(t, size.Name))
}

// sh runs a shell command as the exec user and returns its output and exit code
func (e *imageEnv) sh(
	t *testing.T, ref provider.Ref, script string,
) (string, int) {
	t.Helper()
	return e.exec(t, e.ours, ref, []string{"sh", "-c", script})
}

// asRoot runs a shell command as root, through sudo where the exec user is not root
func (e *imageEnv) asRoot(t *testing.T, ref provider.Ref, script string) (string, int) {
	t.Helper()
	argv := []string{"sh", "-c", script}
	if u := e.ours.Capabilities().ExecUser; u != "" && u != "root" {
		argv = append([]string{"sudo", "-n"}, argv...)
	}
	return e.exec(t, e.ours, ref, argv)
}

func (e *imageEnv) exec(
	t *testing.T, p provider.Provider, ref provider.Ref, argv []string,
) (string, int) {
	t.Helper()
	st, err := p.Exec(t.Context(), ref, provider.ExecSpec{Argv: argv})
	if err != nil {
		t.Fatalf("exec %q: %v", argv[0], err)
	}
	defer func() { _ = st.Close() }()
	out, err := io.ReadAll(io.LimitReader(st, 1<<20))
	if err != nil {
		t.Fatal(err)
	}
	code, err := st.Wait()
	if err != nil {
		t.Fatal(err)
	}
	return string(out), code
}

// job runs script through bugnet-run start, as a job, and returns its output and exit code
func (e *imageEnv) job(t *testing.T, ref provider.Ref, id, script string) (string, int) {
	t.Helper()
	argv := []string{image.RunnerPath, "start", "--job", id, "--", "sh", "-c", script}
	out, _ := e.exec(t, e.ours, ref, argv)
	var data bytes.Buffer
	code := -1
	sc := bufio.NewScanner(strings.NewReader(out))
	sc.Buffer(nil, 1<<20)
	for sc.Scan() {
		var f frame.Frame
		if json.Unmarshal(sc.Bytes(), &f) != nil {
			continue
		}
		switch f.Kind {
		case frame.KindStdout:
			data.Write(f.Data)
		case frame.KindExit:
			code = f.Code
		}
	}
	return data.String(), code
}

// percentiles reports the p50 and p90 of ms
func percentiles(ms []int) (p50, p90 int) {
	s := slices.Clone(ms)
	slices.Sort(s)
	if len(s) == 0 {
		return 0, 0
	}
	return s[len(s)/2], s[min(len(s)-1, len(s)*9/10)]
}

/* IMG1 to IMG10 */

// rowImageFresh is IMG1, a fresh sandbox's manifest and the tools a job sees against the record
func rowImageFresh(t *testing.T, e *imageEnv) {
	ref, _ := e.sandbox(t)
	m, ok, err := image.Read(t.Context(), e.ours, ref)
	if err != nil || !ok {
		t.Fatalf("manifest: ok %v, %v", ok, err)
	}
	target := map[string]string{"e2b": "e2b", "boxd": "boxd", "boat": "boat", "koyeb": "oci"}
	if m.Version != e.rec.Version || m.Channel != e.rec.Channel || m.Target != target[e.name] {
		t.Errorf("manifest %s/%s/%s, record %s/%s", m.Version, m.Channel, m.Target,
			e.rec.Version, e.rec.Channel)
	}
	for tool, want := range e.tools() {
		if got := m.Tools[tool]; got != want {
			t.Errorf("manifest tool %s is %q, the record says %q", tool, got, want)
		}
	}
	out, code := e.job(t, ref, "img1", path.Dir(image.RunnerPath)+"/setup-image.sh probe")
	if code != 0 {
		t.Fatalf("the tool probe exited %d", code)
	}
	var seen map[string]string
	if err := json.Unmarshal(bytes.TrimSpace([]byte(out)), &seen); err != nil {
		t.Fatalf("the probe printed %q: %v", out, err)
	}
	for tool, want := range m.Tools {
		if seen[tool] != want {
			t.Errorf("a job sees %s %q, the manifest says %q", tool, seen[tool], want)
		}
	}
	jobPath, _ := e.job(t, ref, "img1p", "echo $PATH")
	t.Logf("a job's PATH on %s: %s", e.name, strings.TrimSpace(jobPath))
}

// rowImageRunner is IMG2, the baked runner answers with nothing uploaded
func rowImageRunner(t *testing.T, e *imageEnv) {
	ref, _ := e.sandbox(t)
	m, _, err := image.Read(t.Context(), e.ours, ref)
	if err != nil {
		t.Fatal(err)
	}
	if raw, err := os.ReadFile(e.cfg.RunnerLinuxAMD64); err == nil {
		sum := sha256.Sum256(raw)
		if m.RunnerSHA256["amd64"] != hex.EncodeToString(sum[:]) {
			t.Logf("the baked runner differs from runner.linux_amd64, the saga uploads its own")
		}
	}
	if _, code := e.exec(t, e.ours, ref, []string{image.RunnerPath, "ping"}); code != 0 {
		t.Fatalf("the baked runner's ping exited %d", code)
	}
	if _, _, err := e.ours.ReadFile(t.Context(), ref, image.UploadedRunnerPath, 0, 1); err == nil {
		t.Errorf("a runner sits at %s before any upload", image.UploadedRunnerPath)
	}
	if out, code := e.job(t, ref, "img2", "echo ok"); code != 0 || strings.TrimSpace(out) != "ok" {
		t.Errorf("a job gave %q with exit %d", out, code)
	}
}

// rowImageCA is IMG3, setup with only ca_sha256 answers checked and the system trusts the CA
func rowImageCA(t *testing.T, e *imageEnv) {
	if e.cfg.Secrets.CACertFile == "" {
		t.Fatal("secrets.ca_cert_file names no dev CA, so IMG3 cannot run")
	}
	pem, err := os.ReadFile(e.cfg.Secrets.CACertFile)
	if err != nil {
		t.Fatal(err)
	}
	ref, _ := e.sandbox(t)
	sum := sha256.Sum256(pem)
	spec := fmt.Sprintf(`{"v":1,"gate_key":"%s","gate_listen":":%d","deny_ports":[],`+
		`"ca_sha256":"%s","ca_version":%d,"sample":false,"seed":"%s"}`,
		strings.Repeat("k", 43), provider.DefaultGatePort, hex.EncodeToString(sum[:]),
		e.cfg.Secrets.CAVersion, strings.Repeat("QUFB", 11)[:44])
	if err := e.ours.WriteFile(t.Context(), ref, "/tmp/bugnet-setup.json", []byte(spec),
		0o600); err != nil {
		t.Fatal(err)
	}
	if err := e.ours.WriteFile(t.Context(), ref, "/tmp/bugnet-ca.pem", pem, 0o644); err != nil {
		t.Fatal(err)
	}
	out, code := e.asRoot(t, ref, image.RunnerPath+" setup --spec /tmp/bugnet-setup.json")
	if code != 0 || !strings.Contains(out, `"ca":"checked"`) {
		t.Fatalf("setup exited %d with %q, want ca checked", code, out)
	}
	if out, code := e.sh(t, ref,
		"openssl verify -CApath /etc/ssl/certs /tmp/bugnet-ca.pem"); code != 0 {
		t.Errorf("the system does not trust the dev CA: %s", out)
	}
}

// rowImageNoStockSSHD is IMG4, no stock sshd listens, but Boat's, and its units are masked
func rowImageNoStockSSHD(t *testing.T, e *imageEnv) {
	ref, _ := e.sandbox(t)
	out, _ := e.asRoot(t, ref, "ss -ltnp 2>/dev/null | grep -c ':22 ' || true")
	listening := strings.TrimSpace(out) != "0"
	switch {
	case e.name == "boat" && !listening:
		t.Errorf("boat's own sshd is not listening")
	case e.name != "boat" && listening:
		t.Errorf("a stock sshd listens on port 22")
	}
	if e.name == "koyeb" {
		return
	}
	enabled, _ := e.asRoot(t, ref, "systemctl is-enabled ssh ssh.socket 2>&1 || true")
	if e.name != "boat" && !strings.Contains(enabled, "masked") {
		t.Errorf("the ssh units are not masked: %q", enabled)
	}
}

// rowImageRandomness is IMG5, two sandboxes after setup differ and the reseed is recorded
func rowImageRandomness(t *testing.T, e *imageEnv) {
	size := e.smallest(t)
	rep, err := imagebuild.Check(t.Context(), e.ours, e.boot, e.artifact(t, size.Name), size,
		e.needles)
	if err != nil {
		t.Fatal(err)
	}
	if rep.SameRand {
		t.Error("two sandboxes read the same random bytes or got the same host key")
	}
	if slices.Contains(rep.Reseed, "") {
		t.Errorf("a sandbox's setup reported no reseed: %v", rep.Reseed)
	}
	t.Logf("%s reseed: %v", e.name, rep.Reseed)
}

// rowImageScan is IMG6, the image check of CP 19.9 before any start step finds nothing
func rowImageScan(t *testing.T, e *imageEnv) {
	size := e.smallest(t)
	rep, err := imagebuild.Check(t.Context(), e.ours, e.boot, e.artifact(t, size.Name), size,
		e.needles)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range rep.Problems() {
		t.Error(p)
	}
}

// rowImageStartTimes is IMG7, ten starts on the provider's image and ten on ours, per step
func rowImageStartTimes(t *testing.T, e *imageEnv) {
	const starts = 10
	size := e.smallest(t)
	artifact := e.artifact(t, size.Name)
	var runner []byte
	if raw, err := os.ReadFile(e.cfg.RunnerLinuxAMD64); err == nil {
		runner = raw
	}
	measure := func(p provider.Provider, from string) (ready, runnerMS []int) {
		for range starts {
			ref, d := start(t, p, size, from)
			ready = append(ready, int(d.Milliseconds()))
			began := time.Now()
			bin := "/usr/local/lib/bugnet/bugnet-run"
			if from == "" {
				bin = "/tmp/bugnet/bin/bugnet-run"
				if len(runner) > 0 {
					_ = p.WriteFile(t.Context(), ref, bin, runner, 0o755)
				}
			}
			_, _ = e.exec(t, p, ref, []string{bin, "ping"})
			runnerMS = append(runnerMS, int(time.Since(began).Milliseconds()))
		}
		return ready, runnerMS
	}
	for _, c := range []struct {
		label string
		p     provider.Provider
		image string
	}{{"provider's image", e.own, ""}, {"our image", e.ours, artifact}} {
		ready, run := measure(c.p, c.image)
		r50, r90 := percentiles(ready)
		u50, u90 := percentiles(run)
		t.Logf("IMG7 %s on %s: ready p50 %d ms p90 %d ms, runner step p50 %d ms p90 %d ms",
			c.label, e.name, r50, r90, u50, u90)
	}
}

// rowImageDisk is IMG8, the free bytes on / of each size
func rowImageDisk(t *testing.T, e *imageEnv) {
	for _, size := range e.sizes {
		ref, _ := start(t, e.ours, size, e.artifact(t, size.Name))
		out, code := e.sh(t, ref, "df -B1 --output=avail / | tail -n 1")
		if code != 0 {
			t.Fatalf("df exited %d", code)
		}
		t.Logf("IMG8 %s size %s: %s free bytes on / (size says %d GiB)", e.name, size.Name,
			strings.TrimSpace(out), size.Resources.DiskGiB)
	}
}

// rowImageNoApt is IMG9, harden and ssh-up run no apt-get
func rowImageNoApt(t *testing.T, e *imageEnv) {
	ref, _ := e.sandbox(t)
	count := func() string {
		out, _ := e.asRoot(t, ref,
			"grep -c Start-Date /var/log/apt/history.log 2>/dev/null || echo 0")
		return strings.TrimSpace(out)
	}
	before := count()
	booted, err := e.boot(t.Context(), e.ours, ref)
	if err != nil {
		t.Fatalf("the start steps: %v", err)
	}
	spec := `{"v":1,"mail_ports":[25],"deny_ranges":[],"pools":[],"allow":[]}`
	if err := e.ours.WriteFile(t.Context(), booted, "/tmp/bugnet-harden.json", []byte(spec),
		0o600); err != nil {
		t.Fatal(err)
	}
	if out, code := e.asRoot(t, booted,
		image.RunnerPath+" harden --spec /tmp/bugnet-harden.json"); code != 0 {
		t.Fatalf("harden exited %d: %s", code, out)
	}
	if after := count(); after != before {
		t.Errorf("apt ran %s times before and %s after harden and ssh-up", before, after)
	}
}

// rowImageLocale is IMG10, column -t takes a UTF-8 line in a job and sshd sets the locale
func rowImageLocale(t *testing.T, e *imageEnv) {
	ref, _ := e.sandbox(t)
	if out, code := e.job(t, ref, "img10", `printf 'é\n' | column -t`); code != 0 {
		t.Errorf("column -t exited %d in a job: %q", code, out)
	}
	booted, err := e.boot(t.Context(), e.ours, ref)
	if err != nil {
		t.Fatalf("the start steps: %v", err)
	}
	out, _ := e.asRoot(t, booted, "sshd -T 2>/dev/null | grep -i '^setenv' || true")
	if !strings.Contains(out, "LANG=C.UTF-8") {
		t.Errorf("sshd does not set LANG for a session, setenv reads %q", out)
	}
}

/* Per provider */

// rowE2BFinish is IMG-E2B1, the template built from our digest has its sshd masked after E2B's
func rowE2BFinish(t *testing.T, e *imageEnv) {
	rowImageNoStockSSHD(t, e)
}

// rowE2BStartTime is IMG-E2B2, our template's start time beside the account's own template
func rowE2BStartTime(t *testing.T, e *imageEnv) {
	size := e.smallest(t)
	for _, c := range []struct {
		label string
		p     provider.Provider
		image string
	}{{"own template", e.own, ""}, {"our template", e.ours, e.artifact(t, size.Name)}} {
		var ms []int
		for range 5 {
			_, d := start(t, c.p, size, c.image)
			ms = append(ms, int(d.Milliseconds()))
		}
		p50, p90 := percentiles(ms)
		t.Logf("IMG-E2B2 %s: ready p50 %d ms p90 %d ms", c.label, p50, p90)
	}
}

// rowBoxdOCI is IMG-BOXD1, whether boxd boots our digest as image_ref, and its own image's OS
func rowBoxdOCI(t *testing.T, e *imageEnv) {
	size := e.smallest(t)
	builder := func(img string) (string, error) {
		ref, err := e.own.Create(t.Context(), provider.CreateRequest{
			Size: size, Builder: true, Image: img, Lifetime: 20 * time.Minute,
		})
		if err != nil {
			return "", err
		}
		defer func() {
			ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 30*time.Second)
			defer cancel()
			_ = e.own.Destroy(ctx, ref)
		}()
		if err := e.own.WaitReady(t.Context(), ref); err != nil {
			return "", err
		}
		out, _ := e.exec(t, e.own, ref, []string{"cat", "/etc/os-release"})
		return out, nil
	}
	own, err := builder("")
	if err != nil {
		t.Fatalf("a builder from boxd's default image: %v", err)
	}
	t.Logf("IMG-BOXD1 boxd's default image: %s", strings.Join(strings.Fields(
		firstLine(own, "PRETTY_NAME")), " "))
	if e.rec.OCI.Ref == "" {
		t.Fatal("the build record names no OCI digest")
	}
	if _, err := builder("oci:" + e.rec.OCI.Ref); err != nil {
		t.Logf("IMG-BOXD1 decides: image_ref refused our digest, builders start from boxd's "+
			"default image (%v)", err)
		return
	}
	t.Log("IMG-BOXD1 decides: image_ref boots our digest, run image build with --from-oci")
}

func firstLine(text, prefix string) string {
	for _, l := range strings.Split(text, "\n") {
		if strings.HasPrefix(l, prefix) {
			return l
		}
	}
	return ""
}

// rowBoxdSnapshot is IMG-BOXD2, a create from the snapshot keeps its labels and its timer
func rowBoxdSnapshot(t *testing.T, e *imageEnv) {
	size := e.smallest(t)
	labels := map[string]string{"bugnet_image_check": "1", "image_row": "boxd2"}
	ref, err := e.ours.Create(t.Context(), provider.CreateRequest{
		Size: size, Image: e.artifact(t, size.Name), Labels: labels, Lifetime: 20 * time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 30*time.Second)
		defer cancel()
		_ = e.ours.Destroy(ctx, ref)
	})
	if err := e.ours.WaitReady(t.Context(), ref); err != nil {
		t.Fatal(err)
	}
	owned, err := e.ours.ListOwned(t.Context(), labels)
	if err != nil || !slices.ContainsFunc(owned, func(s provider.Summary) bool {
		return s.Ref.ID == ref.ID
	}) {
		t.Errorf("the restore is not listed under its own labels: %v, %v", owned, err)
	}
	st, err := e.ours.Inspect(t.Context(), ref)
	if err != nil {
		t.Fatal(err)
	}
	if st.ExpiresAt.IsZero() {
		t.Error("the restore has no delete-after timer")
	}
	t.Logf("IMG-BOXD2 restore expires at %s", st.ExpiresAt.Format(time.RFC3339))
}

// rowBoxdArchive is IMG-BOXD3, where a snapshot's bytes are and whether a create still works
func rowBoxdArchive(t *testing.T, e *imageEnv) {
	b, ok := e.ours.(*boxd.Provider)
	if !ok {
		t.Fatal("the boxd adapter has an unexpected type")
	}
	for size, name := range e.rec.Boxd.Refs {
		storage, err := b.SnapshotStorage(t.Context(), name)
		if err != nil {
			t.Errorf("snapshot %s: %v", name, err)
			continue
		}
		t.Logf("IMG-BOXD3 snapshot %s of size %s is stored %s, rerun a day after the build",
			name, size, storage)
	}
	ref, _ := e.sandbox(t)
	t.Logf("IMG-BOXD3 a create from the smallest snapshot worked: %s", ref.ID)
}

// rowBoxdReseed is IMG-BOXD4, setup's reseed inside a restored machine is an ioctl
func rowBoxdReseed(t *testing.T, e *imageEnv) {
	size := e.smallest(t)
	ref, _ := start(t, e.ours, size, e.artifact(t, size.Name))
	booted, err := e.boot(t.Context(), e.ours, ref)
	if err != nil {
		t.Fatal(err)
	}
	if got := booted.Meta[saga.MetaReseed]; got != "ioctl" {
		t.Errorf("RNDRESEEDCRNG answered %q, want ioctl", got)
	}
}

// rowKoyebImage is IMG-KOYEB1, a SANDBOX service from our image takes exec, files and bind_port
func rowKoyebImage(t *testing.T, e *imageEnv) {
	ref, _ := e.sandbox(t)
	if out, code := e.sh(t, ref, "echo koyeb"); code != 0 || strings.TrimSpace(out) != "koyeb" {
		t.Fatalf("exec gave %q with exit %d", out, code)
	}
	const probe = "/tmp/img-koyeb1"
	if err := e.ours.WriteFile(t.Context(), ref, probe, []byte("x"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	got, _, err := e.ours.ReadFile(t.Context(), ref, probe, 0, 8)
	if err != nil || string(got) != "x" {
		t.Fatalf("read: %q, %v", got, err)
	}
	g, ok := e.ours.(provider.Gater)
	if !ok {
		t.Fatal("koyeb has no gate route")
	}
	if _, err := g.PrepareGate(t.Context(), ref, provider.DefaultGatePort); err != nil {
		t.Errorf("bind_port: %v", err)
	}
}

// rowKoyebPull is IMG-KOYEB2, ready time of our image beside koyeb/sandbox, first pull and warm
func rowKoyebPull(t *testing.T, e *imageEnv) {
	size := e.smallest(t)
	var ours, stock []int
	for range 3 {
		_, d := start(t, e.ours, size, e.artifact(t, size.Name))
		ours = append(ours, int(d.Milliseconds()))
		_, d = start(t, e.own, size, "")
		stock = append(stock, int(d.Milliseconds()))
	}
	o50, _ := percentiles(ours)
	s50, _ := percentiles(stock)
	t.Logf("IMG-KOYEB2 ready p50 %d ms on our image, %d ms on koyeb/sandbox, first start %d ms "+
		"against %d ms", o50, s50, ours[0], stock[0])
	if o50-s50 > 2000 {
		t.Logf("IMG-KOYEB2 our image adds %d ms at p50, over 2 s, so CP question 25 applies",
			o50-s50)
	}
}

// rowBoatFrom is IMG-BOAT1, a create from the saved snapshot on every size, with the settings
// the adapter sends
func rowBoatFrom(t *testing.T, e *imageEnv) {
	for _, size := range e.sizes {
		_, d := start(t, e.ours, size, e.artifact(t, size.Name))
		t.Logf("IMG-BOAT1 size %s deployed from the saved snapshot in %d ms", size.Name,
			d.Milliseconds())
	}
}

// rowBoatScopes is IMG-BOAT2, whether the runtime key alone may deploy from the snapshot
func rowBoatScopes(t *testing.T, e *imageEnv) {
	size := e.smallest(t)
	ref, err := e.ours.Create(t.Context(), provider.CreateRequest{
		Size: size, Image: e.artifact(t, size.Name), Lifetime: 10 * time.Minute,
		Labels: map[string]string{"bugnet_image_check": "1"},
	})
	if err != nil {
		if pe, ok := provider.AsError(err); ok && pe.Code == provider.CodeAuth {
			t.Fatalf("IMG-BOAT2 decides: the runtime key needs snapshot.read, CP question 23: %v",
				err)
		}
		t.Fatal(err)
	}
	_ = e.ours.Destroy(t.Context(), ref)
	t.Log("IMG-BOAT2 decides: the runtime key deploys from the saved snapshot with no added scope")
}

// rowBoatTools is IMG-BOAT3, the node a job runs and whether Boat's sshd keys differ per sandbox
func rowBoatTools(t *testing.T, e *imageEnv) {
	var keys []string
	for range 2 {
		ref, _ := e.sandbox(t)
		out, code := e.job(t, ref, "boat3", "node --version")
		if code != 0 {
			t.Fatalf("node --version exited %d", code)
		}
		t.Logf("IMG-BOAT3 a job runs node %s, the record pins %s", strings.TrimSpace(out),
			e.tools()["node"])
		fp, _ := e.asRoot(t, ref, "ssh-keygen -lf /etc/ssh/ssh_host_ed25519_key.pub")
		keys = append(keys, strings.TrimSpace(fp))
	}
	if keys[0] != "" && keys[0] == keys[1] {
		t.Error("IMG-BOAT3 decides: Boat's sshd host key is the same in both sandboxes, so " +
			"build --target boat must delete the stock keys")
	}
}

// rowBoatHydration is IMG-BOAT4, ready and first-command time from the snapshot, then plain
func rowBoatHydration(t *testing.T, e *imageEnv) {
	size := e.smallest(t)
	for _, c := range []struct {
		label string
		p     provider.Provider
		image string
	}{{"plain create", e.own, ""}, {"saved snapshot", e.ours, e.artifact(t, size.Name)}} {
		var ready, first []int
		for range 3 {
			ref, d := start(t, c.p, size, c.image)
			ready = append(ready, int(d.Milliseconds()))
			began := time.Now()
			_, _ = e.exec(t, c.p, ref, []string{"true"})
			first = append(first, int(time.Since(began).Milliseconds()))
		}
		r50, _ := percentiles(ready)
		f50, _ := percentiles(first)
		t.Logf("IMG-BOAT4 %s: ready p50 %d ms, first command p50 %d ms", c.label, r50, f50)
	}
}
