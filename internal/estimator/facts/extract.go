package facts

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math/big"
	"path"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/pelletier/go-toml/v2"
	"github.com/tailscale/hujson"
	"go.yaml.in/yaml/v3"
)

const (
	// maxDepth bounds JSON and YAML nesting in an attacker's file
	maxDepth = 64
	// maxServices bounds the compose services kept
	maxServices = 32
	// maxCIServices bounds the CI service classes kept
	maxCIServices = 16
	// noTimeout is GitHub's default job timeout in minutes
	noTimeout = 360
)

// Extract distils a repository's signal files and its tree into facts, keyed by path from the
// source's root
func Extract(files map[string][]byte, tree []Blob) Facts {
	r := &Repo{}
	paths := make([]string, 0, len(files))
	for p := range files {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	var py pythonDeps
	for _, p := range paths {
		b := files[p]
		base := path.Base(p)
		switch {
		case isDevcontainer(p):
			r.Devcontainers++
			if h, ok := devcontainerHost(b); ok {
				r.Host = maxHost(r.Host, h)
			}
		case base == "Dockerfile" || strings.HasSuffix(base, ".Dockerfile"):
			r.Dockerfile = true
			r.CUDABase = r.CUDABase || cudaBase(b)
		case composeNames[base] && !strings.Contains(p, "/"):
			readCompose(r, b)
		case isWorkflow(p):
			readWorkflow(r, b)
		case base == ".gitattributes" && !strings.Contains(p, "/"):
			r.Tree.LFS = bytes.Contains(b, []byte("filter=lfs"))
		case base == "pyproject.toml":
			py.readPyproject(b)
		case base == "go.mod":
			readGoMod(r, b)
		case pinFiles[base] && !strings.Contains(p, "/"):
			readPin(r, base, b)
		}
		if kind := lockKind(base); kind != "" {
			readLockfile(r, &py, kind, b)
		}
		if size, ok := lfsPointer(b); ok {
			r.Tree.LFSPointers++
			r.Tree.LFSBytes += size
		}
	}
	r.ML = py.ml()
	r.Transformers4 = py.transformers4
	readTree(r, tree)
	f := Facts{Repo: r}
	f.Normalize()
	return f
}

/* Devcontainer */

func isDevcontainer(p string) bool {
	if p == ".devcontainer.json" || p == ".devcontainer/devcontainer.json" {
		return true
	}
	parts := strings.Split(p, "/")
	return len(parts) == 3 && parts[0] == ".devcontainer" && parts[2] == "devcontainer.json"
}

func devcontainerHost(b []byte) (Host, bool) {
	std, err := hujson.Standardize(b)
	if err != nil || !jsonDepthOK(std) {
		return Host{}, false
	}
	var dc struct {
		HostRequirements struct {
			CPUs    json.RawMessage `json:"cpus"`
			Memory  string          `json:"memory"`
			Storage string          `json:"storage"`
			GPU     json.RawMessage `json:"gpu"`
		} `json:"hostRequirements"`
	}
	if json.Unmarshal(std, &dc) != nil {
		return Host{}, false
	}
	hr := dc.HostRequirements
	var h Host
	var cpus int
	if json.Unmarshal(hr.CPUs, &cpus) == nil && cpus > 0 && cpus <= 1024 {
		h.CPUs = cpus
	}
	if n, ok := decimalBytes(hr.Memory); ok {
		h.MemoryMiB = int(ceilDiv(n, 1<<20))
	}
	if n, ok := decimalBytes(hr.Storage); ok {
		h.StorageGiB = int(ceilDiv(n, 1<<30))
	}
	var flag bool
	var word string
	var obj struct {
		Cores  int    `json:"cores"`
		Memory string `json:"memory"`
	}
	switch {
	case json.Unmarshal(hr.GPU, &flag) == nil:
		if flag {
			h.GPU = "required"
		}
	case json.Unmarshal(hr.GPU, &word) == nil:
		if word == "optional" {
			h.GPU = "optional"
		}
	case json.Unmarshal(hr.GPU, &obj) == nil:
		h.GPU = "required"
		if obj.Cores > 0 && obj.Cores <= 1<<20 {
			h.GPUCores = obj.Cores
		}
		if n, ok := decimalBytes(obj.Memory); ok {
			h.GPUMemoryGiB = int(ceilDiv(n, 1<<30))
		}
	}
	return h, h != Host{}
}

var decimalSize = regexp.MustCompile(`^(\d{1,9})(?:\.(\d{1,6}))?\s*(tb|gb|mb|kb)$`)

// decimalBytes reads a devcontainer size such as 8gb, where gb means 10^9 bytes
func decimalBytes(s string) (int64, bool) {
	m := decimalSize.FindStringSubmatch(strings.ToLower(strings.TrimSpace(s)))
	if m == nil {
		return 0, false
	}
	unit := map[string]int64{"kb": 1e3, "mb": 1e6, "gb": 1e9, "tb": 1e12}[m[3]]
	return ceilBytes(m[1], m[2], unit)
}

// ceilBytes is whole.frac units in bytes rounded up, refused past 1 PiB
func ceilBytes(whole, frac string, unit int64) (int64, bool) {
	r, ok := new(big.Rat).SetString(whole + "." + frac + "0")
	if !ok {
		return 0, false
	}
	r.Mul(r, new(big.Rat).SetInt64(unit))
	n := new(big.Int).Quo(r.Num(), r.Denom())
	if new(big.Rat).SetInt(n).Cmp(r) != 0 {
		n.Add(n, big.NewInt(1))
	}
	if !n.IsInt64() || n.Int64() <= 0 || n.Int64() > 1<<50 {
		return 0, false
	}
	return n.Int64(), true
}

func maxHost(a *Host, b Host) *Host {
	if a == nil {
		return &b
	}
	h := Host{
		CPUs: max(a.CPUs, b.CPUs), MemoryMiB: max(a.MemoryMiB, b.MemoryMiB),
		StorageGiB: max(a.StorageGiB, b.StorageGiB), GPUCores: max(a.GPUCores, b.GPUCores),
		GPUMemoryGiB: max(a.GPUMemoryGiB, b.GPUMemoryGiB), GPU: a.GPU,
	}
	if gpuRank[b.GPU] > gpuRank[a.GPU] {
		h.GPU = b.GPU
	}
	return &h
}

var gpuRank = map[string]int{"": 0, "optional": 1, "required": 2}

/* Dockerfile */

var fromLine = regexp.MustCompile(`(?im)^\s*FROM\s+(?:--\S+\s+)*(\S+)`)

func cudaBase(b []byte) bool {
	for _, m := range fromLine.FindAllSubmatch(b, 64) {
		img := strings.ToLower(string(m[1]))
		if strings.HasPrefix(img, "nvidia/cuda") || strings.HasPrefix(img, "nvcr.io/nvidia/") ||
			strings.Contains(img, "cuda") {
			return true
		}
	}
	return false
}

/* Compose */

var composeNames = map[string]bool{
	"compose.yaml": true, "compose.yml": true, "docker-compose.yaml": true,
	"docker-compose.yml": true,
}

func readCompose(r *Repo, b []byte) {
	root, ok := yamlRoot(b)
	if !ok {
		return
	}
	if r.Compose == nil {
		r.Compose = &Compose{}
	}
	r.Compose.Files++
	services := mapGet(root, "services")
	for _, name := range mapKeys(services) {
		if len(r.Compose.Services) >= maxServices {
			return
		}
		s := mapGet(services, name)
		svc := Service{Class: ImageClass(scalar(mapGet(s, "image")))}
		res := mapGet(mapGet(s, "deploy"), "resources")
		rsv, lim := mapGet(res, "reservations"), mapGet(res, "limits")
		svc.ReservedMilliCPU = milliCPU(scalar(mapGet(rsv, "cpus")))
		svc.ReservedMemoryMiB = composeMiB(scalar(mapGet(rsv, "memory")))
		if svc.ReservedMemoryMiB == 0 {
			svc.ReservedMemoryMiB = composeMiB(scalar(mapGet(s, "mem_reservation")))
		}
		svc.LimitMilliCPU = milliCPU(scalar(mapGet(lim, "cpus")))
		if svc.LimitMilliCPU == 0 {
			svc.LimitMilliCPU = milliCPU(scalar(mapGet(s, "cpus")))
		}
		svc.LimitMemoryMiB = composeMiB(scalar(mapGet(lim, "memory")))
		if svc.LimitMemoryMiB == 0 {
			svc.LimitMemoryMiB = composeMiB(scalar(mapGet(s, "mem_limit")))
		}
		svc.GPU = mapGet(s, "gpus") != nil || scalar(mapGet(s, "runtime")) == "nvidia" ||
			gpuDevices(mapGet(rsv, "devices"))
		r.Compose.Services = append(r.Compose.Services, svc)
	}
}

func gpuDevices(n *yaml.Node) bool {
	if n == nil || n.Kind != yaml.SequenceNode {
		return false
	}
	for _, d := range n.Content {
		caps := mapGet(d, "capabilities")
		if caps == nil {
			continue
		}
		for _, c := range caps.Content {
			if c.Value == "gpu" {
				return true
			}
		}
	}
	return false
}

var composeSize = regexp.MustCompile(`^(\d{1,12})(?:\.(\d{1,6}))?\s*([kmg]?)b?$`)

// composeMiB reads a compose byte value such as 512m or 1g, where the units are binary
func composeMiB(s string) int {
	m := composeSize.FindStringSubmatch(strings.ToLower(strings.TrimSpace(s)))
	if m == nil {
		return 0
	}
	unit := map[string]int64{"": 1, "k": 1 << 10, "m": 1 << 20, "g": 1 << 30}[m[3]]
	n, ok := ceilBytes(m[1], m[2], unit)
	if !ok {
		return 0
	}
	return int(ceilDiv(n, 1<<20))
}

var cpuValue = regexp.MustCompile(`^(\d{1,4})(?:\.(\d{1,3}))?$`)

func milliCPU(s string) int {
	m := cpuValue.FindStringSubmatch(strings.TrimSpace(s))
	if m == nil {
		return 0
	}
	whole, _ := strconv.Atoi(m[1])
	frac, _ := strconv.Atoi(m[2] + strings.Repeat("0", 3-len(m[2])))
	return whole*1000 + frac
}

var imageClasses = []struct{ key, class string }{
	{"postgis", "postgres"}, {"postgres", "postgres"}, {"mariadb", "mysql"}, {"mysql", "mysql"},
	{"valkey", "redis"}, {"redis", "redis"}, {"mongo", "mongo"}, {"rabbitmq", "rabbitmq"},
	{"minio", "minio"}, {"zookeeper", "zookeeper"}, {"kafka", "kafka"},
	{"redpanda", "kafka"}, {"elasticsearch", "elasticsearch"}, {"opensearch", "elasticsearch"},
	{"clickhouse", "clickhouse"},
}

// ImageClass maps an image reference to a known service class, other when none matches
func ImageClass(image string) string {
	name := strings.ToLower(image)
	if i := strings.IndexAny(name, "@"); i >= 0 {
		name = name[:i]
	}
	name = path.Base(name)
	if i := strings.Index(name, ":"); i >= 0 {
		name = name[:i]
	}
	for _, c := range imageClasses {
		if strings.Contains(name, c.key) {
			return c.class
		}
	}
	return "other"
}

/* Workflows */

func isWorkflow(p string) bool {
	dir, base := path.Split(p)
	return dir == ".github/workflows/" &&
		(strings.HasSuffix(base, ".yml") || strings.HasSuffix(base, ".yaml"))
}

var standardRunners = map[string]bool{
	"ubuntu-latest": true, "ubuntu-24.04": true, "ubuntu-22.04": true, "ubuntu-20.04": true,
}

var coreLabel = regexp.MustCompile(`(\d{1,3})-?cores?\b`)

type job struct {
	labels  []string
	timeout int
	images  []string
}

func readWorkflow(r *Repo, b []byte) {
	root, ok := yamlRoot(b)
	if !ok {
		return
	}
	jobsNode := mapGet(root, "jobs")
	var all, tests []job
	for _, id := range mapKeys(jobsNode) {
		n := mapGet(jobsNode, id)
		j := job{labels: runnerLabels(mapGet(n, "runs-on")), timeout: noTimeout}
		if t, err := strconv.Atoi(scalar(mapGet(n, "timeout-minutes"))); err == nil && t > 0 {
			j.timeout = t
		}
		svcs := mapGet(n, "services")
		for _, name := range mapKeys(svcs) {
			s := mapGet(svcs, name)
			img := scalar(s)
			if img == "" {
				img = scalar(mapGet(s, "image"))
			}
			j.images = append(j.images, img)
		}
		all = append(all, j)
		name := strings.ToLower(id + " " + scalar(mapGet(n, "name")))
		if strings.Contains(name, "test") {
			tests = append(tests, j)
		}
	}
	if len(all) == 0 {
		return
	}
	if len(tests) == 0 {
		tests = all
	}
	if r.CI == nil {
		r.CI = &CI{StandardOnly: true}
	}
	ci := r.CI
	ci.Jobs += len(all)
	timeout := 0
	for _, j := range tests {
		timeout = max(timeout, j.timeout)
		standard := len(j.labels) > 0
		for _, l := range j.labels {
			l = strings.ToLower(l)
			standard = standard && standardRunners[l]
			if m := coreLabel.FindStringSubmatch(l); m != nil {
				n, _ := strconv.Atoi(m[1])
				ci.LargestCores = max(ci.LargestCores, n)
			}
			ci.GPURunner = ci.GPURunner || strings.Contains(l, "gpu")
		}
		ci.StandardOnly = ci.StandardOnly && standard
		for _, img := range j.images {
			if len(ci.Services) < maxCIServices {
				ci.Services = append(ci.Services, ImageClass(img))
			}
		}
	}
	if timeout < noTimeout && (ci.TestTimeoutMinutes == 0 || timeout > ci.TestTimeoutMinutes) {
		ci.TestTimeoutMinutes = timeout
	}
}

func runnerLabels(n *yaml.Node) []string {
	if n == nil {
		return nil
	}
	switch n.Kind {
	case yaml.ScalarNode:
		return []string{n.Value}
	case yaml.SequenceNode:
		var out []string
		for _, c := range n.Content {
			out = append(out, scalar(c))
		}
		return out
	case yaml.MappingNode:
		return runnerLabels(mapGet(n, "labels"))
	}
	return nil
}

/* Lockfiles and Python dependencies */

var lockKinds = map[string]string{
	"package-lock.json": "npm", "pnpm-lock.yaml": "pnpm", "yarn.lock": "yarn", "bun.lock": "bun",
	"uv.lock": "uv", "poetry.lock": "poetry", "Pipfile.lock": "pipenv", "Cargo.lock": "cargo",
	"go.sum": "go", "Gemfile.lock": "bundler", "composer.lock": "composer",
}

var requirementsName = regexp.MustCompile(`^requirements[A-Za-z0-9._-]*\.txt$`)

func lockKind(base string) string {
	if k, ok := lockKinds[base]; ok {
		return k
	}
	if requirementsName.MatchString(base) {
		return "pip"
	}
	return ""
}

type pythonDeps struct {
	names         map[string]bool
	transformers4 bool
}

func (d *pythonDeps) add(name, version string) {
	if d.names == nil {
		d.names = map[string]bool{}
	}
	n := strings.ReplaceAll(strings.ToLower(name), "_", "-")
	d.names[n] = true
	v := strings.TrimSpace(version)
	if n == "transformers" && (strings.HasPrefix(v, "4.") || strings.HasPrefix(v, "==4.") ||
		strings.HasPrefix(v, "~=4.") || strings.Contains(v, "<5")) {
		d.transformers4 = true
	}
}

// ml lists the heavy machine learning packages found, by their rule names
func (d *pythonDeps) ml() []string {
	var out []string
	for _, n := range []string{"diffusers", "tensorflow", "torch", "vllm"} {
		if d.names[n] || (n == "tensorflow" && d.names["tensorflow-gpu"]) {
			out = append(out, n)
		}
	}
	for n := range d.names {
		if strings.HasPrefix(n, "jax-cuda") || strings.HasPrefix(n, "jax[cuda") {
			out = append(out, "jax-cuda")
			break
		}
	}
	return out
}

var reqLine = regexp.MustCompile(`^([A-Za-z0-9][A-Za-z0-9._-]*(?:\[[A-Za-z0-9,._-]+\])?)\s*(.*)$`)

func (d *pythonDeps) readSpec(spec string) {
	m := reqLine.FindStringSubmatch(strings.TrimSpace(spec))
	if m == nil {
		return
	}
	name := m[1]
	if i := strings.Index(name, "["); i >= 0 && !strings.HasPrefix(strings.ToLower(name), "jax[") {
		name = name[:i]
	}
	d.add(name, m[2])
}

func (d *pythonDeps) readPyproject(b []byte) {
	var p struct {
		Project struct {
			Dependencies []string            `toml:"dependencies"`
			Optional     map[string][]string `toml:"optional-dependencies"`
		} `toml:"project"`
		Tool struct {
			Poetry struct {
				Dependencies map[string]any `toml:"dependencies"`
			} `toml:"poetry"`
		} `toml:"tool"`
	}
	if toml.Unmarshal(b, &p) != nil {
		return
	}
	for _, s := range p.Project.Dependencies {
		d.readSpec(s)
	}
	for _, group := range sortedKeys(p.Project.Optional) {
		for _, s := range p.Project.Optional[group] {
			d.readSpec(s)
		}
	}
	for _, name := range sortedKeys(p.Tool.Poetry.Dependencies) {
		v, _ := p.Tool.Poetry.Dependencies[name].(string)
		d.add(name, v)
	}
}

func readLockfile(r *Repo, py *pythonDeps, kind string, b []byte) {
	n := 0
	switch kind {
	case "npm":
		var l struct {
			Packages     map[string]json.RawMessage `json:"packages"`
			Dependencies map[string]json.RawMessage `json:"dependencies"`
		}
		if jsonDepthOK(b) && json.Unmarshal(b, &l) == nil {
			n = len(l.Dependencies)
			if l.Packages != nil {
				n = len(l.Packages)
				if _, root := l.Packages[""]; root {
					n--
				}
			}
		}
	case "pnpm":
		if root, ok := yamlRoot(b); ok {
			n = len(mapKeys(mapGet(root, "packages")))
		}
	case "yarn":
		n = yarnEntries(b)
	case "bun":
		var l struct {
			Packages map[string]json.RawMessage `json:"packages"`
		}
		if std, err := hujson.Standardize(b); err == nil && jsonDepthOK(std) &&
			json.Unmarshal(std, &l) == nil {
			n = len(l.Packages)
		}
	case "uv", "poetry", "cargo":
		var l struct {
			Package []struct {
				Name    string `toml:"name"`
				Version string `toml:"version"`
			} `toml:"package"`
		}
		if toml.Unmarshal(b, &l) == nil {
			n = len(l.Package)
			for _, p := range l.Package {
				if kind != "cargo" {
					py.add(p.Name, p.Version)
				}
			}
			if kind == "cargo" {
				r.CargoCrates += n
			}
		}
	case "pipenv":
		var l struct {
			Default map[string]struct {
				Version string `json:"version"`
			} `json:"default"`
			Develop map[string]json.RawMessage `json:"develop"`
		}
		if jsonDepthOK(b) && json.Unmarshal(b, &l) == nil {
			n = len(l.Default) + len(l.Develop)
			for _, name := range sortedKeys(l.Default) {
				py.add(name, l.Default[name].Version)
			}
		}
	case "pip":
		n = requirementLines(py, b)
	case "go":
		n = goModules(b)
	case "bundler":
		n = gemSpecs(b)
	case "composer":
		var l struct {
			Packages    []json.RawMessage `json:"packages"`
			PackagesDev []json.RawMessage `json:"packages-dev"`
		}
		if jsonDepthOK(b) && json.Unmarshal(b, &l) == nil {
			n = len(l.Packages) + len(l.PackagesDev)
		}
	}
	for i := range r.Lockfiles {
		if r.Lockfiles[i].Kind == kind {
			r.Lockfiles[i].Packages += n
			return
		}
	}
	r.Lockfiles = append(r.Lockfiles, Lockfile{Kind: kind, Packages: n})
}

func yarnEntries(b []byte) int {
	n := 0
	sc := lines(b)
	for sc.Scan() {
		l := sc.Text()
		if l == "" || l[0] == ' ' || l[0] == '\t' || l[0] == '#' || !strings.HasSuffix(l, ":") {
			continue
		}
		if strings.HasPrefix(l, "__metadata") {
			continue
		}
		n++
	}
	return n
}

func requirementLines(py *pythonDeps, b []byte) int {
	n := 0
	sc := lines(b)
	for sc.Scan() {
		l := strings.TrimSpace(sc.Text())
		if i := strings.Index(l, " #"); i >= 0 {
			l = l[:i]
		}
		if l == "" || l[0] == '#' || l[0] == '-' {
			continue
		}
		n++
		py.readSpec(l)
	}
	return n
}

func goModules(b []byte) int {
	seen := map[string]bool{}
	sc := lines(b)
	for sc.Scan() {
		if f := strings.Fields(sc.Text()); len(f) >= 3 {
			seen[f[0]] = true
		}
	}
	return len(seen)
}

var gemSpec = regexp.MustCompile(`^    [A-Za-z0-9._-]+ \(`)

func gemSpecs(b []byte) int {
	n := 0
	sc := lines(b)
	for sc.Scan() {
		if gemSpec.MatchString(sc.Text()) {
			n++
		}
	}
	return n
}

/* Toolchain pins */

var pinFiles = map[string]bool{
	".python-version": true, ".nvmrc": true, ".node-version": true, ".tool-versions": true,
	"rust-toolchain": true, "rust-toolchain.toml": true,
}

var versionPrefix = regexp.MustCompile(`^v?(\d{1,4}(?:\.\d{1,4}){0,2})`)

// version keeps the leading numeric version of s, empty when s starts with none
func version(s string) string {
	m := versionPrefix.FindStringSubmatch(strings.TrimSpace(s))
	if m == nil {
		return ""
	}
	return m[1]
}

func addPin(r *Repo, tool, v string) {
	if v == "" || slices.ContainsFunc(r.Pins, func(p Pin) bool { return p.Tool == tool }) {
		return
	}
	r.Pins = append(r.Pins, Pin{Tool: tool, Version: v})
}

func readPin(r *Repo, base string, b []byte) {
	first := ""
	if sc := lines(b); sc.Scan() {
		first = strings.TrimSpace(sc.Text())
	}
	switch base {
	case ".python-version":
		addPin(r, "python3", version(first))
	case ".nvmrc", ".node-version":
		addPin(r, "node", version(first))
	case ".tool-versions":
		sc := lines(b)
		for sc.Scan() {
			f := strings.Fields(sc.Text())
			if len(f) < 2 {
				continue
			}
			switch f[0] {
			case "python":
				addPin(r, "python3", version(f[1]))
			case "nodejs", "node":
				addPin(r, "node", version(f[1]))
			case "golang", "go":
				addPin(r, "go", version(f[1]))
			case "rust":
				addPin(r, "rust", rustChannel(f[1]))
			}
		}
	case "rust-toolchain":
		addPin(r, "rust", rustChannel(first))
	case "rust-toolchain.toml":
		var t struct {
			Toolchain struct {
				Channel string `toml:"channel"`
			} `toml:"toolchain"`
		}
		if toml.Unmarshal(b, &t) == nil {
			addPin(r, "rust", rustChannel(t.Toolchain.Channel))
		}
	}
}

// rustChannel is nightly, a version, or empty for stable and anything unreadable
func rustChannel(s string) string {
	if strings.HasPrefix(strings.TrimSpace(s), "nightly") {
		return "nightly"
	}
	return version(s)
}

func readGoMod(r *Repo, b []byte) {
	sc := lines(b)
	for sc.Scan() {
		f := strings.Fields(sc.Text())
		if len(f) != 2 {
			continue
		}
		switch f[0] {
		case "go":
			addPin(r, "go", version(f[1]))
		case "toolchain":
			addPin(r, "go-toolchain", version(strings.TrimPrefix(f[1], "go")))
		}
	}
}

/* Tree */

var nativeBuildFiles = map[string]bool{
	"CMakeLists.txt": true, "meson.build": true, "configure.ac": true, "configure": true,
	"binding.gyp": true,
}

func readTree(r *Repo, tree []Blob) {
	var locks []string
	for _, b := range tree {
		switch b.Type {
		case "commit":
			r.Tree.Submodules++
		case "blob":
			r.Tree.Blobs++
			if b.Size > 0 {
				r.Tree.Bytes += b.Size
			}
			if nativeBuildFiles[b.Path] {
				r.NativeBuild = true
			}
			if lockKind(path.Base(b.Path)) != "" {
				locks = append(locks, b.Path+"\x00"+b.SHA+"\n")
			}
		}
	}
	if len(locks) == 0 {
		return
	}
	sort.Strings(locks)
	sum := sha256.Sum256([]byte(strings.Join(locks, "")))
	r.LockKey = hex.EncodeToString(sum[:])
}

var lfsSize = regexp.MustCompile(`(?m)^size (\d{1,15})$`)

// lfsPointer reads the real size out of a Git LFS pointer file
func lfsPointer(b []byte) (int64, bool) {
	if len(b) > 1024 || !bytes.HasPrefix(b, []byte("version https://git-lfs.github.com/spec/")) {
		return 0, false
	}
	m := lfsSize.FindSubmatch(b)
	if m == nil {
		return 0, false
	}
	n, err := strconv.ParseInt(string(m[1]), 10, 64)
	return n, err == nil
}

/* Shared parsing */

func lines(b []byte) *bufio.Scanner {
	sc := bufio.NewScanner(bytes.NewReader(b))
	sc.Buffer(make([]byte, 0, 64<<10), 1<<20)
	return sc
}

// jsonDepthOK says whether b nests at most maxDepth arrays and objects
func jsonDepthOK(b []byte) bool {
	depth, inString, escaped := 0, false, false
	for _, c := range b {
		switch {
		case inString && escaped:
			escaped = false
		case inString && c == '\\':
			escaped = true
		case c == '"':
			inString = !inString
		case inString:
		case c == '[' || c == '{':
			depth++
			if depth > maxDepth {
				return false
			}
		case c == ']' || c == '}':
			depth--
		}
	}
	return true
}

// yamlRoot parses one YAML document, refusing aliases and nesting past maxDepth
func yamlRoot(b []byte) (*yaml.Node, bool) {
	var doc yaml.Node
	if yaml.Unmarshal(b, &doc) != nil || len(doc.Content) == 0 {
		return nil, false
	}
	if !yamlSafe(&doc, 0) {
		return nil, false
	}
	return doc.Content[0], true
}

func yamlSafe(n *yaml.Node, depth int) bool {
	if depth > maxDepth || n.Kind == yaml.AliasNode {
		return false
	}
	for _, c := range n.Content {
		if !yamlSafe(c, depth+1) {
			return false
		}
	}
	return true
}

func mapGet(n *yaml.Node, key string) *yaml.Node {
	if n == nil || n.Kind != yaml.MappingNode {
		return nil
	}
	for i := 0; i+1 < len(n.Content); i += 2 {
		if n.Content[i].Value == key {
			return n.Content[i+1]
		}
	}
	return nil
}

// mapKeys lists a mapping's keys in file order
func mapKeys(n *yaml.Node) []string {
	if n == nil || n.Kind != yaml.MappingNode {
		return nil
	}
	var out []string
	for i := 0; i+1 < len(n.Content); i += 2 {
		out = append(out, n.Content[i].Value)
	}
	return out
}

func scalar(n *yaml.Node) string {
	if n == nil || n.Kind != yaml.ScalarNode {
		return ""
	}
	return n.Value
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func ceilDiv(a, b int64) int64 {
	return (a + b - 1) / b
}
