package hf

import (
	"cmp"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strconv"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

const (
	// tokenLabel starts the message every sandbox token is the HMAC of, design 4.4
	tokenLabel = "bugnet/hf-sbx/v1"
	seedBytes  = 32
	// labelKey holds a Job's key, the one label the adapter sets itself
	labelKey = "bugnet_key"
	// keyMemory is how long a key's Job stays kept for the key's retries in this process
	keyMemory = time.Hour
	// relistBudget bounds the listing after a create whose answer was lost, past the caller's end
	relistBudget = 10 * time.Second
	// sbxPort is sbx-server's port in every Job, public on HF's proxy
	sbxPort = 49983
	// sbxServerBucket is where a Job fetches sbx-server by its digest, HF's public bucket
	sbxServerBucket = "https://huggingface.co/buckets/huggingface/sbx-server/resolve/sbx-server-"
	// bootScript fetches sbx-server, checks its digest and runs it as PID 1, design 4.10
	bootScript = `set -e; d=/tmp/.sbx-server; curl -fsSL -o "$d" "$SBX_SERVER_URL"; ` +
		`echo "$SBX_SERVER_SHA256  $d" | sha256sum -c -; chmod +x "$d"; ` +
		`unset SBX_SERVER_URL SBX_SERVER_SHA256; exec "$d"`
	// maxTimeoutSeconds bounds a timeout read from a Job, past which it is not believed
	maxTimeoutSeconds = 1 << 31
)

// Ref.Meta entries the adapter keeps
const (
	metaJobID     = "job_id"
	metaCreatedAt = "created_at"
	metaTimeout   = "timeout"
	// metaSecret holds the sandbox token, a field the store seals
	metaSecret = "secret"
	metaArch   = "arch"
)

var (
	keyHex = regexp.MustCompile(`^[0-9a-f]{32}$`)
	// labelName and labelWord are the label keys and values HF takes
	labelName = regexp.MustCompile(`^[A-Za-z0-9_-]{1,100}$`)
	labelWord = regexp.MustCompile(`^[A-Za-z0-9_-]{0,100}$`)
	// imageByDigest is an image ref naming its digest, the only kind a Job starts from
	imageByDigest = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._/:-]*@sha256:[0-9a-f]{64}$`)
	sha256Hex     = regexp.MustCompile(`^[0-9a-f]{64}$`)
)

// keptJob is the ref of the Job a key got in this process, and when it got it
type keptJob struct {
	ref provider.Ref
	at  time.Time
}

// jobKey is the 32 lowercase hex a Job's bugnet_key label and token derive from
func jobKey(idempotencyKey string) string {
	if keyHex.MatchString(idempotencyKey) {
		return idempotencyKey
	}
	sum := sha256.Sum256([]byte(idempotencyKey))
	return hex.EncodeToString(sum[:])[:32]
}

// freshKey is 32 random lowercase hex for a create without an idempotency key
func freshKey() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// sandboxToken derives the SBX_TOKEN of the Job carrying key, so any process can adopt the Job
func (p *Provider) sandboxToken(key string) string {
	mac := hmac.New(sha256.New, p.seed)
	mac.Write([]byte(tokenLabel))
	mac.Write([]byte{0})
	mac.Write([]byte(key))
	return hex.EncodeToString(mac.Sum(nil))
}

// jobSpec is the body of POST /api/jobs/{namespace}, design 4.10
type jobSpec struct {
	DockerImage    string            `json:"dockerImage"`
	Command        []string          `json:"command"`
	Arguments      []string          `json:"arguments"`
	Environment    map[string]string `json:"environment"`
	Secrets        map[string]string `json:"secrets"`
	Flavor         string            `json:"flavor"`
	Arch           string            `json:"arch"`
	TimeoutSeconds int64             `json:"timeoutSeconds"`
	Attempts       int               `json:"attempts"`
	Labels         map[string]string `json:"labels"`
	Expose         jobExpose         `json:"expose"`
}

type jobExpose struct {
	Ports       []int `json:"ports"`
	PortsPublic []int `json:"portsPublic"`
}

// order is one Create's request as its calls read it
type order struct {
	key, token string
	// fresh marks a create that came without an idempotency key
	fresh   bool
	labels  map[string]string
	timeout int64
	spec    []byte
}

// Create returns the Job this process made for the key, else adopts a live one, else starts one
func (p *Provider) Create(ctx context.Context, req provider.CreateRequest) (provider.Ref, error) {
	o, err := p.orderFor(req)
	if err != nil {
		return provider.Ref{}, err
	}
	if o.fresh {
		return p.post(ctx, o)
	}
	unlock, err := p.lockKey(ctx, o.key)
	if err != nil {
		// The key's create in flight may start the Job, so only a retry of the key may tell
		return provider.Ref{}, unknownOutcome(fmt.Errorf(
			"hf: another create of the key is still running: %w", err))
	}
	defer unlock()
	if ref, ok := p.keptRef(o.key); ok {
		return ref, nil
	}
	ref, found, err := p.findOwn(ctx, o)
	switch {
	case err != nil:
		return provider.Ref{}, err
	case found:
		p.keep(o, ref)
		return ref, nil
	}
	return p.post(ctx, o)
}

// orderFor checks the request and writes its Job's spec
func (p *Provider) orderFor(req provider.CreateRequest) (order, error) {
	f, ok := p.hw.flavor(req.Size.Name)
	image := cmp.Or(req.Image, p.image)
	switch {
	case req.Builder:
		return order{}, invalid(errors.New("hf saves no images, so it builds none"), false)
	case !ok:
		return order{}, invalid(fmt.Errorf("hf sells no size %q", req.Size.Name), false)
	case req.Lifetime <= 0 || req.Lifetime > p.maxLifetime:
		return order{}, invalid(fmt.Errorf("lifetime %v is not within hf's limit of %v",
			req.Lifetime, p.maxLifetime), false)
	case image == "":
		return order{}, invalid(errors.New("hf has no image to start Jobs from"), true)
	case !imageByDigest.MatchString(image):
		return order{}, invalid(fmt.Errorf("hf image %.200q is not named by digest", image), true)
	}
	if _, ok := req.Labels[labelKey]; ok {
		return order{}, invalid(errors.New("the label bugnet_key is the hf adapter's own"), false)
	}
	if err := checkLabels(req.Labels); err != nil {
		return order{}, err
	}
	o := order{key: jobKey(req.IdempotencyKey), fresh: req.IdempotencyKey == ""}
	if o.fresh {
		// A hash of no key would give every such create one label and one token
		o.key = freshKey()
	}
	o.token, o.timeout = p.sandboxToken(o.key), p.timeoutFor(req.Lifetime)
	o.labels = maps.Clone(req.Labels)
	if o.labels == nil {
		o.labels = map[string]string{}
	}
	o.labels[labelKey] = o.key
	ports := []int{sbxPort, provider.DefaultGatePort}
	spec, err := json.Marshal(jobSpec{
		DockerImage: image, Command: []string{"/bin/sh", "-c", bootScript}, Arguments: []string{},
		Environment: map[string]string{
			"SBX_PORT":          strconv.Itoa(sbxPort),
			"SBX_SERVER_URL":    sbxServerBucket + p.sbxSHA256,
			"SBX_SERVER_SHA256": p.sbxSHA256,
		},
		Secrets: map[string]string{"SBX_TOKEN": o.token}, Flavor: f.name, Arch: arch,
		TimeoutSeconds: o.timeout, Attempts: 1, Labels: o.labels,
		Expose: jobExpose{Ports: ports, PortsPublic: ports},
	})
	if err != nil {
		return order{}, &provider.Error{Code: provider.CodeInternal, Err: err}
	}
	o.spec = spec
	return o, nil
}

// timeoutFor is a Job's timeoutSeconds, its lifetime with the room and margin, design 4.7
func (p *Provider) timeoutFor(lifetime time.Duration) int64 {
	d := min(lifetime+p.extendRoom, p.maxLifetime) + p.margin
	return int64((d + time.Second - 1) / time.Second)
}

// checkLabels refuses a label HF would not take, design 4.8
func checkLabels(labels map[string]string) error {
	for _, k := range slices.Sorted(maps.Keys(labels)) {
		if !labelName.MatchString(k) || !labelWord.MatchString(labels[k]) {
			return invalid(fmt.Errorf("hf takes no label %.40q=%.40q, only letters, digits, "+
				"'-' and '_' up to 100 each", k, labels[k]), true)
		}
	}
	return nil
}

// findOwn adopts the oldest live Job carrying the order's key and labels
func (p *Provider) findOwn(ctx context.Context, o order) (provider.Ref, bool, error) {
	jobs, err := p.jobs.listJobs(ctx, map[string]string{labelKey: o.key}, liveStages)
	if err != nil {
		return provider.Ref{}, false, err
	}
	var best *job
	for i, j := range jobs {
		if liveStage(j.Status.Stage) && hasLabels(j.Labels, o.labels) &&
			jobID.MatchString(j.ID) && (best == nil || j.CreatedAt.Before(best.CreatedAt)) {
			best = &jobs[i]
		}
	}
	if best == nil {
		return provider.Ref{}, false, nil
	}
	return refOf(*best, o.token, time.Time{}, 0), true, nil
}

// post sends the Job's create, and lists by its key when the answer may hide a Job HF started
func (p *Provider) post(ctx context.Context, o order) (provider.Ref, error) {
	sentAt := p.now()
	j, err := p.jobs.createJob(ctx, o.spec, o.token)
	if err == nil {
		ref := refOf(j, o.token, sentAt, o.timeout)
		p.keep(o, ref)
		return ref, nil
	}
	if !mayHaveStarted(err) {
		return provider.Ref{}, err
	}
	lctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), relistBudget)
	defer cancel()
	ref, found, lerr := p.findOwn(lctx, o)
	switch {
	case found:
		p.keep(o, ref)
		return ref, nil
	case lerr != nil:
		return provider.Ref{}, unknownOutcome(fmt.Errorf("%w, then the listing failed, %w",
			causeOf(err), lerr))
	case serverError(err):
		// HF answered and lists no Job of the key, so it shows no Job started
		return provider.Ref{}, err
	}
	return provider.Ref{}, unknownOutcome(causeOf(err))
}

// causeOf is what a provider error wraps, so a new code never repeats the old one's words
func causeOf(err error) error {
	if e, ok := provider.AsError(err); ok && e.Err != nil {
		return e.Err
	}
	return err
}

// refOf is a Job's ref, its start the earlier of HF's and sentAt, its timeout the shorter
func refOf(j job, token string, sentAt time.Time, sent int64) provider.Ref {
	created := j.CreatedAt
	if created.IsZero() || !sentAt.IsZero() && sentAt.Before(created) {
		created = sentAt
	}
	var timeout int64
	if j.Timeout > 0 && j.Timeout < maxTimeoutSeconds {
		timeout = int64(j.Timeout)
	}
	if timeout == 0 || sent > 0 && sent < timeout {
		timeout = sent
	}
	meta := map[string]string{metaJobID: j.ID, metaArch: arch}
	if !created.IsZero() {
		meta[metaCreatedAt] = created.UTC().Truncate(time.Second).Format(time.RFC3339)
	}
	if timeout > 0 {
		meta[metaTimeout] = strconv.FormatInt(timeout, 10)
	}
	if token != "" {
		meta[metaSecret] = token
	}
	return provider.Ref{Provider: providerName, ID: j.ID, Meta: meta}
}

// mayHaveStarted reports a create that got no usable answer once sent, or a server error
func mayHaveStarted(err error) bool {
	e, ok := provider.AsError(err)
	return ok && e.Code == provider.CodeOutcomeUnknown || serverError(err)
}

// serverError reports a 5xx answer
func serverError(err error) bool {
	var ae *apiError
	return errors.As(err, &ae) && ae.Status >= 500
}

// unknownOutcome is a create that may have started a Job, which the saga retries with the key
func unknownOutcome(err error) error {
	return &provider.Error{Code: provider.CodeOutcomeUnknown, Err: err}
}

func invalid(err error, failOver bool) error {
	return &provider.Error{Code: provider.CodeInvalid, CanFailOver: failOver, Err: err}
}

// hasLabels reports whether have carries every label of want
func hasLabels(have, want map[string]string) bool {
	for k, v := range want {
		if got, ok := have[k]; !ok || got != v {
			return false
		}
	}
	return true
}

// lockKey waits until no other create of key runs in this process, an error when ctx ends first
func (p *Provider) lockKey(ctx context.Context, key string) (func(), error) {
	for {
		p.kmu.Lock()
		busy, ok := p.creating[key]
		if !ok {
			done := make(chan struct{})
			p.creating[key] = done
			p.kmu.Unlock()
			return func() {
				p.kmu.Lock()
				delete(p.creating, key)
				p.kmu.Unlock()
				close(done)
			}, nil
		}
		p.kmu.Unlock()
		select {
		case <-busy:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// keep holds the key's Job for its retries, dropping keys past keyMemory, none for a fresh key
func (p *Provider) keep(o order, ref provider.Ref) {
	if o.fresh {
		return
	}
	p.kmu.Lock()
	defer p.kmu.Unlock()
	now := p.now()
	for k, kept := range p.kept {
		if now.Sub(kept.at) > keyMemory {
			delete(p.kept, k)
		}
	}
	p.kept[o.key] = keptJob{ref: cloneRef(ref), at: now}
}

// keptRef returns the key's Job while it is under keyMemory old
func (p *Provider) keptRef(key string) (provider.Ref, bool) {
	p.kmu.Lock()
	defer p.kmu.Unlock()
	kept, ok := p.kept[key]
	if !ok || p.now().Sub(kept.at) > keyMemory {
		return provider.Ref{}, false
	}
	return cloneRef(kept.ref), true
}

func cloneRef(ref provider.Ref) provider.Ref {
	ref.Meta = maps.Clone(ref.Meta)
	return ref
}
