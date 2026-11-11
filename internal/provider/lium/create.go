package lium

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

const (
	// defaultCreateBudget bounds all of Create, inside the saga's placing budget
	defaultCreateBudget = 20 * time.Second
	// maxNodes is how many nodes one create tries
	maxNodes = 3
	// rentMemory is how long a key's node and body stay kept for its retries
	rentMemory = time.Hour
	// minInProgressWait spaces the resends of a rent Lium is still running
	minInProgressWait = 100 * time.Millisecond
	// removalSlack is the saga's lifetime slack, which a deploy-time termination covers too
	removalSlack           = 5 * time.Minute
	terminationMaxLifetime = "max_lifetime"
	terminationLifetime    = "lifetime"
	maxTerminationHours    = 720
)

// rentBody is the rent request, built once per node so a resend carries the same bytes
type rentBody struct {
	PodName                 string   `json:"pod_name"`
	TemplateID              string   `json:"template_id"`
	UserPublicKey           []string `json:"user_public_key"`
	InitialPortCount        int      `json:"initial_port_count"`
	EnableJupyter           bool     `json:"enable_jupyter"`
	EnableBackups           bool     `json:"enable_backups"`
	EnableVolumeEncryption  bool     `json:"enable_volume_encryption"`
	SkipAddAgentSSHKey      bool     `json:"skip_add_agent_ssh_key"`
	TerminationHours        int      `json:"termination_hours"`
	ExpectedPricePerGPUHour float64  `json:"expected_price_per_gpu_hour"`
}

// rental is one rent of one node under one key, kept so the key's retries resend it as it was
type rental struct {
	node         string
	hourlyMicros int64
	gpus         int
	body         []byte
	// idemKey is the Idempotency-Key header, empty for a create without a key
	idemKey, customerRef string
	// sentAt is taken before the first send, so created_at never comes late
	sentAt time.Time
	// image is the ref whose template the body names
	image, template string
	// uncertain marks a rental one of whose sends may have rented
	uncertain bool
	// mixed marks a rental one of whose sends got no answer or one other than a server error
	mixed bool
}

// order is one Create's request as its rents read it
type order struct {
	key, name, image, customerRef string
	// fresh marks a create that came without an idempotency key
	fresh bool
	hours int
	size  size
}

// Create adopts the live pod of the key, else rents the cheapest eligible node, trying up to 3
func (p *Provider) Create(ctx context.Context, req provider.CreateRequest) (provider.Ref, error) {
	o, err := p.orderFor(req)
	if err != nil {
		return provider.Ref{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, p.createBudget)
	defer cancel()
	var tried []string
	var refused []error
	if !o.fresh {
		ref, found, err := p.findOwn(ctx, o)
		r, kept := p.remembered(o.key)
		switch {
		case found:
			return ref, err
		case err != nil && kept && r.uncertain:
			// The key's rental may have rented, so a failed listing leaves the retry to the saga
			return provider.Ref{}, unknownOutcome(err)
		case err != nil:
			return provider.Ref{}, err
		case kept:
			// A retry of the key goes to the node it rented first, with the same key and bytes
			ref, next, err := p.settle(ctx, o, r)
			if !next {
				return ref, err
			}
			tried, refused = append(tried, r.node), append(refused, err)
		}
	}
	return p.rentNodes(ctx, o, tried, refused)
}

// orderFor checks the request and names its pod
func (p *Provider) orderFor(req provider.CreateRequest) (order, error) {
	s, ok := p.size(req.Size.Name)
	switch {
	case req.Builder:
		return order{}, invalid(errors.New("lium saves no images, so it builds none"), false)
	case !ok:
		return order{}, invalid(fmt.Errorf("lium sells no size %q", req.Size.Name), false)
	case req.Lifetime <= 0 || req.Lifetime > p.maxLifetime:
		return order{}, invalid(fmt.Errorf("lifetime %v is not within lium's limit of %v",
			req.Lifetime, p.maxLifetime), false)
	}
	image := cmp.Or(req.Image, p.image)
	if image == "" {
		return order{}, invalid(errors.New("lium has no image to start pods from"), true)
	}
	if _, err := parseImageRef(image); err != nil {
		return order{}, err
	}
	o := order{
		key: podKey(req.IdempotencyKey), image: image, customerRef: customerRef(req.Labels),
		fresh: req.IdempotencyKey == "", hours: p.terminationHours(req.Lifetime), size: s,
	}
	if o.fresh {
		// A hash of no key would give every such create one name, SSH key and replay
		o.key = freshKey()
	}
	o.name = podName(o.key, req.Labels)
	return o, nil
}

// terminationHours is the pod's deploy-time removal in whole hours
func (p *Provider) terminationHours(lifetime time.Duration) int {
	d := p.maxLifetime + removalSlack
	if p.termination == terminationLifetime {
		d = lifetime
	}
	h := int((d + time.Hour - 1) / time.Hour)
	return min(max(h, 1), maxTerminationHours)
}

// findOwn adopts a live pod of the order's name and deletes its dead ones
func (p *Provider) findOwn(ctx context.Context, o order) (provider.Ref, bool, error) {
	pods, err := p.listPods(ctx)
	if err != nil {
		return provider.Ref{}, false, err
	}
	var live *podItem
	var dead []string
	var kept error
	for _, pod := range pods {
		switch {
		case pod.Name != o.name:
		case !knownStatus(pod.Status):
			return provider.Ref{}, false, &provider.Error{Code: provider.CodeInternal,
				Err: fmt.Errorf("lium pod %s has status %q", pod.ID, pod.Status)}
		case adoptable[pod.Status]:
			if live == nil {
				live = &pod
			}
		default:
			dead = append(dead, pod.Status)
			// A failed pod comes back and bills again unless it is deleted
			if pod.Status != statusDeleting {
				if err := p.destroyPod(ctx, pod.ID, pod.Name); err != nil && kept == nil {
					kept = fmt.Errorf("lium pod %s ended as %s and was not deleted, %w", pod.ID,
						pod.Status, err)
				}
			}
		}
	}
	switch {
	case live != nil:
		if kept != nil {
			p.log.Warn("lium: a dead pod of the key was not deleted", "err", kept)
		}
		return p.adopted(o, *live), true, nil
	case kept != nil:
		// The key's pod is still there and may bill again, so the order never moves on beside it
		return provider.Ref{}, true, &provider.Error{Code: provider.CodeInternal, Err: kept}
	case len(dead) > 0:
		// The key's sandbox already died, and a rent under the key would replay it
		return provider.Ref{}, true, &provider.Error{
			Code: provider.CodeCapacity, CanFailOver: true,
			Err: fmt.Errorf("lium pod %s already ended as %s", o.name, dead[0]),
		}
	}
	return provider.Ref{}, false, nil
}

// rentNodes rents the cheapest eligible nodes not tried yet, up to maxNodes in all
func (p *Provider) rentNodes(
	ctx context.Context, o order, tried []string, refused []error,
) (provider.Ref, error) {
	nodes, fresh := p.nodes.fresh()
	if !fresh {
		return provider.Ref{}, &provider.Error{Code: provider.CodeCapacity, CanFailOver: true,
			Err: errors.New("lium's node listing is over 2 minutes old")}
	}
	for _, n := range p.candidates(o.size, nodes) {
		if len(tried) >= maxNodes {
			break
		}
		if slices.Contains(tried, n.id) {
			continue
		}
		template, err := p.template(ctx, o.image)
		if err != nil {
			return provider.Ref{}, err
		}
		tried = append(tried, n.id)
		r, err := p.newRental(o, n, template)
		if err != nil {
			return provider.Ref{}, err
		}
		ref, next, err := p.settle(ctx, o, r)
		if !next {
			return ref, err
		}
		refused = append(refused, err)
	}
	if len(refused) == 0 {
		return provider.Ref{}, &provider.Error{Code: provider.CodeCapacity, CanFailOver: true,
			Err: fmt.Errorf("lium lists no node for size %s", o.size.Name)}
	}
	// Another rent of ours holding a node never sends the order elsewhere by itself
	failOver := slices.ContainsFunc(refused, func(err error) bool {
		return codeOf(err) != codeOwnRentInProgress
	})
	return provider.Ref{}, &provider.Error{Code: provider.CodeCapacity, CanFailOver: failOver,
		Err: fmt.Errorf("lium refused %d nodes, the last with %w", len(refused),
			refused[len(refused)-1])}
}

// newRental builds the rent of one node, its body bytes fixed for every resend
func (p *Provider) newRental(o order, n node, template string) (rental, error) {
	r := rental{
		node: n.id, hourlyMicros: n.hourlyMicros, gpus: n.gpus, customerRef: o.customerRef,
		sentAt: p.now().UTC().Truncate(time.Second), image: o.image,
	}
	if !o.fresh {
		r.idemKey = o.key + ":" + n.id
	}
	return p.withTemplate(o, r, template)
}

// withTemplate writes the rent body naming template
func (p *Provider) withTemplate(o order, r rental, template string) (rental, error) {
	body, err := json.Marshal(rentBody{
		PodName: o.name, TemplateID: template, UserPublicKey: []string{p.podPublicKey(o.name)},
		InitialPortCount: 1, SkipAddAgentSSHKey: true, TerminationHours: o.hours,
		ExpectedPricePerGPUHour: float64(r.hourlyMicros/int64(max(r.gpus, 1))) / 1e6,
	})
	if err != nil {
		return r, &provider.Error{Code: provider.CodeInternal, Err: err}
	}
	r.body, r.template = body, template
	if !o.fresh {
		p.remember(o.key, r)
	}
	return r, nil
}

// settle sends one rental until Lium answers it, next being true when another node may be tried
func (p *Provider) settle(ctx context.Context, o order, r rental) (provider.Ref, bool, error) {
	retried, remade := false, false
	for {
		id, err := p.sendRent(ctx, r)
		if err == nil {
			return p.rented(o, r, id), false, nil
		}
		r = p.noted(o, r, err)
		code := codeOf(err)
		switch {
		case code == codeInProgress:
			if !pause(ctx, max(retryAfterOf(err), minInProgressWait)) {
				return provider.Ref{}, false, unknownOutcome(err)
			}
			continue
		case code == codeOutcomeUnknown:
			return p.afterUnknown(ctx, o, r, err)
		case mayHaveRented(err) && !retried:
			retried = true
			continue
		case r.uncertain && code != codeKeyInvalid && code != codeKeyReused:
			// Once a send may have rented, only an idempotency answer or a listing says what it did
			return p.afterUnknown(ctx, o, r, err)
		case nodeCodes[code] || code == codeConflict:
			p.forget(o.key, r)
			return provider.Ref{}, true, err
		case code == codeOwnRentInProgress:
			// Our rent of this key would have met idempotency_in_progress, so another holds it
			p.forget(o.key, r)
			ref, found, lerr := p.findOwn(ctx, o)
			if lerr != nil || found {
				return ref, false, lerr
			}
			return provider.Ref{}, true, err
		case code == codeTemplateNotFound && !remade:
			remade = true
			p.dropTemplate(r.image, r.template)
			template, terr := p.template(ctx, r.image)
			if terr != nil {
				return provider.Ref{}, false, terr
			}
			if r, err = p.withTemplate(o, r, template); err != nil {
				return provider.Ref{}, false, err
			}
			continue
		case code == codeTemplateInvalid:
			p.dropTemplate(r.image, r.template)
			p.forget(o.key, r)
		case invalidCodes[code]:
			p.forget(o.key, r)
		}
		return provider.Ref{}, false, err
	}
}

// noted marks what a failed send of r shows, keeping r for the key's retries when that changed
func (p *Provider) noted(o order, r rental, err error) rental {
	code := codeOf(err)
	uncertain := r.uncertain || mayHaveRented(err) || code == codeInProgress ||
		code == codeOutcomeUnknown
	answered := errors.As(err, new(*apiError))
	mixed := r.mixed || lostAnswer(err) || answered && !serverError(err)
	if uncertain == r.uncertain && mixed == r.mixed {
		return r
	}
	r.uncertain, r.mixed = uncertain, mixed
	if !o.fresh {
		p.remember(o.key, r)
	}
	return r
}

// sendRent sends one rental and returns the pod id Lium answers
func (p *Provider) sendRent(ctx context.Context, r rental) (string, error) {
	h := http.Header{}
	h.Set("X-Customer-Ref", r.customerRef)
	if r.idemKey != "" {
		h.Set("Idempotency-Key", r.idemKey)
	}
	var out struct {
		PodID string `json:"pod_id"`
		ID    string `json:"id"`
	}
	_, err := p.api.do(ctx, call{
		method: http.MethodPost, route: "/executors/" + url.PathEscape(r.node) + "/rent",
		body: r.body, header: h, rent: true,
	}, &out)
	if err != nil {
		return "", err
	}
	if id := cmp.Or(out.PodID, out.ID); id != "" {
		return id, nil
	}
	return "", &provider.Error{Code: provider.CodeOutcomeUnknown,
		Err: fmt.Errorf("lium rented on node %s but named no pod", r.node)}
}

// mayHaveRented reports a rent that got no usable answer once sent, or a server error
func mayHaveRented(err error) bool {
	return serverError(err) || lostAnswer(err)
}

// serverError reports a 5xx answer
func serverError(err error) bool {
	var ae *apiError
	return errors.As(err, &ae) && ae.Status >= 500
}

// lostAnswer reports a rent that was sent and got no usable answer
func lostAnswer(err error) bool {
	e, ok := provider.AsError(err)
	return ok && e.Code == provider.CodeOutcomeUnknown && !errors.As(err, new(*apiError))
}

// afterUnknown lists by name for a rental that may have rented, adopting the pod when listed
func (p *Provider) afterUnknown(
	ctx context.Context, o order, r rental, cause error,
) (provider.Ref, bool, error) {
	ref, found, err := p.findOwn(ctx, o)
	switch {
	case found:
		return ref, false, err
	case err != nil:
		return provider.Ref{}, false, unknownOutcome(fmt.Errorf(
			"%w, then the listing failed, %w", cause, err))
	case serverError(cause) && !r.mixed:
		// Every send got a server error and no pod is listed, so Lium shows no rent of ours
		return provider.Ref{}, false, cause
	}
	return provider.Ref{}, false, unknownOutcome(cause)
}

// unknownOutcome is a create that may have rented, which the saga retries here with the key
func unknownOutcome(err error) error {
	return &provider.Error{Code: provider.CodeOutcomeUnknown, Err: err}
}

// retryAfterOf is the wait an error answer asks for
func retryAfterOf(err error) time.Duration {
	if e, ok := provider.AsError(err); ok {
		return e.RetryAfter
	}
	return defaultRetryAfter
}

// pause waits d, false when ctx ends first
func pause(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// rented is the ref of a pod a rent made
func (p *Provider) rented(o order, r rental, id string) provider.Ref {
	return provider.Ref{Provider: providerName, ID: id, Meta: map[string]string{
		metaPodName: o.name, metaCreatedAt: r.sentAt.Format(time.RFC3339), metaNode: r.node,
		metaHourlyMicros: strconv.FormatInt(r.hourlyMicros, 10), metaArch: arch,
		metaSize: o.size.Name,
	}}
}

// adopted is the ref of a listed pod of the key
func (p *Provider) adopted(o order, pod podItem) provider.Ref {
	meta := map[string]string{
		metaPodName: pod.Name, metaNode: pod.ExecutorID, metaArch: arch, metaSize: o.size.Name,
	}
	if t, ok := parseTime(pod.CreatedAt); ok {
		meta[metaCreatedAt] = t.Truncate(time.Second).Format(time.RFC3339)
	}
	if pod.Price != nil {
		meta[metaHourlyMicros] = strconv.FormatInt(micros(*pod.Price), 10)
	}
	return provider.Ref{Provider: providerName, ID: pod.ID, Meta: meta}
}

// remember keeps a key's rental, pruning those past rentMemory
func (p *Provider) remember(key string, r rental) {
	p.rmu.Lock()
	defer p.rmu.Unlock()
	now := p.now()
	for k, kept := range p.rentals {
		if now.Sub(kept.at) > rentMemory {
			delete(p.rentals, k)
		}
	}
	p.rentals[key] = keptRental{rental: r, at: now}
}

// remembered returns the key's rental while it is under rentMemory old
func (p *Provider) remembered(key string) (rental, bool) {
	p.rmu.Lock()
	defer p.rmu.Unlock()
	kept, ok := p.rentals[key]
	if !ok || p.now().Sub(kept.at) > rentMemory {
		return rental{}, false
	}
	return kept.rental, true
}

// forget drops the key's rental of r's node, which Lium refused
func (p *Provider) forget(key string, r rental) {
	p.rmu.Lock()
	defer p.rmu.Unlock()
	if kept, ok := p.rentals[key]; ok && kept.node == r.node {
		delete(p.rentals, key)
	}
}

// keptRental is a rental and when it was kept
type keptRental struct {
	rental
	at time.Time
}
