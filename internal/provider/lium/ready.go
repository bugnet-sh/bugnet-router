package lium

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
)

// refuseTimeout bounds the delete of a pod whose boot the checks refused
const refuseTimeout = 30 * time.Second

// ReadyMeta waits for RUNNING, checks the pod's node and address, and returns its SSH and node
func (p *Provider) ReadyMeta(ctx context.Context, ref provider.Ref) (map[string]string, error) {
	id, ok := podID(ref)
	if !ok {
		return nil, notLium(ref)
	}
	d, err := p.waitRunning(ctx, id)
	if err != nil {
		return nil, err
	}
	meta := map[string]string{metaNode: ref.Meta[metaNode]}
	if node := d.node(); node != ref.Meta[metaNode] {
		if why := p.moveRefused(ref, d); why != "" {
			return nil, p.refuseBoot(ctx, ref, fmt.Errorf("lium moved pod %s to node %s, which %s",
				id, node, why))
		}
		meta[metaNode] = node
	}
	if d.Price != nil {
		meta[metaHourlyMicros] = strconv.FormatInt(micros(*d.Price), 10)
	}
	addr, err := netip.ParseAddr(d.Executor.Address)
	if err != nil || p.refuses(addr) {
		return nil, p.refuseBoot(ctx, ref, fmt.Errorf(
			"lium pod %s is at %q, an address no public host has", id, d.Executor.Address))
	}
	port, _ := d.sshPortOf()
	host := addr.Unmap().String()
	meta[metaSSHHost], meta[metaSSHPort] = host, strconv.Itoa(port)
	if p.hostKey != nil {
		key, err := p.hostKey(ctx, ref, net.JoinHostPort(host, strconv.Itoa(port)))
		if err != nil {
			return nil, err
		}
		meta[provider.MetaHostKey] = key
	}
	meta[metaReadyAt] = p.now().UTC().Format(time.RFC3339)
	return meta, nil
}

// WaitReady waits as ReadyMeta does, dropping what it learns
func (p *Provider) WaitReady(ctx context.Context, ref provider.Ref) error {
	_, err := p.ReadyMeta(ctx, ref)
	return err
}

// waitRunning reads the pod every pollEvery until it runs with an SSH port, or its start ends
func (p *Provider) waitRunning(ctx context.Context, id string) (podDetail, error) {
	last := errors.New("not read yet")
	for {
		d, err := p.getPod(ctx, id)
		e, _ := provider.AsError(err)
		switch {
		case err == nil:
			wait, end := startOf(d)
			if end != nil || wait == nil {
				return d, end
			}
			last = wait
		case e != nil && (e.Code == provider.CodeNotFound || e.Code == provider.CodeAuth ||
			e.Code == provider.CodeInvalid || e.Code == provider.CodeUnfunded):
			return d, err
		case ctx.Err() == nil:
			last = err
		}
		if !pause(ctx, p.pollEvery) {
			return d, fmt.Errorf("lium pod %s not running: %w, last %w", id, ctx.Err(), last)
		}
	}
}

// startOf reads one poll, wait being why the start goes on and end an error that ends it
func startOf(d podDetail) (wait, end error) {
	switch s := d.Status; {
	case s == statusRunning:
		if _, ok := d.sshPortOf(); !ok {
			return errors.New("running with no SSH port mapped yet"), nil
		}
		return nil, nil
	case startingStatuses[s]:
		return fmt.Errorf("status %s", s), nil
	case failedStatuses[s]:
		return nil, &provider.Error{Code: provider.CodeCapacity, CanFailOver: true,
			Err: fmt.Errorf("lium pod %s ended its start as %s", d.ID, s)}
	case stoppedStatuses[s]:
		// Lium stops pods only when the balance reaches zero
		return nil, provider.Named(&provider.Error{Code: provider.CodeUnfunded, CanFailOver: true,
			Err: fmt.Errorf("lium stopped pod %s during its start", d.ID)}, providerName)
	case s == statusDeleting:
		return nil, &provider.Error{Code: provider.CodeNotFound,
			Err: fmt.Errorf("lium pod %s is being deleted", d.ID)}
	}
	return nil, &provider.Error{Code: provider.CodeInternal,
		Err: fmt.Errorf("lium pod %s has status %q", d.ID, d.Status)}
}

// moveRefused says why the pod's node breaks design 3.5's rules, empty when it passes
func (p *Provider) moveRefused(ref provider.Ref, d podDetail) string {
	s, ok := p.size(ref.Meta[metaSize])
	if !ok {
		return "belongs to no size this router sells"
	}
	e := d.Executor
	var country string
	if e.Location != nil {
		country = strings.ToUpper(strings.TrimSpace(deref(e.Location.CountryCode)))
	}
	podGPUs, _ := intOf(d.GPUCount)
	switch {
	case deref(e.Tier) != "secure":
		return "is not Secure"
	case !p.allowedCountry(country):
		return "is in a country not allowed"
	case e.GPUCount == nil || *e.GPUCount != s.GPUs || podGPUs != s.GPUs:
		return "the pod does not hold whole"
	case !p.matches(s, deref(e.MachineName)):
		return "has another GPU type"
	case d.Price == nil || micros(*d.Price) > s.MaxHourlyMicros:
		return "costs more than the size's cap"
	}
	return ""
}

// allowedCountry is design 3.5's country rule for a pod's own node
func (p *Provider) allowedCountry(c string) bool {
	return c != "" && !p.blocked[c] && (p.countries[c] || p.otherCountries)
}

// matches reports a machine name the size rents, by match list or the listing's gpu_class
func (p *Provider) matches(s size, machine string) bool {
	if machine == "" {
		return false
	}
	if slices.Contains(s.Match, machine) {
		return true
	}
	class, ok := p.nodes.classOf(machine)
	return ok && slices.Contains(s.Match, class)
}

// refuseBoot deletes a pod the boot checks refused and answers capacity, so the order fails over
func (p *Provider) refuseBoot(ctx context.Context, ref provider.Ref, why error) error {
	dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), refuseTimeout)
	defer cancel()
	if err := p.Destroy(dctx, ref); err != nil {
		why = fmt.Errorf("%w, and the delete failed, %w", why, err)
	}
	return &provider.Error{Code: provider.CodeCapacity, CanFailOver: true, Err: why}
}
