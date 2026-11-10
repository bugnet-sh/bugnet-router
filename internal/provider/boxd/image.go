package boxd

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/boxd/boxdpb"
)

const (
	// ociPrefix marks an Image that names an OCI image for a builder to boot from
	ociPrefix    = "oci:"
	snapshotPoll = 2 * time.Second
	snapshotWait = 15 * time.Minute
	// bytesPerMiB turns a size's memory into the bytes a create without a spec asks for
	bytesPerMiB = 1 << 20
)

var snapshotWord = regexp.MustCompile(`(?i)snapshot|archiv`)

// createRequest builds the CreateVm request for a sandbox, a builder or a restore of a snapshot
//
// The returned labels are the ones the machine must carry, and the request's own map, so a key
// label added later reaches the spec.
func (p *Provider) createRequest(
	req provider.CreateRequest,
) (*boxdpb.CreateVmRequest, map[string]string, error) {
	oci, isOCI := strings.CutPrefix(req.Image, ociPrefix)
	switch {
	case req.Builder:
		return p.builderRequest(req, oci, isOCI)
	case isOCI:
		return nil, nil, invalid(errors.New("an oci image is only for builders"))
	}
	labels, err := encodeLabels(req.Labels)
	if err != nil {
		return nil, nil, err
	}
	snapshot := req.Image
	if snapshot == "" && len(p.snapshots) > 0 {
		var ok bool
		if snapshot, ok = p.snapshots[req.Size.Name]; !ok {
			return nil, nil, invalid(fmt.Errorf("boxd has no snapshot for size %q", req.Size.Name))
		}
	}
	spec := &boxdpb.MachineSpec{Version: 1, Size: req.Size.Name, Labels: labels, Isolated: true}
	if snapshot != "" {
		spec.Base = &boxdpb.MachineSpecBase{
			Kind: &boxdpb.MachineSpecBase_Snapshot{Snapshot: snapshot},
		}
	}
	in := &boxdpb.CreateVmRequest{
		Config: &boxdpb.VmConfig{
			Srf:     &boxdpb.SrfConfig{AutoSuspendTimeoutSecs: proto.Uint32(0)},
			Network: &boxdpb.NetworkConfig{Ssh: proto.Bool(false)},
		},
		Spec: spec,
	}
	if req.IdempotencyKey != "" {
		in.Name = machineName(req.IdempotencyKey)
		labels[keyLabel] = keyDigest(req.IdempotencyKey)
	}
	if req.Egress != nil {
		spec.Egress = egressList(req.Egress)
	}
	return in, labels, nil
}

// builderRequest builds the request of a machine an image is saved from, which carries no labels
//
// Restores inherit a snapshot's labels, so a builder's labels would reach every sandbox.
func (p *Provider) builderRequest(
	req provider.CreateRequest, oci string, isOCI bool,
) (*boxdpb.CreateVmRequest, map[string]string, error) {
	switch {
	case len(req.Labels) > 0:
		return nil, nil, invalid(errors.New("a boxd builder carries no labels"))
	case req.Egress != nil:
		return nil, nil, invalid(errors.New("a boxd builder has no egress rules"))
	case req.Image != "" && !isOCI:
		return nil, nil, invalid(errors.New("a boxd builder starts from boxd's image or oci"))
	case isOCI && oci == "":
		return nil, nil, invalid(errors.New("an oci image needs a reference"))
	}
	cfg := &boxdpb.VmConfig{
		Srf:     &boxdpb.SrfConfig{AutoSuspendTimeoutSecs: proto.Uint32(0)},
		Network: &boxdpb.NetworkConfig{Ssh: proto.Bool(false)},
	}
	if !isOCI {
		return &boxdpb.CreateVmRequest{
			Config: cfg,
			Spec:   &boxdpb.MachineSpec{Version: 1, Size: req.Size.Name, Isolated: true},
		}, map[string]string{}, nil
	}
	// A spec cannot be combined with image_ref, so the size travels in the config
	cfg.Vcpu = uint32(req.Size.Resources.VCPU)
	cfg.MemoryBytes = uint64(req.Size.Resources.MemoryMiB) * bytesPerMiB
	return &boxdpb.CreateVmRequest{ImageRef: oci, Isolated: true, Config: cfg},
		map[string]string{}, nil
}

// snapshotOf is the snapshot a create restores, empty for any other
func snapshotOf(in *boxdpb.CreateVmRequest) string {
	return in.GetSpec().GetBase().GetSnapshot()
}

// missingSnapshot lets a create refused for a missing or archived snapshot fail over
func (p *Provider) missingSnapshot(err error, snapshot string) error {
	e, ok := provider.AsError(err)
	if !ok || snapshot == "" || e.Code == provider.CodeAuth || e.Code == provider.CodeQuota ||
		e.Code == provider.CodeCapacity || e.Code == provider.CodeRateLimited {
		return err
	}
	if !snapshotWord.MatchString(e.Error()) && e.Code != provider.CodeNotFound {
		return err
	}
	p.log.Error("boxd could not restore a base image snapshot", "snapshot", snapshot)
	e.CanFailOver = true
	return e
}

// SaveImage saves the builder as a snapshot called name and returns once boxd says it is ready
func (p *Provider) SaveImage(
	ctx context.Context, sandbox provider.Ref, name string,
) (string, error) {
	id, err := machineID(sandbox)
	if err != nil {
		return "", err
	}
	if name == "" {
		return "", invalid(errors.New("a snapshot needs a name"))
	}
	// A second save under one name makes a new version of it, which live machines may restore
	_, err = p.api.GetSnapshot(ctx, &boxdpb.GetSnapshotRequest{Name: name})
	switch {
	case err == nil:
		return "", invalid(fmt.Errorf("boxd snapshot %s exists already", name))
	case status.Code(err) != codes.NotFound:
		return "", classify(ctx, err)
	}
	if _, err := p.api.CreateSnapshot(ctx, &boxdpb.CreateSnapshotRequest{
		Vm: id, Name: name,
	}); err != nil {
		return "", classify(ctx, err)
	}
	deadline := time.Now().Add(p.imageWait)
	for {
		got, err := p.api.GetSnapshot(ctx, &boxdpb.GetSnapshotRequest{Name: name})
		if err != nil {
			return "", classify(ctx, err)
		}
		switch st := got.GetSnapshot().GetStatus(); st {
		case "ready":
			return name, nil
		case "failed":
			return "", &provider.Error{Code: provider.CodeInternal,
				Err: fmt.Errorf("boxd could not save snapshot %s", name)}
		}
		if time.Now().After(deadline) {
			return "", &provider.Error{Code: provider.CodeInternal,
				Err: fmt.Errorf("boxd snapshot %s was not ready in %v", name, p.imageWait)}
		}
		if err := sleepCtx(ctx, p.imagePoll); err != nil {
			return "", fmt.Errorf("boxd snapshot %s not ready: %w", name, err)
		}
	}
}

// DeleteImage deletes a snapshot and treats one already gone as deleted
func (p *Provider) DeleteImage(ctx context.Context, ref string) error {
	_, err := p.api.DeleteSnapshot(ctx, &boxdpb.DeleteSnapshotRequest{Name: ref})
	if status.Code(err) == codes.NotFound {
		return nil
	}
	return classify(ctx, err)
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
