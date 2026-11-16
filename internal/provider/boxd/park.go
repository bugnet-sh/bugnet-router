package boxd

import (
	"context"
	"fmt"
	"io"
	"regexp"
	"slices"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/provider/boxd/boxdpb"
)

const (
	// wakePing spaces the runner pings after a wake
	wakePing = 25 * time.Millisecond
	// wakeWithin is the longest a wake may take before Unpark gives up
	wakeWithin = 10 * time.Second
	// parkedEURMicrosPerGiBHour is boxd's price of a hibernated machine's disk, EUR 0.00008
	parkedEURMicrosPerGiBHour = 80
	// defaultParkedGiB is the disk written per hibernated member when Options give none
	defaultParkedGiB = 5
)

// runnerPing is the runner's ping, where the router puts bugnet-run in every boxd machine
var runnerPing = []string{"/tmp/bugnet/bin/bugnet-run", "ping"}

// alreadyIn matches boxd's refusal of a hibernate or wake the machine is already in
var alreadyIn = regexp.MustCompile(`(?i)\balready\b`)

// Park hibernates the machine, which keeps its disk and memory and bills the disk alone
func (p *Provider) Park(ctx context.Context, ref provider.Ref) error {
	vmID, err := machineID(ref)
	if err != nil {
		return err
	}
	_, err = p.api.HibernateVm(ctx, &boxdpb.HibernateVmRequest{VmId: vmID})
	// A machine boxd already hibernated is parked
	if refusedAsRedundant(err) {
		return nil
	}
	return classify(ctx, err)
}

// Unpark wakes the machine and pings the runner every 25 ms for up to 10 s
func (p *Provider) Unpark(ctx context.Context, ref provider.Ref) error {
	vmID, err := machineID(ref)
	if err != nil {
		return err
	}
	_, err = p.api.WakeVm(ctx, &boxdpb.WakeVmRequest{VmId: vmID})
	if err != nil && !refusedAsRedundant(err) {
		return classify(ctx, err)
	}
	ctx, cancel := context.WithTimeout(ctx, wakeWithin)
	defer cancel()
	for {
		err := p.ping(ctx, ref)
		if err == nil {
			return nil
		}
		if e, ok := provider.AsError(err); ok && (e.Code == provider.CodeNotFound ||
			e.Code == provider.CodeAuth || e.Code == provider.CodeInvalid) {
			return err
		}
		t := time.NewTimer(wakePing)
		select {
		case <-ctx.Done():
			t.Stop()
			return &provider.Error{
				Code: provider.CodeInternal, CanFailOver: true,
				Err: fmt.Errorf("boxd machine %s did not answer after a wake: %w", vmID, err),
			}
		case <-t.C:
		}
	}
}

// refusedAsRedundant reports a hibernate or wake that boxd refused since it was already done
func refusedAsRedundant(err error) bool {
	st, _ := status.FromError(err)
	return st.Code() == codes.FailedPrecondition && alreadyIn.MatchString(st.Message())
}

// ping runs the runner's ping, which succeeds once the machine's agent and disk answer
func (p *Provider) ping(ctx context.Context, ref provider.Ref) error {
	argv := p.pingArgv
	// A machine that booted with a baked runner keeps it under the path its ref names
	if r := ref.Meta["runner"]; r != "" && slices.Equal(argv, runnerPing) {
		argv = []string{r, "ping"}
	}
	stream, err := p.Exec(ctx, ref, provider.ExecSpec{Argv: argv})
	if err != nil {
		return err
	}
	defer func() { _ = stream.Close() }()
	if _, err := io.Copy(io.Discard, stream); err != nil {
		return err
	}
	code, err := stream.Wait()
	if err == nil && code != 0 {
		err = fmt.Errorf("runner ping exited %d", code)
	}
	return err
}

// ParkedHourlyCostMicros is the disk written per parked member at boxd's rate, in dollars
func (p *Provider) ParkedHourlyCostMicros(provider.Size) int64 {
	eur := int64(p.parkedGiB) * parkedEURMicrosPerGiBHour
	return p.usd(eur)
}
