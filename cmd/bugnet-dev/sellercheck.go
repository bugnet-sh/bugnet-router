package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// The seller check's numbers (XS 6.17), the waits in units of the check's second
const (
	handshakeWait = 90
	holdWait      = 150
	sseEvents     = 36
	sseMaxGap     = 7
	uploadChunks  = 16
	uploadChunk   = 64 << 10
	parallelConns = 32
	burstRequests = 300
	burstWorkers  = 16
	// sseTotal is a stream's whole length, 36 events 5 s apart with a margin
	sseTotal = 200
)

// defaultResolvers are the public resolvers the check uses, as egress does
const defaultResolvers = "1.1.1.1:53,8.8.8.8:53,9.9.9.9:53"

// tailnetRange is the address range Tailscale uses inside its own network
var tailnetRange = netip.MustParsePrefix("100.64.0.0/10")

// sellerCheck is the test seller check, with its seams for tests
type sellerCheck struct {
	// second is the unit of every wait, one second in a real check
	second time.Duration
	// minute is how long check 5's 300 requests may take, one minute in a real check
	minute time.Duration
	// lookup resolves a name through public resolvers
	lookup func(ctx context.Context, host string) ([]netip.Addr, error)
	// addrOf is where to dial an address of host, port 443 in a real check
	addrOf func(host string, ip netip.Addr) string
	// slack is added to each gap between events and to the stream's length, none in a real check
	slack time.Duration
	// roots verifies certificates, the system roots when nil
	roots *x509.CertPool
	out   io.Writer
}

// checkFailure names the check that failed
type checkFailure struct {
	n      int
	title  string
	reason string
}

func (f *checkFailure) Error() string {
	return fmt.Sprintf("seller check %d (%s) failed: %s", f.n, f.title, f.reason)
}

// x402SellerCheck runs x402 seller-check
func (c *cli) x402SellerCheck(args []string) error {
	fs := newFlags("x402 seller-check")
	names := fs.String("names", "", "the two seller names, A,B")
	resolvers := fs.String("resolvers", defaultResolvers, "public resolvers, a host:port list")
	if err := parse(fs, args, false); err != nil {
		return err
	}
	if err := required("names", *names); err != nil {
		return err
	}
	pair := strings.Split(*names, ",")
	if len(pair) != 2 || pair[0] == "" || pair[1] == "" || pair[0] == pair[1] {
		return fmt.Errorf("%w: --names takes two different names, A,B", errUsage)
	}
	sc := c.seller
	if sc == nil {
		sc = &sellerCheck{
			second: time.Second, minute: time.Minute,
			lookup: publicLookup(strings.Split(*resolvers, ",")),
			addrOf: func(_ string, ip netip.Addr) string {
				return net.JoinHostPort(ip.String(), "443")
			},
		}
	}
	sc.out = c.stdout
	return sc.run(c.ctx, pair[0], pair[1])
}

// publicLookup resolves through each resolver and returns every address any of them gave
func publicLookup(resolvers []string) func(context.Context, string) ([]netip.Addr, error) {
	return func(ctx context.Context, host string) ([]netip.Addr, error) {
		var all []netip.Addr
		var errs []error
		for _, addr := range resolvers {
			r := &net.Resolver{PreferGo: true,
				Dial: func(ctx context.Context, _, _ string) (net.Conn, error) {
					return (&net.Dialer{Timeout: 5 * time.Second}).DialContext(ctx, "udp", addr)
				}}
			ips, err := r.LookupNetIP(ctx, "ip", host)
			if err != nil {
				errs = append(errs, err)
				continue
			}
			all = append(all, ips...)
		}
		if len(all) == 0 {
			return nil, fmt.Errorf("no public resolver answered: %w", errors.Join(errs...))
		}
		return all, nil
	}
}

// d is n units of the check's second
func (s *sellerCheck) d(n int) time.Duration { return time.Duration(n) * s.second }

// pass prints one check's line
func (s *sellerCheck) pass(n int, title, note string, since time.Time) {
	_, _ = fmt.Fprintf(s.out, "ok %d %s: %s (%s)\n", n, title, note,
		time.Since(since).Round(10*time.Millisecond))
}

// run is the five checks in order, 2 and 3 together, stopping at the first failure
func (s *sellerCheck) run(ctx context.Context, a, b string) error {
	if f := s.certificates(ctx, a, b); f != nil {
		return f
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	var holdFail, sseFail *checkFailure
	wg.Add(2)
	go func() {
		defer wg.Done()
		if holdFail = s.hold(ctx, a); holdFail != nil {
			cancel()
		}
	}()
	go func() {
		defer wg.Done()
		if sseFail = s.stream(ctx, a); sseFail != nil {
			cancel()
		}
	}()
	wg.Wait()
	if f := firstFailure(holdFail, sseFail); f != nil {
		return f
	}
	if f := s.early402(ctx, a); f != nil {
		return f
	}
	if f := s.burst(ctx, a, b); f != nil {
		return f
	}
	return nil
}

// firstFailure is the first failure that is not a stop caused by the other check's failure
func firstFailure(list ...*checkFailure) *checkFailure {
	var first *checkFailure
	for _, f := range list {
		switch {
		case f == nil:
		case !strings.Contains(f.reason, "context canceled"):
			return f
		case first == nil:
			first = f
		}
	}
	return first
}

// transport is a client transport that dials the name's public addresses and verifies its name
func (s *sellerCheck) transport() *http.Transport {
	return &http.Transport{
		DialContext: s.dial, ForceAttemptHTTP2: true,
		TLSClientConfig:   &tls.Config{RootCAs: s.roots, MinVersion: tls.VersionTLS12},
		DisableKeepAlives: false,
	}
}

// dial resolves the host, refuses any tailnet address and dials the first address
func (s *sellerCheck) dial(ctx context.Context, network, addr string) (net.Conn, error) {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	ips, err := s.lookup(ctx, host)
	if err != nil {
		return nil, err
	}
	for _, ip := range ips {
		if tailnetRange.Contains(ip.Unmap()) {
			return nil, fmt.Errorf("%s resolves to %s, a Tailscale address", host, ip)
		}
	}
	var last error
	for _, ip := range ips {
		d := &net.Dialer{Timeout: 10 * time.Second}
		conn, err := d.DialContext(ctx, network, s.addrOf(host, ip))
		if err == nil {
			return conn, nil
		}
		last = err
	}
	return nil, last
}

// certificates is check 1, each name's first handshake within 90 s
func (s *sellerCheck) certificates(ctx context.Context, names ...string) *checkFailure {
	start := time.Now()
	for _, name := range names {
		deadline := time.Now().Add(s.d(handshakeWait))
		var err error
		for {
			if err = s.handshake(ctx, name); err == nil || time.Now().After(deadline) {
				break
			}
			select {
			case <-ctx.Done():
				return &checkFailure{1, "certificate", ctx.Err().Error()}
			case <-time.After(s.d(1)):
			}
		}
		if err != nil {
			return &checkFailure{1, "certificate", fmt.Sprintf("%s: %v", name, err)}
		}
	}
	s.pass(1, "certificate", "verified for "+strings.Join(names, " and "), start)
	return nil
}

// handshake dials the name and verifies its certificate for that name
func (s *sellerCheck) handshake(ctx context.Context, name string) error {
	raw, err := s.dial(ctx, "tcp", net.JoinHostPort(name, "443"))
	if err != nil {
		return err
	}
	conn := tls.Client(raw, &tls.Config{
		ServerName: name, RootCAs: s.roots, MinVersion: tls.VersionTLS12,
	})
	defer func() { _ = conn.Close() }()
	hs, cancel := context.WithTimeout(ctx, s.d(handshakeWait))
	defer cancel()
	return conn.HandshakeContext(hs)
}

// get sends GET https://name/path on a connection of its own
func (s *sellerCheck) get(ctx context.Context, name, path string) (*http.Response, func(), error) {
	tr := s.transport()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://"+name+path, nil)
	if err != nil {
		return nil, nil, err
	}
	res, err := (&http.Client{Transport: tr}).Do(req)
	if err != nil {
		tr.CloseIdleConnections()
		return nil, nil, err
	}
	return res, func() { _ = res.Body.Close(); tr.CloseIdleConnections() }, nil
}

// hold is check 2, an answer held silent for 150 s
func (s *sellerCheck) hold(ctx context.Context, name string) *checkFailure {
	start := time.Now()
	res, done, err := s.get(ctx, name, "/hold/150/")
	if err != nil {
		return &checkFailure{2, "silent hold", err.Error()}
	}
	defer done()
	waited := time.Since(start)
	if res.StatusCode != http.StatusOK {
		return &checkFailure{2, "silent hold", fmt.Sprintf("status %d", res.StatusCode)}
	}
	if waited < s.d(holdWait) {
		reason := fmt.Sprintf("answered after %s, before the hold ended",
			waited.Round(time.Millisecond))
		return &checkFailure{2, "silent hold", reason}
	}
	_, _ = io.Copy(io.Discard, res.Body)
	s.pass(2, "silent hold", "answered 200 after the hold", start)
	return nil
}

// stream is check 3, 36 server-sent events, each within 7 s of the one before
func (s *sellerCheck) stream(ctx context.Context, name string) *checkFailure {
	start := time.Now()
	ctx, cancel := context.WithTimeout(ctx, s.d(sseTotal)+s.slack)
	defer cancel()
	res, done, err := s.get(ctx, name, "/sse/")
	if err != nil {
		return &checkFailure{3, "event stream", err.Error()}
	}
	defer done()
	if res.StatusCode != http.StatusOK {
		return &checkFailure{3, "event stream", fmt.Sprintf("status %d", res.StatusCode)}
	}
	last, events := start, 0
	sc := bufio.NewScanner(res.Body)
	for events < sseEvents && sc.Scan() {
		if !strings.HasPrefix(sc.Text(), "data:") {
			continue
		}
		now := time.Now()
		if gap := now.Sub(last); gap > s.d(sseMaxGap)+s.slack {
			reason := fmt.Sprintf("event %d came %s after the one before", events+1,
				gap.Round(time.Millisecond))
			return &checkFailure{3, "event stream", reason}
		}
		last = now
		events++
	}
	if events < sseEvents {
		reason := fmt.Sprintf("only %d of %d events", events, sseEvents)
		return &checkFailure{3, "event stream", reason}
	}
	s.pass(3, "event stream", fmt.Sprintf("%d events in time", events), start)
	return nil
}

// early402 is check 4, a 1 MiB upload at 64 KiB a second that gets its 402 before it ends
func (s *sellerCheck) early402(ctx context.Context, name string) *checkFailure {
	start := time.Now()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	pr, pw := io.Pipe()
	var sent atomic.Int64
	go func() {
		defer func() { _ = pw.Close() }()
		chunk := make([]byte, uploadChunk)
		for i := 0; i < uploadChunks; i++ {
			if i > 0 {
				select {
				case <-time.After(s.second):
				case <-ctx.Done():
					return
				}
			}
			if _, err := pw.Write(chunk); err != nil {
				return
			}
			sent.Add(uploadChunk)
		}
	}()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://"+name+"/cent/x", pr)
	if err != nil {
		return &checkFailure{4, "early 402", err.Error()}
	}
	req.ContentLength = uploadChunks * uploadChunk
	tr := s.transport()
	defer tr.CloseIdleConnections()
	res, err := (&http.Client{Transport: tr}).Do(req)
	if err != nil {
		return &checkFailure{4, "early 402", err.Error()}
	}
	defer func() { _ = res.Body.Close() }()
	got := sent.Load()
	if res.StatusCode != http.StatusPaymentRequired {
		return &checkFailure{4, "early 402", fmt.Sprintf("status %d, not 402", res.StatusCode)}
	}
	if got >= uploadChunks*uploadChunk {
		return &checkFailure{4, "early 402", "the 402 came after the last byte was sent"}
	}
	note := fmt.Sprintf("402 after %d of %d bytes sent", got, uploadChunks*uploadChunk)
	s.pass(4, "early 402", note, start)
	return nil
}

// burst is check 5, 32 parallel connections, then 300 requests in a minute, then name B
func (s *sellerCheck) burst(ctx context.Context, a, b string) *checkFailure {
	start := time.Now()
	fail := func(reason string) *checkFailure {
		return &checkFailure{5, "parallel requests", reason}
	}
	errs := make(chan error, parallelConns)
	var wg sync.WaitGroup
	gate := make(chan struct{})
	for i := 0; i < parallelConns; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-gate
			errs <- s.free(ctx, a, s.transport())
		}()
	}
	close(gate)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			return fail("one of 32 parallel requests failed: " + err.Error())
		}
	}
	tr := s.transport()
	defer tr.CloseIdleConnections()
	ctx300, cancel := context.WithTimeout(ctx, s.minute)
	defer cancel()
	var next atomic.Int64
	failed := make(chan error, burstWorkers)
	for w := 0; w < burstWorkers; w++ {
		go func() {
			for next.Add(1) <= burstRequests {
				if err := s.free(ctx300, a, tr); err != nil {
					failed <- err
					return
				}
			}
			failed <- nil
		}()
	}
	for w := 0; w < burstWorkers; w++ {
		if err := <-failed; err != nil {
			cancel()
			return fail("a request of the 300 failed: " + err.Error())
		}
	}
	if err := s.free(ctx, b, s.transport()); err != nil {
		return fail("the second name failed: " + err.Error())
	}
	s.pass(5, "parallel requests", "32 at once, 300 in a minute, and the second name", start)
	return nil
}

// free sends GET /free/ on tr and wants a 200
func (s *sellerCheck) free(ctx context.Context, name string, tr *http.Transport) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://"+name+"/free/", nil)
	if err != nil {
		return err
	}
	res, err := (&http.Client{Transport: tr}).Do(req)
	if err != nil {
		return err
	}
	_, _ = io.Copy(io.Discard, res.Body)
	_ = res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("status %d", res.StatusCode)
	}
	return nil
}
