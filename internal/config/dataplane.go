package config

import (
	"cmp"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
)

// maxDataplaneWorkers is the most workers River lets one queue run
const maxDataplaneWorkers = 10_000

// dataPlane checks the data-plane tables of f and sets them on c
func (f file) dataPlane(c *Config) error {
	d := defaults()
	if err := checkValkeyAddr(c.ValkeyAddr); err != nil {
		return err
	}
	if w := f.Worker.DataplaneWorkers; w < 1 || w > maxDataplaneWorkers {
		return fmt.Errorf("worker.dataplane_workers must be 1 to %d", maxDataplaneWorkers)
	}
	c.DataplaneWorkers = int(f.Worker.DataplaneWorkers)
	c.Previews = f.Previews
	b, x, s, k := f.EdgeBudget, f.Transfers, f.SSH, f.Secrets
	for _, n := range []struct {
		key  string
		raw  int64
		into *int
	}{
		{"edge_budget.requests_per_second", b.RequestsPerSecond, &c.EdgeBudget.RequestsPerSecond},
		{"edge_budget.requests_burst", b.RequestsBurst, &c.EdgeBudget.RequestsBurst},
		{"edge_budget.queued_requests", b.QueuedRequests, &c.EdgeBudget.QueuedRequests},
		{"edge_budget.websockets", b.WebSockets, &c.EdgeBudget.WebSockets},
		{"edge_budget.egress_requests_in_flight", b.EgressRequestsInFlight,
			&c.EdgeBudget.EgressRequestsInFlight},
		{"previews.max_per_placement", int64(f.Previews.MaxPerPlacement),
			&c.Previews.MaxPerPlacement},
		{"transfers.max_active_per_placement", x.MaxActivePerPlacement,
			&c.Transfers.MaxActivePerPlacement},
		{"transfers.max_tokens_per_placement", x.MaxTokensPerPlacement,
			&c.Transfers.MaxTokensPerPlacement},
		{"transfers.max_claims", x.MaxClaims, &c.Transfers.MaxClaims},
		{"ssh.max_connections_per_placement", s.MaxConnectionsPerPlacement,
			&c.SSH.MaxConnectionsPerPlacement},
		{"ssh.max_keys_per_placement", s.MaxKeysPerPlacement, &c.SSH.MaxKeysPerPlacement},
		{"secrets.ca_version", k.CAVersion, &c.Secrets.CAVersion},
		{"secrets.max_per_placement", k.MaxPerPlacement, &c.Secrets.MaxPerPlacement},
		{"secrets.links_per_hour", k.LinksPerHour, &c.Secrets.LinksPerHour},
		{"secrets.puts_per_hour", k.PutsPerHour, &c.Secrets.PutsPerHour},
	} {
		if n.raw < 1 {
			return fmt.Errorf("%s must be a positive whole number", n.key)
		}
		*n.into = int(n.raw)
	}
	for _, p := range []struct {
		key  string
		raw  int64
		into *int
	}{
		{"names.ssh_port", int64(f.Names.SSHPort), &c.Names.SSHPort},
		{"previews.gate_port", int64(f.Previews.GatePort), &c.Previews.GatePort},
		{"ssh.sshd_port", s.SSHDPort, &c.SSH.SSHDPort},
		{"ssh.sshd_fallback_port", s.SSHDFallbackPort, &c.SSH.SSHDFallbackPort},
	} {
		if p.raw < 1 || p.raw > 65535 {
			return fmt.Errorf("%s must be a port from 1 to 65535", p.key)
		}
		*p.into = int(p.raw)
	}
	db, dx, df := d.EdgeBudget, d.Transfers, d.Fetch
	for _, z := range []struct {
		key, raw, def string
		into          *int64
	}{
		{"edge_budget.bytes_per_10_minutes", b.BytesPer10Minutes, db.BytesPer10Minutes,
			&c.EdgeBudget.BytesPer10Minutes},
		{"edge_budget.bytes_per_second", b.BytesPerSecond, db.BytesPerSecond,
			&c.EdgeBudget.BytesPerSecond},
		{"edge_budget.egress_bytes_per_10_minutes", b.EgressBytesPer10Minutes,
			db.EgressBytesPer10Minutes, &c.EdgeBudget.EgressBytesPer10Minutes},
		{"edge_budget.egress_bytes_per_second", b.EgressBytesPerSecond, db.EgressBytesPerSecond,
			&c.EdgeBudget.EgressBytesPerSecond},
		{"transfers.max_bytes", x.MaxBytes, dx.MaxBytes, &c.Transfers.MaxBytes},
		{"transfers.koyeb_part", x.KoyebPart, dx.KoyebPart, &c.Transfers.KoyebPart},
		{"transfers.koyeb_buffer_total", x.KoyebBufferTotal, dx.KoyebBufferTotal,
			&c.Transfers.KoyebBufferTotal},
		{"fetch.max_bytes", f.Fetch.MaxBytes, df.MaxBytes, &c.Fetch.MaxBytes},
	} {
		n, err := byteSize(z.key, cmp.Or(z.raw, z.def))
		if err != nil {
			return err
		}
		*z.into = n
	}
	ds, dk := d.SSH, d.Secrets
	for _, t := range []struct {
		key, raw, def string
		into          *time.Duration
	}{
		{"transfers.url_ttl", x.URLTTL, dx.URLTTL, &c.Transfers.URLTTL},
		{"transfers.max_duration", x.MaxDuration, dx.MaxDuration, &c.Transfers.MaxDuration},
		{"transfers.idle_timeout", x.IdleTimeout, dx.IdleTimeout, &c.Transfers.IdleTimeout},
		{"ssh.connection_lease", s.ConnectionLease, ds.ConnectionLease, &c.SSH.ConnectionLease},
		{"ssh.prepare_timeout", s.PrepareTimeout, ds.PrepareTimeout, &c.SSH.PrepareTimeout},
		{"ssh.install_timeout", s.InstallTimeout, ds.InstallTimeout, &c.SSH.InstallTimeout},
		{"secrets.link_ttl", k.LinkTTL, dk.LinkTTL, &c.Secrets.LinkTTL},
		{"secrets.put_grant_ttl", k.PutGrantTTL, dk.PutGrantTTL, &c.Secrets.PutGrantTTL},
		{"secrets.apply_wait", k.ApplyWait, dk.ApplyWait, &c.Secrets.ApplyWait},
	} {
		v, err := time.ParseDuration(cmp.Or(t.raw, t.def))
		if err != nil || v <= 0 {
			return fmt.Errorf("%s must be a positive Go duration such as 10m or 30s", t.key)
		}
		*t.into = v
	}
	if c.Transfers.KoyebPart > c.Transfers.KoyebBufferTotal {
		return errors.New("transfers.koyeb_part must not exceed transfers.koyeb_buffer_total")
	}
	if c.SSH.SSHDPort == c.SSH.SSHDFallbackPort {
		return errors.New("ssh.sshd_port and ssh.sshd_fallback_port must differ")
	}
	for _, l := range []struct {
		key  string
		raw  []string
		into *[]string
	}{
		{"previews.providers", f.Previews.Providers, &c.Previews.Providers},
		{"ssh.providers", s.Providers, &c.SSH.Providers},
		{"secrets.providers", k.Providers, &c.Secrets.Providers},
		{"hooks.providers", f.Hooks.Providers, &c.Hooks.Providers},
	} {
		names, err := featureList(l.key, l.raw)
		if err != nil {
			return err
		}
		*l.into = names
	}
	if slices.Contains(c.Secrets.Providers, "local") {
		return errors.New("secrets.providers may not list local, " +
			"whose sandboxes cannot run the egress forwarder")
	}
	if err := checkNames(c.Names); err != nil {
		return err
	}
	if err := c.SSH.readHostKeys(s.HostKeys); err != nil {
		return err
	}
	if err := c.Secrets.read(k); err != nil {
		return err
	}
	c.Fetch.EgressAddr = cmp.Or(f.Fetch.EgressAddr, df.EgressAddr)
	if !isHostPort(c.Fetch.EgressAddr) {
		return errors.New("fetch.egress_addr must be host:port with a port from 1 to 65535")
	}
	switch c.Boxd.GateRoute {
	case "subdomain", "default":
	default:
		return errors.New(`boxd.gate_route must be "subdomain" or "default"`)
	}
	switch c.Boat.SSHPath {
	case "gate", "native":
	default:
		return errors.New(`boat.ssh_path must be "gate" or "native"`)
	}
	return nil
}

// checkValkeyAddr accepts a host and port, or the unix scheme and an absolute socket path
func checkValkeyAddr(addr string) error {
	if socket, ok := strings.CutPrefix(addr, "unix://"); ok {
		if filepath.IsAbs(socket) {
			return nil
		}
	} else if isHostPort(addr) {
		return nil
	}
	return errors.New("valkey.addr must be host:port, or unix:// and an absolute socket path")
}

// isHostPort says whether addr is a host or address with a port from 1 to 65535
func isHostPort(addr string) bool {
	host, port, err := net.SplitHostPort(addr)
	n, perr := strconv.Atoi(port)
	return err == nil && host != "" && perr == nil && n >= 1 && n <= 65535
}

// featureList checks a list of the providers a data-plane feature passed its live test on
func featureList(key string, names []string) ([]string, error) {
	seen := map[string]bool{}
	for i, name := range names {
		switch {
		case name == "":
			return nil, fmt.Errorf("%s has an empty entry", key)
		case !knownProviders[name]:
			// An unknown entry is never quoted, since a secret could have been pasted there
			return nil, fmt.Errorf("%s entry %d is not one of %s", key, i+1, knownList())
		case seen[name]:
			return nil, fmt.Errorf("%s lists %q more than once", key, name)
		}
		seen[name] = true
	}
	if len(names) == 0 {
		return nil, nil
	}
	return slices.Clone(names), nil
}

// hostLabels is a lowercase DNS name of letters, digits and inner hyphens, with no final dot
var hostLabels = regexp.MustCompile(
	`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?(\.[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?)*$`)

// checkNames accepts an empty name, or a lowercase host name that is not an IP address
func checkNames(n Names) error {
	for _, h := range []struct{ key, name string }{
		{"names.preview_domain", n.PreviewDomain}, {"names.transfer_host", n.TransferHost},
		{"names.secrets_host", n.SecretsHost}, {"names.ssh_host", n.SSHHost},
		{"names.egress_host", n.EgressHost},
	} {
		if h.name == "" {
			continue
		}
		if _, err := netip.ParseAddr(h.name); err == nil || len(h.name) > 253 ||
			!hostLabels.MatchString(h.name) {
			return fmt.Errorf("%s must be a lowercase host name such as host.example.com, "+
				"with no scheme, port or final dot", h.key)
		}
	}
	return nil
}

// keyType is the first field of an OpenSSH public key line
var keyType = regexp.MustCompile(`^(ssh|ecdsa|sk)-[a-z0-9@.-]+$`)

// readHostKeys keeps lines of a key type and its base64, the form known_hosts takes
func (s *SSH) readHostKeys(lines []string) error {
	for i, line := range lines {
		fields := strings.Fields(line)
		ok := len(fields) >= 2 && keyType.MatchString(fields[0]) &&
			!strings.ContainsAny(line, "\r\n")
		if ok {
			_, err := base64.StdEncoding.DecodeString(fields[1])
			ok = err == nil
		}
		if !ok {
			return fmt.Errorf("ssh.host_keys entry %d must be a public key line such as "+
				"ssh-ed25519 and its base64", i+1)
		}
	}
	if len(lines) > 0 {
		s.HostKeys = slices.Clone(lines)
	}
	return nil
}

// keyID is what a box key id must look like
var keyID = regexp.MustCompile(`^[a-z0-9-]{1,32}$`)

// read checks the egress addresses and box keys of the secrets table
func (s *Secrets) read(f secretFile) error {
	s.CACertFile = f.CACertFile
	s.CustomHosts = f.CustomHosts
	for i, raw := range f.EgressIPs {
		ip, err := netip.ParseAddr(raw)
		if err != nil || ip.Zone() != "" {
			return fmt.Errorf("secrets.egress_ips entry %d must be an IP address", i+1)
		}
		if slices.Contains(s.EgressIPs, ip) {
			return fmt.Errorf("secrets.egress_ips lists %s more than once", ip)
		}
		s.EgressIPs = append(s.EgressIPs, ip)
	}
	for i, k := range f.BoxPublicKeys {
		if !keyID.MatchString(k.ID) {
			return fmt.Errorf("secrets.box_public_keys entry %d id must be 1 to 32 lowercase "+
				"letters, digits or hyphens", i+1)
		}
		if slices.ContainsFunc(s.BoxPublicKeys, func(b BoxKey) bool { return b.ID == k.ID }) {
			return fmt.Errorf("secrets.box_public_keys lists %q more than once", k.ID)
		}
		key, err := base64.StdEncoding.DecodeString(k.Key)
		if err != nil || len(key) != 32 {
			return fmt.Errorf("secrets.box_public_keys entry %d key must be base64 of 32 bytes",
				i+1)
		}
		s.BoxPublicKeys = append(s.BoxPublicKeys, BoxKey{ID: k.ID, Key: key})
	}
	return nil
}
