package main

import (
	"bytes"
	"errors"
	"fmt"
	"net/netip"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"text/template"

	"github.com/pelletier/go-toml/v2"

	"github.com/oynozan/bugnet-router/deploy/vps"
	"github.com/oynozan/bugnet-router/internal/hostname"
)

/* Deploy */

// site is deploy/vps/site.toml, the VPS's facts that are not secret and the founder's answers
type site struct {
	IPv4         string   `toml:"ipv4"`
	IPv6         string   `toml:"ipv6"`
	AdminSSHPort int      `toml:"admin_ssh_port"`
	SSHGatewayIP string   `toml:"ssh_gateway_ip"`
	EgressIP     string   `toml:"egress_ip"`
	DNSProvider  string   `toml:"dns_provider"`
	DNSEnv       []string `toml:"dns_env"`
}

// publicName is one line of deploy/vps/names.toml
type publicName struct {
	Name    string `toml:"name"`
	Kind    string `toml:"kind"`
	Backend string `toml:"backend"`
	Cert    string `toml:"cert"`
}

// names is deploy/vps/names.toml, each set in the order its map lines take
type names struct {
	Production []publicName `toml:"production"`
	Dev        []publicName `toml:"dev"`
}

const (
	defaultDNSProvider = "cloudflare"
	certsRoot          = "/var/lib/bugnet-certs"
	acmeProduction     = "https://acme-v02.api.letsencrypt.org/directory"
	acmeStaging        = "https://acme-staging-v02.api.letsencrypt.org/directory"
	// tlsMaxconn is frontend tls's share of the global 20,000, split with egress_ip's when set
	tlsMaxconn = 14000
)

// defaultDNSEnv is what the cloudflare provider reads
var defaultDNSEnv = []string{"CF_DNS_API_TOKEN"}

// sniBackends are the backends a TLS name may go to, each defined in haproxy.cfg.tmpl
var sniBackends = []string{
	"edge", "secrets", "egress", "mcp", "website", "edge_dev", "secrets_dev", "egress_dev",
}

// frontPorts are the ports HAProxy and the edge take on the VPS, so admin SSH cannot use them
var frontPorts = []int{80, 443, 2200}

var (
	providerName = regexp.MustCompile(`^[a-z0-9][a-z0-9.-]*$`)
	envName      = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
	certName     = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,62}$`)
	// reservedCerts are the directories under certsRoot that hold no live pair
	reservedCerts = []string{"acme", "staging"}
)

// deploy runs one deploy subcommand, maps today
func (c *cli) deploy(args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("%w: deploy needs a subcommand, maps", errUsage)
	}
	if args[0] != "maps" {
		return fmt.Errorf("%w: unknown deploy subcommand %q", errUsage, args[0])
	}
	fs := newFlags("deploy maps")
	namesPath := fs.String("names", "", "names.toml")
	sitePath := fs.String("site", "", "site.toml")
	out := fs.String("out", "", "directory to write into")
	front := fs.Bool("front", false, "the dev front, without frontend ssh or production wildcards")
	if err := parse(fs, args[1:], false); err != nil {
		return err
	}
	for _, f := range [][2]string{{"names", *namesPath}, {"site", *sitePath}, {"out", *out}} {
		if err := required(f[0], f[1]); err != nil {
			return err
		}
	}
	files, err := deployFiles(*namesPath, *sitePath, *front)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(*out, 0o755); err != nil {
		return err
	}
	var wrote [][2]string
	for _, f := range files {
		path := filepath.Join(*out, f.name)
		if err := os.WriteFile(path, []byte(f.body), 0o644); err != nil {
			return err
		}
		if err := os.Chmod(path, 0o644); err != nil {
			return err
		}
		wrote = append(wrote, [2]string{"wrote", path})
	}
	return c.print(wrote)
}

// deployFile is one file deploy maps writes
type deployFile struct {
	name string
	body string
}

// deployFiles renders the maps, haproxy.cfg, bugnet-certs' settings and deploy.env
func deployFiles(namesPath, sitePath string, front bool) ([]deployFile, error) {
	n, err := loadNames(namesPath)
	if err != nil {
		return nil, err
	}
	s, err := loadSite(sitePath)
	if err != nil {
		return nil, err
	}
	if err := s.usable(sitePath, front); err != nil {
		return nil, err
	}
	cfg, err := haproxyConfig(s, front)
	if err != nil {
		return nil, err
	}
	all := slices.Concat(n.Production, n.Dev)
	wild := n.Dev
	if !front {
		wild = all
	}
	return []deployFile{
		{"sni.map", nameMap("# /etc/haproxy/sni.map, exact names\n", all, "exact", "")},
		{"end.map", nameMap("# /etc/haproxy/end.map, suffixes\n", all, "suffix", ".")},
		{"haproxy.cfg", cfg},
		{"certs.toml", certsSettings(s, wild, false, front)},
		{"certs-staging.toml", certsSettings(s, wild, true, front)},
		{"deploy.env", deployEnv(s, n, front)},
	}, nil
}

// nameMap writes one HAProxy map, its keys padded to the column DP 15.2 shows
func nameMap(header string, all []publicName, kind, prefix string) string {
	var b strings.Builder
	b.WriteString(header)
	for _, p := range all {
		if p.Kind == kind {
			fmt.Fprintf(&b, "%-27s %s\n", prefix+p.Name, p.Backend)
		}
	}
	return b.String()
}

// haproxyData fills haproxy.cfg.tmpl
type haproxyData struct {
	TLSBinds      []string
	TLSMaxconn    int
	EgressIP      string
	EgressMaxconn int
	SSHBinds      []string
}

func haproxyConfig(s site, front bool) (string, error) {
	d := haproxyData{TLSBinds: []string{":443", ":::443 v6only"}, TLSMaxconn: tlsMaxconn}
	if s.EgressIP != "" {
		// The proxy's own address takes its 443, so the split binds only the shared address
		d.TLSBinds[0] = s.IPv4 + ":443"
		d.EgressIP = s.EgressIP
		d.TLSMaxconn, d.EgressMaxconn = tlsMaxconn/2, tlsMaxconn/2
	}
	switch {
	case front:
	case s.SSHGatewayIP != "":
		d.SSHBinds = []string{s.SSHGatewayIP + ":22"}
	default:
		d.SSHBinds = []string{":22", ":::22 v6only"}
	}
	t, err := template.New("haproxy.cfg").Option("missingkey=error").Parse(vps.HAProxyTemplate)
	if err != nil {
		return "", err
	}
	var b bytes.Buffer
	if err := t.Execute(&b, d); err != nil {
		return "", err
	}
	return b.String(), nil
}

// certsSettings is bugnet-certs' settings file for the wildcards of wild
func certsSettings(s site, wild []publicName, staging, front bool) string {
	var b strings.Builder
	directory := acmeProduction
	if staging {
		directory = acmeStaging
		b.WriteString(stagingHeader)
	} else {
		b.WriteString(certsHeader)
	}
	fmt.Fprintf(&b, "\ndirectory = %s\ndns_provider = %s\ndns_env = %s\n",
		strconv.Quote(directory), strconv.Quote(s.DNSProvider), quoted(s.DNSEnv))
	if staging || front {
		// No edge runs beside the dev front, and a staging run checks only its own pairs
		b.WriteString("cert_metrics = []\n")
	}
	for _, p := range wild {
		if p.Kind == "suffix" {
			fmt.Fprintf(&b, "\n[[certificates]]\nnames = [%s]\ndir = %s\n",
				strconv.Quote("*."+p.Name), strconv.Quote(certDir(p.Cert, staging)))
		}
	}
	return b.String()
}

const (
	certsHeader = "# /etc/bugnet/certs.toml, written by bugnet-dev deploy maps from names.toml " +
		"and site.toml\n# The DNS token goes in /etc/bugnet/certs.env under the names in " +
		"dns_env, and nowhere else\n"
	stagingHeader = "# /etc/bugnet/certs-staging.toml, for one test run against Let's Encrypt " +
		"staging\n# Its pairs go to their own directories, so a staging pair never stands in " +
		"for a real one\n"
)

func certDir(cert string, staging bool) string {
	if staging {
		return certsRoot + "/staging/" + cert
	}
	return certsRoot + "/" + cert
}

func quoted(list []string) string {
	q := make([]string, len(list))
	for i, s := range list {
		q[i] = strconv.Quote(s)
	}
	return "[" + strings.Join(q, ", ") + "]"
}

// deployEnv holds what front.sh needs from site.toml and names.toml, one KEY=value a line
func deployEnv(s site, n names, front bool) string {
	dirs := func(set []publicName, staging bool) string {
		var out []string
		for _, p := range set {
			if p.Kind == "suffix" {
				out = append(out, certDir(p.Cert, staging))
			}
		}
		return strings.Join(out, " ")
	}
	production, staging := dirs(n.Production, false), dirs(n.Dev, true)
	if front {
		production = ""
	} else {
		staging = dirs(slices.Concat(n.Production, n.Dev), true)
	}
	return fmt.Sprintf("# What front.sh reads, from names.toml and site.toml\n"+
		"ADMIN_SSH_PORT=%d\nPRODUCTION_CERT_DIRS=%s\nDEV_CERT_DIRS=%s\nSTAGING_CERT_DIRS=%s\n",
		s.AdminSSHPort, production, dirs(n.Dev, false), staging)
}

/* Deploy settings files */

// decodeStrict decodes the TOML file at path into v, naming the line of an unknown key
func decodeStrict(path string, v any) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	dec := toml.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	err = dec.Decode(v)
	var missing *toml.StrictMissingError
	if errors.As(err, &missing) {
		var out []string
		for _, e := range missing.Errors {
			row, _ := e.Position()
			out = append(out, fmt.Sprintf("line %d: unknown key %s", row,
				strings.Join(e.Key(), ".")))
		}
		return fmt.Errorf("%s: %s", path, strings.Join(out, ", "))
	}
	var de *toml.DecodeError
	if errors.As(err, &de) {
		row, _ := de.Position()
		lines := strings.Split(string(raw), "\n")
		key := ""
		if row >= 1 && row <= len(lines) {
			key, _, _ = strings.Cut(lines[row-1], "=")
		}
		return fmt.Errorf("%s: line %d: %s: %s", path, row, strings.TrimSpace(key),
			strings.TrimPrefix(de.Error(), "toml: "))
	}
	if err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	return nil
}

// loadSite reads site.toml strictly over its defaults and checks each value it holds
func loadSite(path string) (site, error) {
	s := site{
		AdminSSHPort: 2222, DNSProvider: defaultDNSProvider, DNSEnv: slices.Clone(defaultDNSEnv),
	}
	if err := decodeStrict(path, &s); err != nil {
		return site{}, err
	}
	if err := s.check(); err != nil {
		return site{}, fmt.Errorf("%s: %w", path, err)
	}
	return s, nil
}

// ipv4 is a plain IPv4 address, never IPv4 inside IPv6
func ipv4(s string) bool {
	a, err := netip.ParseAddr(s)
	return err == nil && a.Is4()
}

func (s site) check() error {
	ip6, err := netip.ParseAddr(s.IPv6)
	switch {
	case s.IPv4 != "" && !ipv4(s.IPv4):
		return errors.New("ipv4 must be an IPv4 address such as 203.0.113.7, or empty")
	case s.IPv6 != "" && (err != nil || !ip6.Is6() || ip6.Is4In6() || ip6.Zone() != ""):
		return errors.New("ipv6 must be an IPv6 address such as 2001:db8::7, or empty")
	case s.AdminSSHPort < 1 || s.AdminSSHPort > 65535:
		return errors.New("admin_ssh_port must be 1 to 65535")
	case slices.Contains(frontPorts, s.AdminSSHPort):
		return fmt.Errorf("admin_ssh_port %d is a port the front serves", s.AdminSSHPort)
	case s.SSHGatewayIP != "" && !ipv4(s.SSHGatewayIP):
		return errors.New("ssh_gateway_ip must be an IPv4 address, or empty")
	case s.SSHGatewayIP != "" && s.SSHGatewayIP == s.IPv4:
		return errors.New("ssh_gateway_ip must be a second address, not ipv4")
	case s.EgressIP != "" && !ipv4(s.EgressIP):
		return errors.New("egress_ip must be an IPv4 address, or empty")
	case s.EgressIP != "" && s.EgressIP == s.IPv4:
		return errors.New("egress_ip must be the proxy's own address, not ipv4")
	case !providerName.MatchString(s.DNSProvider):
		return errors.New("dns_provider must be a lego provider name such as cloudflare")
	}
	if len(s.DNSEnv) == 0 ||
		s.DNSProvider != defaultDNSProvider && slices.Equal(s.DNSEnv, defaultDNSEnv) {
		return fmt.Errorf("dns_env must list the variables %s reads, such as CF_DNS_API_TOKEN",
			s.DNSProvider)
	}
	for i, name := range s.DNSEnv {
		if !envName.MatchString(name) {
			return fmt.Errorf("dns_env entry %d must be a variable name", i+1)
		}
	}
	return nil
}

// usable checks what rendering needs beyond a valid file
func (s site) usable(path string, front bool) error {
	switch {
	case s.IPv4 == "":
		return fmt.Errorf("%s: ipv4 is empty, write the VPS's address there first", path)
	case !front && s.AdminSSHPort == 22 && s.SSHGatewayIP == "":
		return fmt.Errorf("%s: admin SSH on 22 needs ssh_gateway_ip, the gateway's own "+
			"address (DP question 1 b), or admin SSH on another port (a)", path)
	}
	return nil
}

// loadNames reads names.toml strictly and checks every line
func loadNames(path string) (names, error) {
	var n names
	if err := decodeStrict(path, &n); err != nil {
		return names{}, err
	}
	if err := n.check(); err != nil {
		return names{}, fmt.Errorf("%s: %w", path, err)
	}
	return n, nil
}

func (n names) check() error {
	seen := map[string]bool{}
	certs := map[string]bool{}
	for _, set := range []struct {
		name  string
		lines []publicName
	}{{"production", n.Production}, {"dev", n.Dev}} {
		for i, p := range set.lines {
			at := fmt.Sprintf("%s entry %d", set.name, i+1)
			switch {
			case !hostname.Valid(p.Name):
				return fmt.Errorf("%s: name must be a lowercase host name, with no * or "+
					"leading dot", at)
			case p.Kind != "exact" && p.Kind != "suffix":
				return fmt.Errorf("%s: kind must be exact or suffix", at)
			case !slices.Contains(sniBackends, p.Backend):
				return fmt.Errorf("%s: backend must be one of %s", at,
					strings.Join(sniBackends, ", "))
			case seen[p.Kind+" "+p.Name]:
				return fmt.Errorf("%s: %s is listed twice as %s", at, p.Name, p.Kind)
			case p.Kind == "exact" && p.Cert != "":
				return fmt.Errorf("%s: cert is only for a suffix", at)
			case p.Kind == "suffix" && (!certName.MatchString(p.Cert) ||
				slices.Contains(reservedCerts, p.Cert)):
				return fmt.Errorf("%s: a suffix needs cert, its directory's name in lower "+
					"case, not %s", at, strings.Join(reservedCerts, " or "))
			case p.Kind == "suffix" && certs[p.Cert]:
				return fmt.Errorf("%s: cert %s is used twice", at, p.Cert)
			}
			seen[p.Kind+" "+p.Name] = true
			if p.Kind == "suffix" {
				certs[p.Cert] = true
			}
		}
	}
	if len(seen) == 0 {
		return errors.New("lists no name")
	}
	return nil
}
