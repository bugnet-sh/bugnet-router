package config

import (
	"encoding/base64"
	"net/netip"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/oynozan/bugnet-router/internal/provider"
)

// loadExample loads config/example.toml with an order token key, as a deployment fills it in
func loadExample(t *testing.T) Config {
	t.Helper()
	content := strings.Replace(committed(t, "example.toml"), `order_token_public_key = ""`,
		`order_token_public_key = "`+pubB64(t)+`"`, 1)
	c, err := LoadFile(writeFile(t, content))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestDataPlaneDefaults(t *testing.T) {
	empty, err := LoadFile(writeFile(t, ""))
	if err != nil {
		t.Fatal(err)
	}
	for name, c := range map[string]Config{"example.toml": loadExample(t), "empty": empty} {
		t.Run(name, func(t *testing.T) {
			eq(t, c.Previews.GatePort, 29417)
			eq(t, c.Previews.GatePort, provider.DefaultGatePort)
			eq(t, c.Previews.MaxPerPlacement, 8)
			eq(t, c.Previews.Interstitial, true)
			eq(t, c.Previews.Providers, []string(nil))
			eq(t, c.EdgeBudget, EdgeBudget{
				BytesPer10Minutes: 2 << 30, BytesPerSecond: 5 << 20, RequestsPerSecond: 100,
				RequestsBurst: 200, QueuedRequests: 64, WebSockets: 32,
				EgressBytesPer10Minutes: 2 << 30, EgressBytesPerSecond: 2 << 20,
				EgressRequestsInFlight: 32,
			})
			eq(t, c.Transfers, Transfers{
				URLTTL: 10 * time.Minute, MaxBytes: 1 << 30, MaxDuration: 30 * time.Minute,
				IdleTimeout: time.Minute, MaxActivePerPlacement: 2, MaxTokensPerPlacement: 16,
				MaxClaims: 5, KoyebPart: 8 << 20, KoyebBufferTotal: 256 << 20,
			})
			eq(t, c.SSH, SSH{
				SSHDPort: 22, SSHDFallbackPort: 2022, MaxConnectionsPerPlacement: 8,
				MaxKeysPerPlacement: 4, ConnectionLease: time.Minute,
				PrepareTimeout: 40 * time.Second, InstallTimeout: 2 * time.Minute,
			})
			eq(t, c.Secrets, Secrets{
				CAVersion: 1, LinkTTL: 10 * time.Minute, PutGrantTTL: 10 * time.Minute,
				CustomHosts: true, MaxPerPlacement: 20, LinksPerHour: 10, PutsPerHour: 30,
				ApplyWait: 15 * time.Second,
			})
			eq(t, c.Fetch, Fetch{MaxBytes: 2 << 30, EgressAddr: "127.0.0.1:9443"})
			eq(t, c.Hooks, Hooks{})
			eq(t, c.DataplaneWorkers, 8)
			eq(t, c.Boxd.GateRoute, "subdomain")
			eq(t, c.Boat.SSHPath, "gate")
			eq(t, c.LocalSSHDPath, "/usr/sbin/sshd")
			eq(t, c.ValkeyAddr, "127.0.0.1:6379")
		})
	}
	eq(t, empty.Names, Names{SSHPort: 22})
}

func TestExampleNamesAreTheProductionSet(t *testing.T) {
	eq(t, loadExample(t).Names, Names{
		PreviewDomain: "preview.bugnet.sh", TransferHost: "transfer.bugnet.sh",
		SecretsHost: "secrets.bugnet.sh", SSHHost: "ssh.bugnet.sh", SSHPort: 22,
		EgressHost: "egress.bugnet.sh",
	})
}

func TestDataPlaneSettings(t *testing.T) {
	key := base64.StdEncoding.EncodeToString(make([]byte, 32))
	other := base64.StdEncoding.EncodeToString([]byte(strings.Repeat("k", 32)))
	c, err := LoadFile(writeFile(t, `[valkey]
addr = "unix:///run/valkey/valkey.sock"

[worker]
dataplane_workers = 3

[names]
preview_domain = "preview.dev.example.com"
transfer_host = "transfer.dev.example.com"
secrets_host = "secrets.dev.example.com"
ssh_host = "ssh.dev.example.com"
ssh_port = 2200
egress_host = "egress.dev.example.com"

[edge_budget]
bytes_per_10_minutes = "4GiB"
bytes_per_second = "10MiB"
requests_per_second = 1
requests_burst = 2
queued_requests = 3
websockets = 4
egress_bytes_per_10_minutes = "1GiB"
egress_bytes_per_second = "512KiB"
egress_requests_in_flight = 5

[previews]
providers = ["local", "e2b"]
gate_port = 29418
max_per_placement = 2
interstitial = false

[transfers]
url_ttl = "5m"
max_bytes = "2GiB"
max_duration = "1h"
idle_timeout = "30s"
max_active_per_placement = 3
max_tokens_per_placement = 4
max_claims = 6
koyeb_part = "4MiB"
koyeb_buffer_total = "64MiB"

[ssh]
providers = ["boat"]
host_keys = ["ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIBN0 gateway"]
sshd_port = 2222
sshd_fallback_port = 2023
max_connections_per_placement = 9
max_keys_per_placement = 5
connection_lease = "2m"
prepare_timeout = "45s"
install_timeout = "90s"

[secrets]
providers = ["e2b"]
egress_ips = ["203.0.113.7", "2001:db8::7"]
ca_cert_file = "/etc/bugnet/egress-ca.pem"
ca_version = 2
box_public_keys = [{ id = "k2", key = "`+key+`" }, { id = "k1", key = "`+other+`" }]
link_ttl = "15m"
put_grant_ttl = "5m"
custom_hosts = false
max_per_placement = 7
links_per_hour = 11
puts_per_hour = 12
apply_wait = "20s"

[fetch]
max_bytes = "3GiB"
egress_addr = "[::1]:9444"

[hooks]
providers = ["e2b"]

[local]
sshd_path = "/opt/sshd"

[boxd]
gate_route = "default"

[boat]
ssh_path = "native"
`))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, c.ValkeyAddr, "unix:///run/valkey/valkey.sock")
	eq(t, c.DataplaneWorkers, 3)
	eq(t, c.Names, Names{
		PreviewDomain: "preview.dev.example.com", TransferHost: "transfer.dev.example.com",
		SecretsHost: "secrets.dev.example.com", SSHHost: "ssh.dev.example.com", SSHPort: 2200,
		EgressHost: "egress.dev.example.com",
	})
	eq(t, c.EdgeBudget, EdgeBudget{
		BytesPer10Minutes: 4 << 30, BytesPerSecond: 10 << 20, RequestsPerSecond: 1,
		RequestsBurst: 2, QueuedRequests: 3, WebSockets: 4, EgressBytesPer10Minutes: 1 << 30,
		EgressBytesPerSecond: 512 << 10, EgressRequestsInFlight: 5,
	})
	eq(t, c.Previews, Previews{
		Providers: []string{"local", "e2b"}, GatePort: 29418, MaxPerPlacement: 2,
	})
	eq(t, c.Transfers, Transfers{
		URLTTL: 5 * time.Minute, MaxBytes: 2 << 30, MaxDuration: time.Hour,
		IdleTimeout: 30 * time.Second, MaxActivePerPlacement: 3, MaxTokensPerPlacement: 4,
		MaxClaims: 6, KoyebPart: 4 << 20, KoyebBufferTotal: 64 << 20,
	})
	eq(t, c.SSH, SSH{
		Providers: []string{"boat"},
		HostKeys:  []string{"ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIBN0 gateway"},
		SSHDPort:  2222, SSHDFallbackPort: 2023, MaxConnectionsPerPlacement: 9,
		MaxKeysPerPlacement: 5, ConnectionLease: 2 * time.Minute,
		PrepareTimeout: 45 * time.Second, InstallTimeout: 90 * time.Second,
	})
	eq(t, c.Secrets, Secrets{
		Providers: []string{"e2b"},
		EgressIPs: []netip.Addr{
			netip.MustParseAddr("203.0.113.7"), netip.MustParseAddr("2001:db8::7"),
		},
		CACertFile: "/etc/bugnet/egress-ca.pem", CAVersion: 2,
		BoxPublicKeys: []BoxKey{
			{ID: "k2", Key: make([]byte, 32)}, {ID: "k1", Key: []byte(strings.Repeat("k", 32))},
		},
		LinkTTL: 15 * time.Minute, PutGrantTTL: 5 * time.Minute, MaxPerPlacement: 7,
		LinksPerHour: 11, PutsPerHour: 12, ApplyWait: 20 * time.Second,
	})
	eq(t, c.Fetch, Fetch{MaxBytes: 3 << 30, EgressAddr: "[::1]:9444"})
	eq(t, c.Hooks, Hooks{Providers: []string{"e2b"}})
	eq(t, c.LocalSSHDPath, "/opt/sshd")
	eq(t, c.Boxd.GateRoute, "default")
	eq(t, c.Boat.SSHPath, "native")
}

func TestDataPlaneRelativePathsResolve(t *testing.T) {
	path := writeFile(t,
		"[secrets]\nca_cert_file = \"keys/ca.pem\"\n[local]\nsshd_path = \"bin/sshd\"")
	t.Chdir(t.TempDir())
	c, err := LoadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for got, rel := range map[string]string{
		c.Secrets.CACertFile: "keys/ca.pem", c.LocalSSHDPath: "bin/sshd",
	} {
		abs, err := filepath.Abs(rel)
		if err != nil {
			t.Fatal(err)
		}
		eq(t, got, abs)
	}
}

func TestDataPlaneValidation(t *testing.T) {
	key := base64.StdEncoding.EncodeToString(make([]byte, 32))
	cases := []struct{ name, toml, want string }{
		{"valkey socket not absolute", "[valkey]\naddr = \"unix://run/valkey.sock\"",
			"valkey.addr must be host:port, or unix:// and an absolute socket path"},
		{"valkey socket missing", "[valkey]\naddr = \"unix://\"", "valkey.addr must be"},
		{"valkey address without a port", "[valkey]\naddr = \"127.0.0.1\"",
			"valkey.addr must be host:port"},
		{"valkey name without a port", "[valkey]\naddr = \"localhost\"",
			"valkey.addr must be host:port"},
		{"no dataplane workers", "[worker]\ndataplane_workers = 0",
			"worker.dataplane_workers must be a positive whole number"},
		{"preview domain with a scheme", "[names]\npreview_domain = \"https://p.example.com\"",
			"names.preview_domain must be a lowercase host name"},
		{"transfer host in capitals", "[names]\ntransfer_host = \"Transfer.example.com\"",
			"names.transfer_host must be a lowercase host name"},
		{"secrets host with a port", "[names]\nsecrets_host = \"s.example.com:443\"",
			"names.secrets_host"},
		{"ssh host with a trailing dot", "[names]\nssh_host = \"ssh.example.com.\"",
			"names.ssh_host"},
		{"egress host as an address", "[names]\negress_host = \"203.0.113.7\"",
			"names.egress_host"},
		{"ssh port zero", "[names]\nssh_port = 0", "names.ssh_port must be a port from 1 to"},
		{"ssh port too high", "[names]\nssh_port = 65536", "names.ssh_port"},
		{"edge bytes zero", "[edge_budget]\nbytes_per_10_minutes = \"0\"",
			"edge_budget.bytes_per_10_minutes must be 1 byte to 1 TiB"},
		{"edge pace in GB", "[edge_budget]\nbytes_per_second = \"5GB\"",
			"edge_budget.bytes_per_second"},
		{"edge rate zero", "[edge_budget]\nrequests_per_second = 0",
			"edge_budget.requests_per_second must be a positive whole number"},
		{"edge burst negative", "[edge_budget]\nrequests_burst = -1",
			"edge_budget.requests_burst"},
		{"queued requests zero", "[edge_budget]\nqueued_requests = 0",
			"edge_budget.queued_requests"},
		{"websockets zero", "[edge_budget]\nwebsockets = 0", "edge_budget.websockets"},
		{"egress bytes zero", "[edge_budget]\negress_bytes_per_10_minutes = \"0MiB\"",
			"edge_budget.egress_bytes_per_10_minutes"},
		{"egress pace zero", "[edge_budget]\negress_bytes_per_second = \"0\"",
			"edge_budget.egress_bytes_per_second"},
		{"egress in flight zero", "[edge_budget]\negress_requests_in_flight = 0",
			"edge_budget.egress_requests_in_flight"},
		{"unknown preview provider", "[previews]\nproviders = [\"nimbus\"]",
			"previews.providers entry 1 is not one of"},
		{"preview provider twice", "[previews]\nproviders = [\"e2b\", \"e2b\"]",
			`previews.providers lists "e2b" more than once`},
		{"empty ssh provider", "[ssh]\nproviders = [\"\"]", "ssh.providers has an empty entry"},
		{"local with secrets", "[secrets]\nproviders = [\"local\"]",
			"secrets.providers may not list local"},
		{"unknown hook provider", "[hooks]\nproviders = [\"e2b\", \"x\"]",
			"hooks.providers entry 2 is not one of"},
		{"gate port zero", "[previews]\ngate_port = 0", "previews.gate_port must be a port"},
		{"no previews", "[previews]\nmax_per_placement = 0", "previews.max_per_placement"},
		{"url ttl in words", "[transfers]\nurl_ttl = \"ten minutes\"",
			"transfers.url_ttl must be a positive Go duration"},
		{"negative duration", "[transfers]\nmax_duration = \"-1m\"", "transfers.max_duration"},
		{"zero idle timeout", "[transfers]\nidle_timeout = \"0s\"", "transfers.idle_timeout"},
		{"transfer bytes zero", "[transfers]\nmax_bytes = \"0\"", "transfers.max_bytes"},
		{"no claims", "[transfers]\nmax_claims = 0", "transfers.max_claims"},
		{"no active transfers", "[transfers]\nmax_active_per_placement = 0",
			"transfers.max_active_per_placement"},
		{"no transfer tokens", "[transfers]\nmax_tokens_per_placement = 0",
			"transfers.max_tokens_per_placement"},
		{"koyeb part above the total", "[transfers]\nkoyeb_part = \"8MiB\"\n" +
			"koyeb_buffer_total = \"4MiB\"",
			"transfers.koyeb_part must not exceed transfers.koyeb_buffer_total"},
		{"host key without a key", "[ssh]\nhost_keys = [\"ssh-ed25519\"]",
			"ssh.host_keys entry 1 must be a public key line"},
		{"host key not base64", "[ssh]\nhost_keys = [\"ssh-ed25519 !!!\"]",
			"ssh.host_keys entry 1"},
		{"private host key", "[ssh]\nhost_keys = [\"-----BEGIN OPENSSH PRIVATE KEY-----\"]",
			"ssh.host_keys entry 1"},
		{"host key with a second line",
			"[ssh]\nhost_keys = [\"ssh-ed25519 AAAA\\nssh-ed25519 AAAA\"]",
			"ssh.host_keys entry 1 must be a public key line"},
		{"host key with a carriage return",
			"[ssh]\nhost_keys = [\"ssh-ed25519 AAAA\\rssh-ed25519 AAAA\"]",
			"ssh.host_keys entry 1 must be a public key line"},
		{"sshd ports equal", "[ssh]\nsshd_port = 2022", "ssh.sshd_port and " +
			"ssh.sshd_fallback_port must differ"},
		{"sshd port too high", "[ssh]\nsshd_fallback_port = 70000", "ssh.sshd_fallback_port"},
		{"no ssh connections", "[ssh]\nmax_connections_per_placement = 0",
			"ssh.max_connections_per_placement"},
		{"no ssh keys", "[ssh]\nmax_keys_per_placement = 0", "ssh.max_keys_per_placement"},
		{"lease in words", "[ssh]\nconnection_lease = \"long\"", "ssh.connection_lease"},
		{"prepare timeout zero", "[ssh]\nprepare_timeout = \"0s\"", "ssh.prepare_timeout"},
		{"install timeout in words", "[ssh]\ninstall_timeout = \"x\"", "ssh.install_timeout"},
		{"egress ip as a name", "[secrets]\negress_ips = [\"egress.example.com\"]",
			"secrets.egress_ips entry 1 must be an IP address"},
		{"egress ip with a zone", "[secrets]\negress_ips = [\"fe80::1%eth0\"]",
			"secrets.egress_ips entry 1"},
		{"egress ip twice", "[secrets]\negress_ips = [\"203.0.113.7\", \"203.0.113.7\"]",
			"secrets.egress_ips lists 203.0.113.7 more than once"},
		{"ca version zero", "[secrets]\nca_version = 0", "secrets.ca_version"},
		{"box key short", "[secrets]\nbox_public_keys = [{ id = \"k1\", key = \"AAAA\" }]",
			"secrets.box_public_keys entry 1 key must be base64 of 32 bytes"},
		{"box key id empty", "[secrets]\nbox_public_keys = [{ id = \"\", key = \"" + key +
			"\" }]", "secrets.box_public_keys entry 1 id must be 1 to 32 lowercase"},
		{"box key id twice", "[secrets]\nbox_public_keys = [{ id = \"k1\", key = \"" + key +
			"\" }, { id = \"k1\", key = \"" + key + "\" }]",
			`secrets.box_public_keys lists "k1" more than once`},
		{"box key unknown field", "[secrets]\nbox_public_keys = [{ id = \"k1\", pub = \"x\" }]",
			"secrets.box_public_keys.pub is not a setting"},
		{"link ttl zero", "[secrets]\nlink_ttl = \"0s\"", "secrets.link_ttl"},
		{"put grant ttl in words", "[secrets]\nput_grant_ttl = \"soon\"", "secrets.put_grant_ttl"},
		{"apply wait negative", "[secrets]\napply_wait = \"-1s\"", "secrets.apply_wait"},
		{"no secrets per placement", "[secrets]\nmax_per_placement = 0",
			"secrets.max_per_placement"},
		{"no links", "[secrets]\nlinks_per_hour = 0", "secrets.links_per_hour"},
		{"no puts", "[secrets]\nputs_per_hour = 0", "secrets.puts_per_hour"},
		{"custom hosts as a string", "[secrets]\ncustom_hosts = \"yes\"",
			"secrets.custom_hosts must be true or false"},
		{"fetch bytes zero", "[fetch]\nmax_bytes = \"0\"", "fetch.max_bytes"},
		{"fetch address without a port", "[fetch]\negress_addr = \"127.0.0.1\"",
			"fetch.egress_addr must be host:port"},
		{"fetch port zero", "[fetch]\negress_addr = \"127.0.0.1:0\"", "fetch.egress_addr"},
		{"fetch address without a host", "[fetch]\negress_addr = \":9443\"",
			"fetch.egress_addr must be host:port"},
		{"gate route unknown", "[boxd]\ngate_route = \"tunnel\"",
			`boxd.gate_route must be "subdomain" or "default"`},
		{"ssh path unknown", "[boat]\nssh_path = \"direct\"",
			`boat.ssh_path must be "gate" or "native"`},
		{"hooks with another key", "[hooks]\nsecret = \"x\"", "hooks.secret is not a setting"},
	}
	for _, c := range cases {
		msg := problem(t, c.toml)
		if !strings.Contains(msg, c.want) {
			t.Errorf("%s: %q, want an error with %q", c.name, msg, c.want)
		}
		for _, v := range []string{
			"nimbus", "tunnel", "direct", "!!!", "AAAA", "BEGIN", "127.0.0.1", "localhost",
			"9443",
		} {
			if strings.Contains(msg, v) {
				t.Errorf("%s: error quotes %q: %s", c.name, v, msg)
			}
		}
	}
}

func TestShortCallerTokenRefused(t *testing.T) {
	path := writeFile(t, serving(t))
	short := strings.Repeat("s", 31)
	for _, name := range callerTokens {
		_, err := Load(path, env(map[string]string{name: short}))
		want := name + " must be empty or at least 32 characters"
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s of 31 characters gave %v, want %q", name, err, want)
			continue
		}
		if strings.Contains(err.Error(), short) {
			t.Errorf("%s: error echoes its value: %v", name, err)
		}
	}
	tokens := map[string]string{}
	for i, name := range callerTokens {
		tokens[name] = strings.Repeat(string(rune('a'+i)), 32)
	}
	c, err := Load(path, env(tokens))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, []string{c.EdgeToken, c.SecretsToken, c.SSHToken, c.EgressToken, c.EgressFetchToken},
		[]string{
			tokens["BUGNET_EDGE_TOKEN"], tokens["BUGNET_SECRETS_TOKEN"], tokens["BUGNET_SSH_TOKEN"],
			tokens["BUGNET_EGRESS_TOKEN"], tokens["BUGNET_EGRESS_FETCH_TOKEN"],
		})
	none, err := Load(path, env(nil))
	if err != nil {
		t.Fatal(err)
	}
	eq(t, none.EdgeToken+none.SecretsToken+none.SSHToken+none.EgressToken+
		none.EgressFetchToken, "")
}

func TestCallerTokensMustDiffer(t *testing.T) {
	path := writeFile(t, serving(t))
	_, err := Load(path, env(map[string]string{
		"BUGNET_EDGE_TOKEN": secretToken, "BUGNET_SSH_TOKEN": strings.Repeat("z", 32),
	}))
	want := "BUGNET_EDGE_TOKEN must differ from BUGNET_INTERNAL_TOKEN"
	if err == nil || !strings.Contains(err.Error(), want) || strings.Contains(err.Error(),
		secretToken) {
		t.Fatalf("an edge token equal to the internal token gave %v, want %q", err, want)
	}
	same := strings.Repeat("q", 40)
	_, err = Load(path, env(map[string]string{
		"BUGNET_SECRETS_TOKEN": same, "BUGNET_EGRESS_FETCH_TOKEN": same,
	}))
	want = "BUGNET_EGRESS_FETCH_TOKEN must differ from BUGNET_SECRETS_TOKEN"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("two equal caller tokens gave %v, want %q", err, want)
	}
}

func TestCallerTokensInTheFileAreSecrets(t *testing.T) {
	for _, c := range []struct{ toml, key, env string }{
		{"edge_token = \"" + secretToken + "\"", "edge_token", "BUGNET_EDGE_TOKEN"},
		{"[ssh]\ntoken = \"" + secretToken + "\"", "ssh.token", "BUGNET_SSH_TOKEN"},
		{"[secrets]\ntoken = \"" + secretToken + "\"", "secrets.token", "BUGNET_SECRETS_TOKEN"},
		{"egress_fetch_token = \"" + secretToken + "\"", "egress_fetch_token",
			"BUGNET_EGRESS_FETCH_TOKEN"},
		{"[hooks]\nhook_path_secret = \"" + secretToken + "\"", "hooks.hook_path_secret",
			"BUGNET_HOOK_PATH_SECRET"},
		{"[e2b]\nwebhook_secret = \"" + secretToken + "\"", "e2b.webhook_secret",
			"E2B_WEBHOOK_SECRET"},
	} {
		msg := problem(t, c.toml)
		want := c.key + " is a secret, set " + c.env + " in the environment instead"
		if !strings.Contains(msg, want) || strings.Contains(msg, secretToken) {
			t.Errorf("%s in the file gave %q, want %q", c.key, msg, want)
		}
	}
}
