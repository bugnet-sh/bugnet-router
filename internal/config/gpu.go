package config

import (
	"cmp"
	"errors"
	"fmt"
	"math/big"
	"path"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/gpu"
)

// gpuProviders are the providers that sell only GPU sandboxes
var gpuProviders = map[string]bool{"lium": true, "hf": true}

// euCountries are the 27 EU member states, the node countries Lium tries first
var euCountries = []string{
	"AT", "BE", "BG", "HR", "CY", "CZ", "DK", "EE", "FI", "FR", "DE", "GR", "HU", "IE", "IT", "LV",
	"LT", "LU", "MT", "NL", "PL", "PT", "RO", "SK", "SI", "ES", "SE",
}

// sanctionedCountries are the countries no Lium node is ever used in
var sanctionedCountries = []string{"BY", "CU", "IR", "KP", "RU", "SY"}

// countryCode is a two-letter code as Lium's location.country_code writes it
var countryCode = regexp.MustCompile(`^[A-Z]{2}$`)

// sha256Hex is a SHA-256 digest in lowercase hex, as sha256sum prints it
var sha256Hex = regexp.MustCompile(`^[0-9a-f]{64}$`)

func defaultLium() liumFile {
	return liumFile{
		// Clones, since go-toml decodes a list into the slice it finds
		Countries: slices.Clone(euCountries), OtherCountries: true,
		BlockedCountries: slices.Clone(sanctionedCountries), MaxLifetime: "2h",
		ReadyTimeout: "6m", StartBilled: "7m", SSHPath: "native", HardenRequired: true,
		HardenNoGateway: true, OpsPerSecond: 5, MaxConcurrent: 10,
		KeyDailyBudgetMicros: 2_300_000_000, CreatesPerSec: 1, TemplateVolume: "/workspace",
		Termination: "max_lifetime",
	}
}

func defaultHF() hfFile {
	return hfFile{
		MaxLifetime: "2h", ExtendRoom: "60m", TimeoutMargin: "10m", ReadyTimeout: "5m",
		StartBilled: "3m", PollEvery: "3s", SBXServer: "fetch", SBXServerVersion: "0.7.0",
		SBXServerSHA256: "eb2b04f79fbe765195300aaf1582be2a2a9ea698ff067d0cab428171d5584932",
		TokenPlan:       "free", OpsPerSecond: 3, MaxConcurrent: 10, CreatesPerSec: 1,
	}
}

// read checks the [lium] keys only Lium has, d holding the defaults
func (f liumFile) read(d liumFile) (Lium, error) {
	l := Lium{
		OtherCountries: f.OtherCountries, KeyDailyBudgetMicros: f.KeyDailyBudgetMicros,
		TemplateVolume: cmp.Or(f.TemplateVolume, d.TemplateVolume),
	}
	if !f.HardenRequired {
		return Lium{}, errors.New("lium.harden_required must stay true, so a pod whose " +
			"harden fails never serves and the order goes to the next provider")
	}
	if !f.HardenNoGateway {
		return Lium{}, errors.New("lium.harden_no_gateway must stay true, a pod's default " +
			"gateway is the Lium host, whose services a renter must not reach")
	}
	for _, list := range []struct {
		key  string
		raw  []string
		into *[]string
	}{
		{"lium.countries", f.Countries, &l.Countries},
		{"lium.blocked_countries", f.BlockedCountries, &l.BlockedCountries},
	} {
		for i, code := range list.raw {
			if !countryCode.MatchString(code) {
				// An entry is never quoted, since a secret could have been pasted there
				return Lium{}, fmt.Errorf("%s entry %d must be a two-letter country code in "+
					"capitals such as DE", list.key, i+1)
			}
		}
		*list.into = listOf(list.raw)
	}
	if len(l.Countries) == 0 {
		return Lium{}, errors.New("lium.countries must name at least one country, the node " +
			"countries tried first")
	}
	for i, code := range l.Countries {
		if slices.Contains(l.BlockedCountries, code) {
			return Lium{}, fmt.Errorf("lium.countries entry %d is also in "+
				"lium.blocked_countries", i+1)
		}
	}
	err := readDurations("lium", []durationKey{
		{"max_lifetime", f.MaxLifetime, d.MaxLifetime, time.Second, &l.MaxLifetime},
		{"start_billed", f.StartBilled, d.StartBilled, 0, &l.StartBilled},
	})
	if err != nil {
		return Lium{}, err
	}
	if l.SSHPath, err = oneOf("lium.ssh_path", f.SSHPath, d.SSHPath, "gate", "native"); err != nil {
		return Lium{}, err
	}
	l.Termination, err = oneOf("lium.termination", f.Termination, d.Termination,
		"max_lifetime", "lifetime")
	if err != nil {
		return Lium{}, err
	}
	if f.MinDownloadMbps < 0 || f.MinDownloadMbps > maxProviderCount {
		return Lium{}, fmt.Errorf("lium.min_download_mbps must be 0 to %d", maxProviderCount)
	}
	l.MinDownloadMbps = int(f.MinDownloadMbps)
	if l.KeyDailyBudgetMicros < 0 {
		return Lium{}, fmt.Errorf("lium.key_daily_budget_micros must be 0 or more")
	}
	if !path.IsAbs(l.TemplateVolume) {
		return Lium{}, fmt.Errorf("lium.template_volume must be an absolute path such as %s",
			d.TemplateVolume)
	}
	for i, s := range f.Sizes {
		if err := checkSize(i, s, l.Sizes); err != nil {
			return Lium{}, err
		}
		s.Match = slices.Clone(s.Match)
		l.Sizes = append(l.Sizes, s)
	}
	return l, nil
}

// checkSize checks lium.sizes entry i against the entries before it
func checkSize(i int, s LiumSize, before []LiumSize) error {
	entry := fmt.Sprintf("lium.sizes entry %d", i+1)
	switch {
	case !sizeName.MatchString(s.Size):
		return fmt.Errorf("%s size must be 1 to 32 lowercase letters, digits or hyphens", entry)
	case !gpu.Known(s.Model):
		return fmt.Errorf("%s model must be a canonical GPU model, one of %s", entry,
			strings.Join(gpu.Models(), ", "))
	case s.MaxHourlyMicros <= 0:
		return fmt.Errorf("%s max_hourly_micros must be above 0, the cap per node and hour", entry)
	case s.GPUs < 1:
		return fmt.Errorf("%s gpus must be 1 or more", entry)
	case len(s.Match) == 0:
		return fmt.Errorf("%s match must name at least one gpu_class", entry)
	}
	if j := slices.IndexFunc(before, func(b LiumSize) bool { return b.Size == s.Size }); j >= 0 {
		return fmt.Errorf("%s repeats the size of entry %d", entry, j+1)
	}
	return nil
}

// read checks the [hf] keys only HF has, d holding the defaults
func (f hfFile) read(d hfFile) (HF, error) {
	h := HF{
		Namespace: f.Namespace, Flavors: listOf(f.Flavors),
		SBXServerVersion: cmp.Or(f.SBXServerVersion, d.SBXServerVersion),
		SBXServerSHA256:  cmp.Or(f.SBXServerSHA256, d.SBXServerSHA256),
	}
	for i, flavor := range h.Flavors {
		if !gpu.HFFlavor(flavor) {
			// An entry is never quoted, since a secret could have been pasted there
			return HF{}, fmt.Errorf("hf.flavors entry %d is not an HF GPU flavor, use one of %s",
				i+1, strings.Join(gpu.HFFlavors(), ", "))
		}
	}
	err := readDurations("hf", []durationKey{
		{"max_lifetime", f.MaxLifetime, d.MaxLifetime, time.Second, &h.MaxLifetime},
		{"extend_room", f.ExtendRoom, d.ExtendRoom, 0, &h.ExtendRoom},
		{"timeout_margin", f.TimeoutMargin, d.TimeoutMargin, 0, &h.TimeoutMargin},
		{"start_billed", f.StartBilled, d.StartBilled, 0, &h.StartBilled},
		{"poll_every", f.PollEvery, d.PollEvery, time.Millisecond, &h.PollEvery},
	})
	if err != nil {
		return HF{}, err
	}
	if h.SBXServer, err = oneOf("hf.sbx_server", f.SBXServer, d.SBXServer,
		"fetch", "baked"); err != nil {
		return HF{}, err
	}
	if h.TokenPlan, err = oneOf("hf.token_plan", f.TokenPlan, d.TokenPlan,
		"free", "pro"); err != nil {
		return HF{}, err
	}
	if !sha256Hex.MatchString(h.SBXServerSHA256) {
		return HF{}, fmt.Errorf("hf.sbx_server_sha256 must be 64 lowercase hex digits, as " +
			"sha256sum prints them")
	}
	return h, nil
}

// checkGPU refuses the settings under which a GPU provider would serve unsafely or never serve
func (c *Config) checkGPU() error {
	if slices.Contains(c.Providers, "hf") && c.HF.Namespace == "" {
		return errors.New("hf.namespace is required while providers lists hf, the " +
			"organization the Jobs run and bill under")
	}
	if slices.Contains(c.Providers, "lium") && len(c.Lium.Sizes) == 0 {
		return errors.New("lium.sizes must name at least one size while providers lists lium")
	}
	if err := c.checkLiumKeyBudget(); err != nil {
		return err
	}
	if slices.Contains(c.Previews.Providers, "lium") {
		return errors.New("previews.providers may not list lium, whose pods run no gate")
	}
	if slices.Contains(c.SSH.Providers, "lium") && c.Lium.SSHPath != "native" {
		return errors.New(`ssh.providers may list lium only while lium.ssh_path is "native", ` +
			"since lium pods run no gate to carry SSH")
	}
	return nil
}

// checkLiumKeyBudget refuses a key budget the router could reach, since Lium then deletes its pods
func (c *Config) checkLiumKeyBudget() error {
	budget := c.Lium.KeyDailyBudgetMicros
	if budget == 0 {
		return nil
	}
	var dearest int64
	for _, s := range c.Lium.Sizes {
		dearest = max(dearest, s.MaxHourlyMicros)
	}
	slots := cmp.Or(c.MaxConcurrent["lium"], DefaultMaxConcurrent)
	day := new(big.Int).Mul(big.NewInt(dearest), big.NewInt(int64(slots)*24))
	if big.NewInt(budget).Cmp(day) < 0 {
		return fmt.Errorf("lium.key_daily_budget_micros must be 0 or at least %s, %d pods at "+
			"the dearest lium.sizes cap for 24 hours, since Lium deletes a key's pods once its "+
			"spend reaches the budget", day, slots)
	}
	return nil
}

// durationKey is one duration setting of a table, refused below min
type durationKey struct {
	key, raw, def string
	min           time.Duration
	into          *time.Duration
}

// readDurations reads each duration of the named table, its default when empty
func readDurations(table string, keys []durationKey) error {
	for _, k := range keys {
		got, err := time.ParseDuration(cmp.Or(k.raw, k.def))
		if err != nil || got < k.min {
			return fmt.Errorf("%s.%s must be a Go duration of %s or more such as %s", table,
				k.key, k.min, k.def)
		}
		*k.into = got
	}
	return nil
}

// oneOf reads a setting that takes one of a few words, its default when empty
func oneOf(key, raw, def string, allowed ...string) (string, error) {
	v := cmp.Or(raw, def)
	if !slices.Contains(allowed, v) {
		return "", fmt.Errorf("%s must be one of %s", key, strings.Join(allowed, ", "))
	}
	return v, nil
}

// listOf copies a list, nil when it is empty
func listOf(s []string) []string {
	if len(s) == 0 {
		return nil
	}
	return slices.Clone(s)
}
