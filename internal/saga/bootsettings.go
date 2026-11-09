package saga

import (
	"bytes"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"log/slog"
	"os"

	"github.com/bugnet-sh/bugnet-router/internal/config"
)

// LoadBootSettings reads the CA and the mining pool list once, so no sandbox lacks a CA
func LoadBootSettings(log *slog.Logger, cfg config.Config) (BootSettings, error) {
	if len(cfg.Secrets.Providers) > 0 && cfg.Secrets.CACertFile == "" {
		return BootSettings{}, errors.New("secrets.providers lists providers while " +
			"secrets.ca_cert_file is empty, so their sandboxes could not trust the egress proxy, " +
			"set secrets.ca_cert_file or empty secrets.providers")
	}
	var ca []byte
	if cfg.Secrets.CACertFile != "" {
		var err error
		if ca, err = readCA(cfg.Secrets.CACertFile); err != nil {
			return BootSettings{}, err
		}
	}
	pools, err := config.LoadPoolList(cfg.Hardening.PoolList)
	if err != nil {
		return BootSettings{}, err
	}
	if len(pools) == 0 {
		log.Warn("the mining pool list is empty or missing, so harden blocks no pools",
			"setting", "hardening.pool_list")
	}
	return BootSettings{
		GatePort: cfg.Previews.GatePort, PreviewDomain: cfg.Names.PreviewDomain,
		CAPEM: ca, CAVersion: cfg.Secrets.CAVersion, MailPorts: cfg.Hardening.MailPorts,
		DenyRanges: cfg.Hardening.DenyRanges, Pools: pools, Harden: cfg.Hardening.Rules,
		EgressHost: cfg.Names.EgressHost, EgressIPs: cfg.Secrets.EgressIPs,
	}, nil
}

// readCA reads the Bugnet CA, which must be certificates alone, as setup accepts them
func readCA(path string) ([]byte, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("secrets.ca_cert_file: %w", err)
	}
	rest, n := raw, 0
	for {
		block, next := pem.Decode(rest)
		if block == nil {
			break
		}
		if block.Type != "CERTIFICATE" {
			return nil, errors.New("secrets.ca_cert_file holds a block that is not a certificate")
		}
		if _, err := x509.ParseCertificate(block.Bytes); err != nil {
			return nil, errors.New("secrets.ca_cert_file holds a certificate that does not parse")
		}
		rest, n = next, n+1
	}
	if n == 0 || len(bytes.TrimSpace(rest)) > 0 {
		return nil, errors.New("secrets.ca_cert_file is not a PEM certificate")
	}
	return raw, nil
}
