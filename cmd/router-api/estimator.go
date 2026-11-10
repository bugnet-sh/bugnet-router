package main

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/valkey-io/valkey-go"

	"github.com/bugnet-sh/bugnet-router/internal/catalog"
	"github.com/bugnet-sh/bugnet-router/internal/config"
	"github.com/bugnet-sh/bugnet-router/internal/estimator"
	"github.com/bugnet-sh/bugnet-router/internal/estimator/fetch"
	"github.com/bugnet-sh/bugnet-router/internal/estimator/rules"
	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

// tokenCheckTimeout bounds the start-up calls that read the GitHub token's limits
const tokenCheckTimeout = 15 * time.Second

// buildEstimator wires the estimator to its fetchers and stores the catalog it quotes from
func buildEstimator(
	ctx context.Context, log *slog.Logger, cfg config.Config, st *store.Store, vk valkey.Client,
	cat *catalog.Catalog, reg *provider.Registry,
) (*estimator.Service, error) {
	cache := fetch.NewCache(vk, cfg.ValkeyPrefix, st, cfg.Estimator, cfg.ReplicaID, log)
	guard := fetch.NewGuard(vk, cfg.ValkeyPrefix, cfg.Estimator.GithubReserve, log)
	gh := fetch.NewGitHub(cfg.Estimator, cfg.GithubToken, cache, guard, log)
	if cfg.GithubToken == "" {
		log.Warn("BUGNET_GITHUB_TOKEN is empty, so GitHub sources are read anonymously, about " +
			"15 new repositories an hour")
	}
	cctx, cancel := context.WithTimeout(ctx, tokenCheckTimeout)
	gh.CheckToken(cctx)
	cancel()
	hf := fetch.NewHuggingFace(cfg.Estimator, cfg.HFToken, cache, guard, log)
	svc, err := estimator.New(estimator.Deps{
		Store: st, Valkey: vk, Prefix: cfg.ValkeyPrefix, Config: cfg.Estimator, Catalog: cat,
		Images:             imageManifests(cfg.Images, cfg.Providers),
		MaxLifetimeMinutes: maxLifetimeMinutes(reg),
		GitHub:             gh, HuggingFace: hf, Log: log,
	})
	if err != nil {
		return nil, fmt.Errorf("estimator: %w", err)
	}
	cache.Admit = svc.AdmitNewSource
	if err := svc.Start(ctx); err != nil {
		return nil, fmt.Errorf("estimator: store the catalog: %w", err)
	}
	return svc, nil
}

// imageManifests turns the [images] tables into the rules' manifests, in provider order
func imageManifests(images config.Images, providers []string) []rules.Image {
	var out []rules.Image
	for _, name := range slices.Sorted(slices.Values(providers)) {
		p, ok := images.Of(name)
		if !ok {
			continue
		}
		t := p.Tools
		img := rules.Image{Provider: name, Version: p.Version, OS: p.OS, Tools: rules.ImageTools{
			Git: t["git"], Tar: t["tar"], Python3: t["python3"], Node: t["node"],
			Docker: t["docker"],
		}}
		for tool, v := range t {
			if !slices.Contains(estimatorTools, tool) {
				if img.MoreTools == nil {
					img.MoreTools = map[string]string{}
				}
				img.MoreTools[tool] = v
			}
		}
		out = append(out, img)
	}
	return out
}

// estimatorTools are the five tools the rules read
var estimatorTools = []string{"git", "tar", "python3", "node", "docker"}

// maxLifetimeMinutes is the longest run any provider allows, 0 when one has no limit
func maxLifetimeMinutes(reg *provider.Registry) int {
	longest := time.Duration(0)
	for _, p := range reg.Ordered() {
		l := reg.Capabilities(p).MaxLifetime
		if l == 0 {
			return 0
		}
		longest = max(longest, l)
	}
	return int(longest / time.Minute)
}
