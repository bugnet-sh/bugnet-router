package main

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"time"

	"github.com/bugnet-sh/bugnet-router/internal/provider"
	"github.com/bugnet-sh/bugnet-router/internal/store"
)

// imageWindow is how far back the image lines read the start samples
const imageWindow = time.Hour

// imageLines reports CP 19.14's lines, a provider images.accept keeps out of placements, any
// image_mismatch and any reseed the kernel refused in the last hour
func imageLines(
	log *slog.Logger, st *store.Store, reg *provider.Registry, accept []string,
) problemSource {
	return func(ctx context.Context) []string {
		var out []string
		if len(accept) > 0 {
			for _, p := range reg.Ordered() {
				if !reg.ImageAccepted(p, accept) {
					out = append(out, fmt.Sprintf("%s is kept out of placements, its image "+
						"version %q is not in images.accept", p.Name(), reg.ImageVersion(p)))
				}
			}
		}
		counts, err := st.ImageSamples(ctx, time.Now().Add(-imageWindow))
		if err != nil {
			log.Warn("monitor cannot read the image samples", "err", err)
			return out
		}
		for _, name := range slices.Sorted(maps.Keys(counts.Mismatches)) {
			out = append(out, fmt.Sprintf("%s had %d image_mismatch in the last hour", name,
				counts.Mismatches[name]))
		}
		for _, name := range slices.Sorted(maps.Keys(counts.BadReseeds)) {
			out = append(out, fmt.Sprintf("%s had %d sandboxes whose seed was not reseeded with "+
				"the ioctl in the last hour", name, counts.BadReseeds[name]))
		}
		return out
	}
}
