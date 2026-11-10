package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// maxImageManifest bounds the manifest a row keeps
const maxImageManifest = 2 << 10

// ReadyImage is what MarkPlacementReady records about the sandbox's base image
type ReadyImage struct {
	Version string
	// Manifest is the canonical JSON of os and tools, at most 2 KiB
	Manifest []byte
}

// ImageUseRow is the live placements of one provider on one image version
type ImageUseRow struct {
	Provider string
	// Version is empty for sandboxes on the provider's own image
	Version string
	// Live counts placements ready or terminating
	Live int
	// LastReadyAt is the last time a placement on this version became ready
	LastReadyAt time.Time
}

// imageArgs are a ReadyImage's two columns, null for none
func imageArgs(img *ReadyImage) (version any, manifest any, err error) {
	if img == nil || img.Version == "" {
		return nil, nil, nil
	}
	if len(img.Manifest) > maxImageManifest {
		return nil, nil, fmt.Errorf("the image manifest is over %d bytes", maxImageManifest)
	}
	if len(img.Manifest) == 0 {
		return img.Version, nil, nil
	}
	if !json.Valid(img.Manifest) {
		return nil, nil, fmt.Errorf("the image manifest is not JSON")
	}
	return img.Version, string(img.Manifest), nil
}

// ImageUse counts live placements by provider and image version, for ListImageUse
//
// A provider name limits the answer to that provider, empty for every provider. last_ready_at is
// the newest ready_at over every placement of the pair, live or not.
func (s *Store) ImageUse(ctx context.Context, provider string) ([]ImageUseRow, error) {
	rows, err := s.q.Query(ctx, `
		select provider, coalesce(image_version, ''),
			count(*) filter (where state in ('ready', 'terminating'))::int,
			coalesce(max(ready_at), 'epoch'::timestamptz)
		from placements
		where provider is not null and ($1 = '' or provider = $1)
		group by provider, coalesce(image_version, '')
		having count(*) filter (where state in ('ready', 'terminating')) > 0
			or max(ready_at) is not null
		order by provider, coalesce(image_version, '')`, provider)
	if err != nil {
		return nil, fmt.Errorf("image use: %w", err)
	}
	defer rows.Close()
	var out []ImageUseRow
	for rows.Next() {
		var r ImageUseRow
		if err := rows.Scan(&r.Provider, &r.Version, &r.Live, &r.LastReadyAt); err != nil {
			return nil, fmt.Errorf("image use: %w", err)
		}
		r.LastReadyAt = utc(r.LastReadyAt)
		out = append(out, r)
	}
	return out, rows.Err()
}

// ImageSampleCounts are the image trouble the start samples since a time show, by provider
type ImageSampleCounts struct {
	// Mismatches counts attempts that failed as image_mismatch
	Mismatches map[string]int
	// BadReseeds counts sandboxes whose setup could not reseed with the ioctl
	BadReseeds map[string]int
}

// ImageSamples counts the image_mismatch attempts and the refused reseeds since a time
func (s *Store) ImageSamples(ctx context.Context, since time.Time) (ImageSampleCounts, error) {
	out := ImageSampleCounts{Mismatches: map[string]int{}, BadReseeds: map[string]int{}}
	rows, err := s.q.Query(ctx, `
		select provider, count(*) filter (where error_code = 'image_mismatch')::int,
			count(*) filter (where reseed in ('write', 'failed'))::int
		from start_samples where started_at >= $1
		group by provider`, utc(since))
	if err != nil {
		return out, fmt.Errorf("image samples: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		var mismatches, bad int
		if err := rows.Scan(&name, &mismatches, &bad); err != nil {
			return out, fmt.Errorf("image samples: %w", err)
		}
		if mismatches > 0 {
			out.Mismatches[name] = mismatches
		}
		if bad > 0 {
			out.BadReseeds[name] = bad
		}
	}
	return out, rows.Err()
}
