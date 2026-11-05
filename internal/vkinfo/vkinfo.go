// Package vkinfo reads Valkey's memory use and eviction policy from INFO memory
package vkinfo

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/valkey-io/valkey-go"
)

// NoEviction is the only maxmemory-policy the router runs with, it may lose no stream or lease
const NoEviction = "noeviction"

// ErrIncomplete means INFO memory answered without a field the router reads
var ErrIncomplete = errors.New("valkey info memory is incomplete")

const checkTimeout = 2 * time.Second

// Memory is Valkey's used memory and maxmemory in bytes, -1 when INFO lacks one, and its policy
type Memory struct {
	Used, Max int64
	Policy    string
}

// Reader reads a Valkey's memory, keeping the fields it found beside an error, Read outside tests
type Reader func(context.Context, valkey.Client) (Memory, error)

// Read asks c for INFO memory and parses the reply
func Read(ctx context.Context, c valkey.Client) (Memory, error) {
	info, err := c.Do(ctx, c.B().Info().Section("memory").Build()).ToString()
	if err != nil {
		return Memory{Used: -1, Max: -1}, fmt.Errorf("valkey info memory: %w", err)
	}
	return Parse(info)
}

// Parse reads used_memory, maxmemory and maxmemory_policy from INFO text, naming each one missing
func Parse(info string) (Memory, error) {
	fields := map[string]string{}
	for line := range strings.Lines(info) {
		if key, value, ok := strings.Cut(strings.TrimSpace(line), ":"); ok {
			fields[key] = value
		}
	}
	used, usedErr := number(fields, "used_memory")
	limit, limitErr := number(fields, "maxmemory")
	policy := fields["maxmemory_policy"]
	var policyErr error
	if policy == "" {
		policyErr = fmt.Errorf("%w: no maxmemory_policy", ErrIncomplete)
	}
	return Memory{Used: used, Max: limit, Policy: policy}, errors.Join(usedErr, limitErr, policyErr)
}

func number(fields map[string]string, key string) (int64, error) {
	v, ok := fields[key]
	if !ok {
		return -1, fmt.Errorf("%w: no %s", ErrIncomplete, key)
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return -1, fmt.Errorf("%w: %s is not an integer", ErrIncomplete, key)
	}
	return n, nil
}

// CheckStart refuses a Valkey that may evict keys and warns when its memory goes unwatched
func CheckStart(ctx context.Context, log *slog.Logger, c valkey.Client, read Reader) error {
	ctx, cancel := context.WithTimeout(ctx, checkTimeout)
	defer cancel()
	m, err := read(ctx, c)
	if m.Policy != "" && m.Policy != NoEviction {
		return fmt.Errorf("valkey: maxmemory-policy is %s, the router needs noeviction", m.Policy)
	}
	if err != nil {
		log.Warn("valkey info memory cannot be read in full, starting anyway", "err", err)
	}
	if m.Max == 0 {
		log.Warn("valkey has no maxmemory, so /monitor cannot alert on memory")
	}
	return nil
}
