package store

import (
	"context"
	"fmt"

	"github.com/google/uuid"
)

// IndexTransfer records which placement a transfer token belongs to, once per token
func (s *Store) IndexTransfer(ctx context.Context, tokenSHA256 []byte, placement uuid.UUID) error {
	_, err := s.q.Exec(ctx, `
		insert into transfer_index (token_sha256, placement_id) values ($1, $2)
		on conflict (token_sha256) do nothing`, tokenSHA256, placement)
	if err != nil {
		return fmt.Errorf("index transfer: %w", err)
	}
	return nil
}

// TransferIndexed reports whether a transfer token was ever indexed
func (s *Store) TransferIndexed(ctx context.Context, tokenSHA256 []byte) (bool, error) {
	var found bool
	err := s.q.QueryRow(ctx, `
		select exists (select 1 from transfer_index where token_sha256 = $1)`, tokenSHA256).
		Scan(&found)
	if err != nil {
		return false, fmt.Errorf("find transfer: %w", err)
	}
	return found, nil
}
