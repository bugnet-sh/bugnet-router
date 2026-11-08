// Package wallet checks Solana payer addresses and decides which payer an order counts under.
//
// ParseSolana accepts only the one canonical base58 text of 32 bytes, through the router's one
// decoder in internal/solana. Decide runs the payer guard of CP 7.4, so the facilitator's
// address, which pays every sponsored transaction's fee, never stands for every user.
package wallet

import (
	"errors"
	"slices"

	"github.com/bugnet-sh/bugnet-router/internal/solana"
)

// ErrBadAddress means a text is not the canonical base58 form of a 32-byte Solana address
var ErrBadAddress = errors.New("wallet: not a canonical Solana address")

// UnguardedKey is the Valkey key, under the prefix, that says a payer counted as absent at step 4
const UnguardedKey = "wallet:unguarded"

// ParseSolana returns s when it is the canonical base58 form of 32 bytes
func ParseSolana(s string) (string, error) {
	if len(s) < 32 || len(s) > 44 {
		return "", ErrBadAddress
	}
	if _, ok := solana.PublicKey(s); !ok {
		return "", ErrBadAddress
	}
	return s, nil
}

// Reason says why a payer claim counts as no payer, Present when it counts
type Reason string

// The steps of CP 7.4
const (
	Present Reason = ""
	// AbsentOff is step 1, wallets.use_payer is false
	AbsentOff Reason = "use_payer_off"
	// AbsentNoClaim means the token carried no payer
	AbsentNoClaim Reason = "no_payer"
	// AbsentIsFeePayer is step 2, the payer equals the token's fee payer
	AbsentIsFeePayer Reason = "payer_is_fee_payer"
	// AbsentFacilitator is step 3, a listed facilitator or an earlier order's fee payer
	AbsentFacilitator Reason = "facilitator_address"
	// AbsentUnguarded is step 4, no fee payer came and no facilitator is listed
	AbsentUnguarded Reason = "unguarded"
)

// Guard holds the settings the payer decision reads
type Guard struct {
	UsePayer     bool
	Facilitators []string
}

// Claim is what an order token or a CheckPayer call says about who paid
type Claim struct {
	Payer    string
	FeePayer string
}

// Decide returns the payer an order counts under, or none and why, seen naming earlier fee payers
func Decide(
	g Guard, c Claim, seen func(addr string) (bool, error),
) (string, Reason, error) {
	switch {
	case !g.UsePayer:
		return "", AbsentOff, nil
	case c.Payer == "":
		return "", AbsentNoClaim, nil
	case c.FeePayer != "" && c.Payer == c.FeePayer:
		return "", AbsentIsFeePayer, nil
	case slices.Contains(g.Facilitators, c.Payer):
		return "", AbsentFacilitator, nil
	}
	before, err := seen(c.Payer)
	if err != nil {
		return "", "", err
	}
	switch {
	case before:
		return "", AbsentFacilitator, nil
	case c.FeePayer == "" && len(g.Facilitators) == 0:
		return "", AbsentUnguarded, nil
	}
	return c.Payer, Present, nil
}
