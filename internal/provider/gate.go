package provider

import (
	"errors"
	"fmt"
)

// ErrGateUnprepared is wrapped by GateRoute when the ref lacks what PrepareGate stores
var ErrGateUnprepared = errors.New("the gate route is not prepared, run PrepareGate first")

// CheckGatePort refuses a gate port outside 1 to 65535 as invalid
func CheckGatePort(port int) error {
	if port < 1 || port > 65535 {
		return &Error{Code: CodeInvalid, Err: fmt.Errorf("gate port %d is not a TCP port", port)}
	}
	return nil
}

// GateUnprepared is the invalid error GateRoute returns for a ref missing the meta key
func GateUnprepared(provider, id, key string) error {
	return &Error{
		Code: CodeInvalid,
		Err:  fmt.Errorf("sandbox %s/%s has no %s: %w", provider, id, key, ErrGateUnprepared),
	}
}
