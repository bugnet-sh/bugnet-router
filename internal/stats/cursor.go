package stats

import (
	"encoding/base64"
	"encoding/binary"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
)

// cursorPrefix names the cursor's form, so a later form can be told apart
const cursorPrefix = "s1."

// ErrBadCursor means a before value is not a cursor this router wrote
var ErrBadCursor = errors.New("stats: bad cursor")

var cursorEncoding = base64.RawURLEncoding.Strict()

// EncodeCursor writes the row's created_at in Unix microseconds and its id, both big-endian
func EncodeCursor(createdAt time.Time, id uuid.UUID) string {
	var b [24]byte
	binary.BigEndian.PutUint64(b[:8], uint64(createdAt.UnixMicro()))
	copy(b[8:], id[:])
	return cursorPrefix + cursorEncoding.EncodeToString(b[:])
}

// DecodeCursor reads a cursor EncodeCursor wrote, ErrBadCursor for anything else
func DecodeCursor(s string) (time.Time, uuid.UUID, error) {
	rest, ok := strings.CutPrefix(s, cursorPrefix)
	if !ok || cursorEncoding.DecodedLen(len(rest)) != 24 {
		return time.Time{}, uuid.Nil, ErrBadCursor
	}
	b, err := cursorEncoding.DecodeString(rest)
	if err != nil || len(b) != 24 {
		return time.Time{}, uuid.Nil, ErrBadCursor
	}
	at := time.UnixMicro(int64(binary.BigEndian.Uint64(b[:8]))).UTC()
	return at, uuid.UUID(b[8:]), nil
}
