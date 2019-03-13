package bytes

import (
	"bytes"
	"encoding/json"
)

// mutability determines whether the bytes are mutable by contract.
type mutability int

// A list of supported mutability modes.
const (
	mutable mutability = iota
	immutable
)

// Bytes contains a byte slice alongside metadata informaion such as
// mutability and ownership.
type Bytes struct {
	// Raw underlying bytes.
	raw []byte

	// Whether the underlying byte slice may be mutated internally or externally
	// by contract or semantics. Users should check this flag when determining
	// whether they need to make a copy of the underlying bytes if they wish to
	// retain the raw bytes for long period of time as they can change underneath them.
	mutability mutability
}

// NewMutableBytes creates a new bytes object with bytes that may be mutated
// underneath at a later point in time.
func NewMutableBytes(raw []byte) Bytes {
	return Bytes{
		raw:        raw,
		mutability: mutable,
	}
}

// NewImmutableBytes creates a new bytes object whose bytes are contractually immutable.
func NewImmutableBytes(raw []byte) Bytes {
	return Bytes{
		raw:        raw,
		mutability: immutable,
	}
}

// IsMutable returns whether the underlying raw bytes are externally mutable by contract.
func (b Bytes) IsMutable() bool { return b.mutability == mutable }

// IsImmutable returns whether the underlying raw bytes are immutable by contract.
func (b Bytes) IsImmutable() bool { return b.mutability == immutable }

// Bytes returns the raw underlying bytes.
func (b Bytes) Bytes() []byte { return b.raw }

// SafeBytes returns the bytes that's safe to retain externally.
func (b Bytes) SafeBytes() []byte {
	if b.IsImmutable() {
		return b.raw
	}
	clone := make([]byte, len(b.raw))
	copy(clone, b.raw)
	return clone
}

// Equal compares two bytes objects and returns true if they are equal.
func (b Bytes) Equal(other Bytes) bool {
	return bytes.Equal(b.raw, other.raw)
}

// Compare compares two bytes objects, and returns
// * -1 if v1 < v2
// * 0 if v1 == v2
// * 1 if v1 > v2
func (b Bytes) Compare(other Bytes) int {
	return bytes.Compare(b.raw, other.raw)
}

// MarshalJSON marshals value as a JSON object.
// NB: This is inefficient but it is also not on the critical path so we don't care.
func (b Bytes) MarshalJSON() ([]byte, error) {
	return json.Marshal(string(b.raw))
}
