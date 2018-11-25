package hash

import (
	"github.com/xichen2020/eventdb/x/unsafe"

	"github.com/m3db/stackmurmur3"
)

// Hash is the hash type.
type Hash uint64

// BytesHash returns the hash of a byte slice.
func BytesHash(d []byte) Hash {
	return Hash(murmur3.Sum64(d))
}

// StringHash returns the hash of a string.
func StringHash(s string) Hash {
	return BytesHash(unsafe.ToBytes(s))
}

// StringArrayHash returns the hash of a string array
// with a separator.
func StringArrayHash(d []string, sep byte) Hash {
	// NB: This should allocate on the stack.
	var (
		b = []byte{sep}
		h = murmur3.New64()
	)
	// NB: If needed, can fork murmur3 to do this more properly
	for i := 0; i < len(d); i++ {
		h = h.Write(unsafe.ToBytes(d[i]))
		if i < len(d)-1 {
			h = h.Write(b)
		}
	}
	return Hash(h.Sum64())
}
