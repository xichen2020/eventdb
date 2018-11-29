package bool

import (
	"io"

	"github.com/xichen2020/eventdb/encoding/common"
)

// Encoder encodes bool values.
type Encoder interface {
	// Encode a collection of bools.
	// Callers should explicitly call `Reset` before
	// subsequent call to `Encode`.
	Encode(values Iterator) error
	// Bytes returns the encoded bytes of the encoder.
	Bytes() []byte
	// Reset should be called between `Encode` calls.
	Reset()
}

// Decoder decodes bool values.
type Decoder interface {
	// Decode the source bytes.
	// Callers should explicitly call `Reset` before
	// subsequent call to `Decode`.
	Decode(src common.Reader) (Iterator, error)
	// Reset should be called between `Decode` calls.
	Reset()
}

// Iterator lazily produces bool values decoded from a byte stream.
type Iterator interface {
	io.Closer

	// Next returns true if there is another value
	// in the data stream. If it returns false, err should be
	// checked for errors.
	Next() bool
	// Current returns the current value in the iteration.
	Current() bool
	// Err returns any error encountered during iteration.
	Err() error
	// Reset iteration back to the first item in the collection.
	Reset()
}
