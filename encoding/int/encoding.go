package int

import "io"

// Encoder encodes int values.
type Encoder interface {
	// Encode a collection of ints.
	// Callers should explicitly call `Reset` before
	// subsequent call to `Encode`.
	Encode(Iterator) error
	// Bytes returns the encoded bytes of the encoder.
	// The format of the byte slice is as follows:
	//   - version:                 1 byte
	//   - metadata length bytes:   VInt
	//   - metadata bytes:          Variable bytes
	//   - data bytes:              Variable bytes
	Bytes() []byte
	// Reset should be called between `Encode` calls.
	Reset()
}

// Decoder decodes int values.
type Decoder interface {
	// Decode the source bytes.
	// Callers should explicitly call `Reset` before
	// subsequent call to `Decode`.
	Decode(src io.Reader) (Iterator, error)
	// Reset should be called between `Decode` calls.
	Reset()
}

// Iterator lazily produces int values decoded from a byte stream.
type Iterator interface {
	io.Closer

	// Next returns true if there is another value
	// in the data stream. If it returns false, err should be
	// checked for errors.
	Next() bool
	// Current returns the current value in the iteration.
	Current() int
	// Err returns any error encountered during iteration.
	Err() error
}
