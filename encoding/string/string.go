package string

import "bytes"

type encoder struct {
	buf bytes.Buffer
}

// NewEncoder returns a new encoder instance.
func NewEncoder() Encoder {
	return &encoder{}
}

// Encode a collection of strings.
func (e *encoder) Encode(iter Iterator) error {
	return nil
}

// Bytes returns the encoded bytes.
func (e *encoder) Bytes() []byte {
	return e.buf.Bytes()
}

// Reset the internal byte buffer.
func (e *encoder) Reset() {
	e.buf.Reset()
}
