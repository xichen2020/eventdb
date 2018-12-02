package encoding

import (
	"errors"
	"io"
)

// IntEncoder encodes int values.
type IntEncoder interface {
	// Encode encodes a collection of ints and writes the encoded bytes to the writer.
	// Callers should explicitly call `Reset` before subsequent call to `Encode`.
	Encode(writer io.Writer, values ForwardIntIterator) error

	// Reset resets the encoder.
	Reset()
}

// IntEnc is a int encoder.
type IntEnc struct{}

// NewIntEncoder creates a new int encoder.
func NewIntEncoder(writer io.Writer) *IntEnc { return &IntEnc{} }

// Encode encodes a collection of ints and writes the encoded bytes to the writer.
func (enc *IntEnc) Encode(writer io.Writer, values ForwardIntIterator) error {
	return errors.New("not implemented")
}

// Reset resets the encoder.
func (enc *IntEnc) Reset() { panic("not implemented") }
