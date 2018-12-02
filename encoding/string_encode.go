package encoding

import (
	"errors"
	"io"
)

// StringEncoder encodes string values.
type StringEncoder interface {
	// Encode encodes a collection of strings and writes the encoded bytes to the writer.
	// Callers should explicitly call `Reset` before subsequent call to `Encode`.
	Encode(writer io.Writer, values ForwardStringIterator) error

	// Reset resets the encoder.
	Reset()
}

// StringEnc is a string encoder.
type StringEnc struct{}

// NewStringEncoder creates a new string encoder.
func NewStringEncoder(writer io.Writer) *StringEnc { return &StringEnc{} }

// Encode encodes a collection of strings and writes the encoded bytes to the writer.
func (enc *StringEnc) Encode(writer io.Writer, values ForwardStringIterator) error {
	return errors.New("not implemented")
}

// Reset resets the encoder.
func (enc *StringEnc) Reset() { panic("not implemented") }
