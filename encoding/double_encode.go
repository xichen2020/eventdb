package encoding

import (
	"errors"
	"io"
)

// DoubleEncoder encodes double values.
type DoubleEncoder interface {
	// Encode encodes a collection of doubles and writes the encoded bytes to the writer.
	// Callers should explicitly call `Reset` before subsequent call to `Encode`.
	Encode(writer io.Writer, values ForwardDoubleIterator) error

	// Reset resets the encoder.
	Reset()
}

// DoubleEnc is a double encoder.
type DoubleEnc struct{}

// NewDoubleEncoder creates a new double encoder.
func NewDoubleEncoder(writer io.Writer) *DoubleEnc { return &DoubleEnc{} }

// Encode encodes a collection of doubles and writes the encoded bytes to the writer.
func (enc *DoubleEnc) Encode(writer io.Writer, values ForwardDoubleIterator) error {
	return errors.New("not implemented")
}

// Reset resets the encoder.
func (enc *DoubleEnc) Reset() { panic("not implemented") }
