package encoding

import (
	"errors"
	"io"
)

// BoolEncoder encodes bool values.
type BoolEncoder interface {
	// Encode encodes a collection of bools and writes the encoded bytes to the writer.
	// Callers should explicitly call `Reset` before subsequent call to `Encode`.
	Encode(writer io.Writer, values ForwardBoolIterator) error
}

// BoolEnc is a bool encoder.
type BoolEnc struct{}

// NewBoolEncoder creates a new bool encoder.
func NewBoolEncoder(writer io.Writer) *BoolEnc { return &BoolEnc{} }

// Encode encodes a collection of bools and writes the encoded bytes to the writer.
func (enc *BoolEnc) Encode(writer io.Writer, values ForwardBoolIterator) error {
	return errors.New("not implemented")
}
