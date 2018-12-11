package encoding

import (
	"errors"
	"io"
)

// TimeEncoder encodes times values.
type TimeEncoder interface {
	// Encode encodes a collection of time values and writes the encoded bytes to the writer.
	// Callers should explicitly call `Reset` before subsequent call to `Encode`.
	Encode(writer io.Writer, values ForwardTimeIterator) error
}

// TimeEnc is a int encoder.
type TimeEnc struct{}

// NewTimeEncoder creates a new time encoder.
func NewTimeEncoder(writer io.Writer) *TimeEnc { return &TimeEnc{} }

// Encode encodes a collection of time values and writes the encoded bytes to the writer.
func (enc *TimeEnc) Encode(writer io.Writer, values ForwardTimeIterator) error {
	return errors.New("not implemented")
}
