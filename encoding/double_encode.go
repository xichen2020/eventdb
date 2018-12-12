package encoding

import (
	"encoding/binary"
	"io"
	"math"
)

// DoubleEncoder encodes double values.
type DoubleEncoder interface {
	// Encode encodes a collection of doubles and writes the encoded bytes to the writer.
	// Callers should explicitly call `Reset` before subsequent call to `Encode`.
	Encode(writer io.Writer, values ForwardDoubleIterator) error
}

// DoubleEnc is a double encoder.
type DoubleEnc struct{}

// NewDoubleEncoder creates a new double encoder.
func NewDoubleEncoder() *DoubleEnc { return &DoubleEnc{} }

// Encode encodes a collection of doubles and writes the encoded bytes to the writer.
func (enc *DoubleEnc) Encode(writer io.Writer, valuesIt ForwardDoubleIterator) error {
	// Encode doubles as 8 bytes on disk.
	for valuesIt.Next() {
		if err := binary.Write(writer, endianness, math.Float64bits(valuesIt.Current())); err != nil {
			return err
		}
	}
	return valuesIt.Err()
}
