package encoding

import (
	"io"
	"math"

	xio "github.com/xichen2020/eventdb/x/io"
)

// DoubleEncoder encodes double values.
type DoubleEncoder interface {
	// Encode encodes a collection of doubles and writes the encoded bytes to the writer.
	// Callers should explicitly call `Reset` before subsequent call to `Encode`.
	Encode(writer io.Writer, values ForwardDoubleIterator) error
}

// DoubleEnc is a double encoder.
type DoubleEnc struct {
	buf []byte
}

// NewDoubleEncoder creates a new double encoder.
func NewDoubleEncoder() *DoubleEnc {
	return &DoubleEnc{
		// Ensure that there is enough buffer size to hold 8 bytes.
		buf: make([]byte, uint64SizeBytes),
	}
}

// Encode encodes a collection of doubles and writes the encoded bytes to the writer.
func (enc *DoubleEnc) Encode(writer io.Writer, valuesIt ForwardDoubleIterator) error {
	// Encode doubles as 8 bytes on disk.
	for valuesIt.Next() {
		curr := valuesIt.Current()
		xio.WriteInt(math.Float64bits(curr), uint64SizeBytes, enc.buf)
		if _, err := writer.Write(enc.buf); err != nil {
			return err
		}
	}
	return valuesIt.Err()
}
