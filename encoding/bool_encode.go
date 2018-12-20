package encoding

import (
	"io"
)

const (
	trueByte byte = 1
)

// BoolEncoder encodes bool values.
type BoolEncoder interface {
	// Encode encodes a collection of bools and writes the encoded bytes to the writer.
	Encode(writer io.Writer, valuesIt ForwardBoolIterator) error
}

// BoolEnc is a bool encoder.
type BoolEnc struct {
	buf []byte
}

// NewBoolEncoder creates a new bool encoder.
func NewBoolEncoder() *BoolEnc {
	return &BoolEnc{}
}

// Encode encodes a collection of bools and writes the encoded bytes to the writer.
// TODO(bodu): implement bit packing and intelligently pick an encoding scheme.
func (enc *BoolEnc) Encode(writer io.Writer, valuesIt ForwardBoolIterator) error {
	return runLengthEncodeBool(writer, &enc.buf, enc.writeBool, valuesIt)
}

func (enc *BoolEnc) writeBool(writer io.Writer, value bool) error {
	// Write a whole byte since we won't win much by writing a single bit since
	// varints are byte packed.
	var b [1]byte
	if value {
		b[0] = trueByte
	}
	_, err := writer.Write(b[:])
	return err
}
