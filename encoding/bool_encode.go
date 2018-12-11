package encoding

import (
	"io"
)

// BoolEncoder encodes bool values.
type BoolEncoder interface {
	// Encode encodes a collection of bools and writes the encoded bytes to the writer.
	Encode(writer io.Writer, valuesIt ForwardBoolIterator) error
}

// BoolEnc is a bool encoder.
type BoolEnc struct {
	buf []byte
	t   []byte
	f   []byte
}

// NewBoolEncoder creates a new bool encoder.
func NewBoolEncoder() *BoolEnc {
	return &BoolEnc{
		t: []byte{1},
		f: []byte{0},
	}
}

// Encode encodes a collection of bools and writes the encoded bytes to the writer.
func (enc *BoolEnc) Encode(writer io.Writer, valuesIt ForwardBoolIterator) error {
	return runLengthEncodeBool(writer, &enc.buf, enc.marshalFn, valuesIt)
}

func (enc *BoolEnc) marshalFn(writer io.Writer, value bool) error {
	// Write a whole byte since we won't win much by writing a single bit since
	// varints are byte packed.
	writeValue := enc.t
	if !value {
		writeValue = enc.f
	}
	_, err := writer.Write(writeValue)
	return err
}
