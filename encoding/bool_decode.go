package encoding

import "github.com/xichen2020/eventdb/x/io"

// BoolDecoder decodes bool values.
type BoolDecoder interface {
	// Decode decodes bools from reader.
	Decode(reader io.Reader) (ForwardBoolIterator, error)
}

// BoolDec is a bool decoder.
type BoolDec struct{}

// NewBoolDecoder creates a new bool decoder.
func NewBoolDecoder() *BoolDec { return &BoolDec{} }

// Decode encoded bool data in a streaming fashion.
func (dec *BoolDec) Decode(reader io.Reader) (ForwardBoolIterator, error) {
	return runLengthDecodeBool(reader, dec.readBool), nil
}

func (dec *BoolDec) readBool(reader io.Reader) (bool, error) {
	var value bool
	b, err := reader.ReadByte()
	if err != nil {
		return value, err
	}
	if b == trueByte {
		value = true
	}
	return value, nil
}
