package encoding

import "github.com/xichen2020/eventdb/x/io"

const (
	trueByte = 1
)

// BoolDecoder decodes bool values.
type BoolDecoder interface {
	// Decode decodes bools from reader.
	Decode(reader io.Reader) (ForwardBoolIterator, error)
}

// BoolDec is a bool decoder.
type BoolDec struct {
	t byte
}

// NewBoolDecoder creates a new bool decoder.
func NewBoolDecoder() *BoolDec {
	return &BoolDec{
		t: trueByte,
	}
}

// Decode encoded bool data in a streaming fashion.
func (dec *BoolDec) Decode(reader io.Reader) (ForwardBoolIterator, error) {
	return runLengthDecodeBool(reader, dec.unmarshalFn), nil
}

func (dec *BoolDec) unmarshalFn(reader io.Reader) (bool, error) {
	var value bool
	b, err := reader.ReadByte()
	if err != nil {
		return value, err
	}
	if b == dec.t {
		value = true
	}
	return value, nil
}
