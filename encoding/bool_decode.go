package encoding

import "github.com/xichen2020/eventdb/x/io"

// BoolDecoder decodes bool values.
type BoolDecoder interface {
	// Decode decodes bools from reader.
	Decode(reader io.Reader) (ForwardBoolIterator, error)

	// Reset resets the decoder.
	Reset()
}
