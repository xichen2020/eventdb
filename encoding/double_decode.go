package encoding

import "github.com/xichen2020/eventdb/x/io"

// DoubleDecoder decodes double values.
type DoubleDecoder interface {
	// Decode decodes doubles from reader.
	Decode(reader io.Reader) (ForwardDoubleIterator, error)
}
