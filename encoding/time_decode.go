package encoding

import "github.com/xichen2020/eventdb/x/io"

// TimeDecoder decodes time values.
type TimeDecoder interface {
	// Decode decodes times from reader.
	Decode(reader io.Reader) (ForwardTimeIterator, error)
}
