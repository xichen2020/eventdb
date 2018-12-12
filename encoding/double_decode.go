package encoding

import (
	"encoding/binary"
	"math"

	"github.com/xichen2020/eventdb/x/io"
)

// DoubleDecoder decodes double values.
type DoubleDecoder interface {
	// Decode decodes doubles from reader.
	Decode(reader io.Reader) (ForwardDoubleIterator, error)
}

// DoubleDec is a double Decoder.
type DoubleDec struct{}

// NewDoubleDecoder creates a new double Decoder.
func NewDoubleDecoder() *DoubleDec { return &DoubleDec{} }

// Decode encoded double data in a streaming fashion.
func (dec *DoubleDec) Decode(reader io.Reader) (ForwardDoubleIterator, error) {
	return newDefaultDoubleIterator(reader), nil
}

// DefaultDoubleIterator iterates over a stream of default encoded double data.
type DefaultDoubleIterator struct {
	reader io.Reader
	curr   float64
	err    error
	closed bool
}

func newDefaultDoubleIterator(reader io.Reader) *DefaultDoubleIterator {
	return &DefaultDoubleIterator{
		reader: reader,
	}
}

// Next iteration.
func (it *DefaultDoubleIterator) Next() bool {
	if it.closed || it.err != nil {
		return false
	}
	var v uint64
	it.err = binary.Read(it.reader, endianness, &v)
	if it.err != nil {
		return false
	}
	it.curr = math.Float64frombits(v)
	return true
}

// Current returns the current double.
func (it *DefaultDoubleIterator) Current() float64 { return it.curr }

// Err returns any error recorded while iterating.
func (it *DefaultDoubleIterator) Err() error { return it.err }

// Close the iterator.
func (it *DefaultDoubleIterator) Close() error {
	it.closed = true
	return nil
}
