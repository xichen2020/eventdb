package decoding

import (
	"fmt"
	"math"

	"github.com/xichen2020/eventdb/generated/proto/encodingpb"
	"github.com/xichen2020/eventdb/values"
	"github.com/xichen2020/eventdb/values/iterator"
	xio "github.com/xichen2020/eventdb/x/io"
	xproto "github.com/xichen2020/eventdb/x/proto"
)

// DoubleDecoder decodes double values.
type DoubleDecoder interface {
	// DecodeRaw decodes raw encoded bytes into a closeable double values.
	DecodeRaw(data []byte) (values.CloseableDoubleValues, error)
}

// doubleDecoder is a double Decoder.
type doubleDecoder struct{}

// NewDoubleDecoder creates a new double Decoder.
func NewDoubleDecoder() DoubleDecoder { return &doubleDecoder{} }

// DecodeRaw decodes raw encoded bytes into a closeable double values.
func (dec *doubleDecoder) DecodeRaw(data []byte) (values.CloseableDoubleValues, error) {
	var metaProto encodingpb.DoubleMeta
	bytesRead, err := xproto.DecodeDoubleMetaRaw(data, &metaProto)
	if err != nil {
		return nil, err
	}

	return newFsBasedDoubleValues(metaProto, data[bytesRead:]), nil
}

const (
	uint64SizeBytes = 8
)

func newDoubleIteratorFromMeta(
	_ encodingpb.DoubleMeta,
	encodedBytes []byte,
) (iterator.ForwardDoubleIterator, error) {
	return newBitPatternDoubleIterator(encodedBytes), nil
}

// bitPatternDoubleIterator iterates over a stream of double data encoded using its bit pattern.
type bitPatternDoubleIterator struct {
	encodedBytes []byte

	closed  bool
	currIdx int
	curr    float64
	err     error
}

func newBitPatternDoubleIterator(encodedBytes []byte) *bitPatternDoubleIterator {
	return &bitPatternDoubleIterator{
		encodedBytes: encodedBytes,
	}
}

// Next iteration.
func (it *bitPatternDoubleIterator) Next() bool {
	if it.closed || it.err != nil {
		return false
	}
	if it.currIdx == len(it.encodedBytes) {
		// We have read all bytes.
		return false
	}
	if it.currIdx+uint64SizeBytes > len(it.encodedBytes) {
		it.err = fmt.Errorf("double iterator index %d out of range %d", it.currIdx+uint64SizeBytes, len(it.encodedBytes))
		return false
	}
	v := xio.ReadInt(uint64SizeBytes, it.encodedBytes[it.currIdx:])
	it.curr = math.Float64frombits(v)
	it.currIdx += uint64SizeBytes
	return true
}

// Current returns the current double.
func (it *bitPatternDoubleIterator) Current() float64 { return it.curr }

// Err returns any error recorded while iterating.
func (it *bitPatternDoubleIterator) Err() error { return it.err }

// Close the iterator.
func (it *bitPatternDoubleIterator) Close() error {
	if it.closed {
		return nil
	}
	it.closed = true
	it.encodedBytes = nil
	it.err = nil
	return nil
}
