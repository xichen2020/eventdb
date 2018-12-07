package encoding

import (
	"fmt"

	"github.com/xichen2020/eventdb/generated/proto/encodingpb"
	"github.com/xichen2020/eventdb/x/io"
	"github.com/xichen2020/eventdb/x/proto"
)

// IntDecoder decodes int values.
type IntDecoder interface {
	// Decode decodes ints from reader.
	Decode(reader io.Reader) (ForwardIntIterator, error)

	// Reset resets the decoder.
	Reset()
}

// IntDec is a int Decoder.
type IntDec struct {
	dictionaryProto encodingpb.IntDictionary
	metaProto       encodingpb.IntMeta
	buf             []byte
}

// NewIntDecoder creates a new int Decoder.
func NewIntDecoder() *IntDec {
	return &IntDec{
		// Make buf at least big enough to hold Uint64 values.
		buf: make([]byte, uint64SizeBytes),
	}
}

// Decode encoded int data in a streaming fashion.
func (dec *IntDec) Decode(reader io.Reader) (ForwardIntIterator, error) {
	// Decode metadata first.
	if err := proto.DecodeIntMeta(&dec.metaProto, &dec.buf, reader); err != nil {
		return nil, err
	}

	var (
		iter ForwardIntIterator
		err  error
	)
	switch dec.metaProto.Encoding {
	case encodingpb.EncodingType_DELTA:
		iter = dec.decodeDelta(reader, dec.metaProto.BitsPerEncodedValue, dec.metaProto.DeltaStart)
	case encodingpb.EncodingType_DICTIONARY:
		iter, err = dec.decodeDictionary(reader, dec.metaProto.BytesPerDictionaryValue, dec.metaProto.BitsPerEncodedValue)
	default:
		return nil, fmt.Errorf("Invalid encoding type: %v", dec.metaProto.Encoding)
	}

	return iter, err
}

// Reset the int decoder.
func (dec *IntDec) Reset() {}

func (dec *IntDec) decodeDelta(
	reader io.Reader,
	bitsPerEncodedValue int64,
	deltaStart int64,
) *DeltaIntIterator {
	return NewDeltaIntIterator(reader, bitsPerEncodedValue, deltaStart)
}

func (dec *IntDec) decodeDictionary(
	reader io.Reader,
	bytesPerDictionaryValue int64,
	bitsPerEncodedValue int64,
) (*DictionaryBasedIntIterator, error) {
	return NewDictionaryBasedIntIterator(reader, &dec.dictionaryProto, &dec.buf, bytesPerDictionaryValue, bitsPerEncodedValue)
}
