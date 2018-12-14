package encoding

import (
	"fmt"

	"github.com/xichen2020/eventdb/generated/proto/encodingpb"
	"github.com/xichen2020/eventdb/x/io"
	"github.com/xichen2020/eventdb/x/proto"

	bitstream "github.com/dgryski/go-bitstream"
)

// IntDecoder decodes int values.
type IntDecoder interface {
	// Decode decodes ints from reader.
	Decode(reader io.Reader) (ForwardIntIterator, error)
}

// IntDec is a int Decoder.
type IntDec struct {
	bitReader       *bitstream.BitReader
	dictionaryProto encodingpb.IntDictionary
	metaProto       encodingpb.IntMeta
	buf             []byte
}

// NewIntDecoder creates a new int Decoder.
func NewIntDecoder() *IntDec {
	return &IntDec{
		// Make buf at least big enough to hold Uint64 values.
		buf: make([]byte, uint64SizeBytes),
		// Make a bitWriter w/ a nil buffer that will be re-used for every `Decode` call.
		bitReader: bitstream.NewReader(nil),
	}
}

// Decode encoded int data in a streaming fashion.
func (dec *IntDec) Decode(reader io.Reader) (ForwardIntIterator, error) {
	// Reset internal state at the beginning of every `Encode` call.
	dec.reset(reader)

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
		iter = dec.decodeDelta()
	case encodingpb.EncodingType_DICTIONARY:
		iter, err = dec.decodeDictionary(reader)
	default:
		return nil, fmt.Errorf("Invalid encoding type: %v", dec.metaProto.Encoding)
	}

	return iter, err
}

// Reset the int decoder.
func (dec *IntDec) reset(reader io.Reader) {
	// Reset the BitReader at the start of each `Decode` call.
	dec.bitReader.Reset(reader)
	dec.metaProto.Reset()
	dec.dictionaryProto.Reset()
}

func (dec *IntDec) decodeDelta() *DeltaIntIterator {
	return newDeltaIntIterator(
		dec.bitReader,
		dec.metaProto.BitsPerEncodedValue,
		int(dec.metaProto.DeltaStart),
	)
}

func (dec *IntDec) decodeDictionary(reader io.Reader) (*DictionaryBasedIntIterator, error) {
	return newDictionaryBasedIntIterator(
		reader,
		dec.bitReader,
		&dec.dictionaryProto,
		&dec.buf,
		dec.metaProto.MinValue,
		dec.metaProto.BytesPerDictionaryValue,
		dec.metaProto.BitsPerEncodedValue,
	)
}
