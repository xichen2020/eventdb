package encoding

import (
	"fmt"

	"github.com/xichen2020/eventdb/generated/proto/encodingpb"
	"github.com/xichen2020/eventdb/x/io"
	"github.com/xichen2020/eventdb/x/proto"
)

// StringDecoder decodes string values.
type StringDecoder interface {
	// Decode decodes strings from reader.
	Decode(reader io.Reader) (ForwardStringIterator, error)

	// Reset the string decoder between `Decode` calls.
	Reset()
}

// StringDec is a string decoder.
type StringDec struct {
	buf             []byte
	metaProto       encodingpb.StringMeta
	dictionaryProto encodingpb.StringArray
}

// NewStringDecoder creates a new string decoder.
func NewStringDecoder() *StringDec { return &StringDec{} }

// Decode encoded string data in a streaming fashion.
func (dec *StringDec) Decode(reader io.Reader) (ForwardStringIterator, error) {
	// Decode metadata first.
	if err := proto.DecodeStringMeta(&dec.metaProto, &dec.buf, reader); err != nil {
		return nil, err
	}

	switch dec.metaProto.Compression {
	case encodingpb.CompressionType_ZSTD:
		reader = NewCompressReader(reader)
	default:
		return nil, fmt.Errorf("invalid compression type: %v", dec.metaProto.Compression)
	}

	var (
		iter ForwardStringIterator
		err  error
	)
	switch dec.metaProto.Encoding {
	case encodingpb.EncodingType_RAW_SIZE:
		iter = dec.decodeRawSize(reader)
	case encodingpb.EncodingType_DICTIONARY:
		iter, err = dec.decodeDictionary(reader)
	default:
		return nil, fmt.Errorf("invalid encoding type: %v", dec.metaProto.Encoding)
	}

	return iter, err
}

// Reset the string encoder between `Encode` calls.
func (dec *StringDec) Reset() {
	dec.metaProto.Reset()
	dec.dictionaryProto.Reset()
}

func (dec *StringDec) decodeDictionary(reader io.Reader) (*DictionaryBasedStringIterator, error) {
	return NewDictionaryBasedStringIterator(reader, &dec.dictionaryProto, &dec.buf)
}

func (dec *StringDec) decodeRawSize(reader io.Reader) *RawSizeStringIterator {
	return NewRawSizeStringIterator(reader, &dec.buf)
}
