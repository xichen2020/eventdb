package encoding

import (
	"fmt"

	"github.com/xichen2020/eventdb/generated/proto/encodingpb"
	"github.com/xichen2020/eventdb/x/bytes"
)

// StringDecoder decodes string values.
type StringDecoder interface {
	// Decode decodes strings from reader.
	Decode(reader Reader) (ForwardStringIterator, error)
}

// StringDec is a string decoder.
type StringDec struct {
	buf             []byte
	metaProto       encodingpb.StringMeta
	dictionaryProto encodingpb.StringArray
}

// NewStringDecoder creates a new string decoder.
func NewStringDecoder() *StringDec {
	return &StringDec{
		buf: make([]byte, 4),
	}
}

// Decode encoded string data in a streaming fashion.
func (dec *StringDec) Decode(reader Reader) (ForwardStringIterator, error) {
	// Decode metadata first.
	if _, err := reader.Read(dec.buf[:Uint32SizeBytes]); err != nil {
		return nil, err
	}
	protoSizeBytes := int(endianness.Uint32(dec.buf[:Uint32SizeBytes]))
	dec.buf = bytes.EnsureBufferSize(dec.buf, protoSizeBytes, bytes.DontCopyData)

	if err := ProtoDecode(&dec.metaProto, dec.buf[:protoSizeBytes], reader); err != nil {
		return nil, err
	}

	var compressReader Reader
	switch dec.metaProto.Compression {
	case encodingpb.CompressionType_ZSTD:
		compressReader = NewCompressReader(reader)
	default:
		return nil, fmt.Errorf("invalid compression type: %v", dec.metaProto.Compression)
	}

	var (
		iter ForwardStringIterator
		err  error
	)
	switch dec.metaProto.Encoding {
	case encodingpb.EncodingType_RAW_SIZE:
		iter = dec.decodeLength(compressReader)
	case encodingpb.EncodingType_DICTIONARY:
		iter, err = dec.decodeDictionary(compressReader)
	default:
		return nil, fmt.Errorf("invalid encoding type: %v", dec.metaProto.Encoding)
	}

	return iter, err
}

func (dec *StringDec) decodeDictionary(reader Reader) (ForwardStringIterator, error) {
	return newDictionaryBasedStringIterator(reader, &dec.dictionaryProto, &dec.buf)
}

func (dec *StringDec) decodeLength(reader Reader) ForwardStringIterator {
	return newRawSizeStringIterator(reader)
}
