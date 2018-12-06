package encoding

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/xichen2020/eventdb/generated/proto/encodingpb"
	"github.com/xichen2020/eventdb/x/bytes"
)

// StringDecoder decodes string values.
type StringDecoder interface {
	// Decode decodes strings from reader.
	Decode(reader Reader) (ForwardStringIterator, error)

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
func NewStringDecoder() *StringDec {
	return &StringDec{
		buf: make([]byte, 4),
	}
}

// Decode encoded string data in a streaming fashion.
func (dec *StringDec) Decode(reader Reader) (ForwardStringIterator, error) {
	// Decode metadata first.
	protoSizeBytes, err := binary.ReadVarint(reader)
	if err != nil {
		return nil, err
	}
	dec.buf = bytes.EnsureBufferSize(dec.buf, int(protoSizeBytes), bytes.DontCopyData)

	if _, err := io.ReadFull(reader, dec.buf[:protoSizeBytes]); err != nil {
		return nil, err
	}
	if err := dec.metaProto.Unmarshal(dec.buf[:protoSizeBytes]); err != nil {
		return nil, err
	}

	var compressReader Reader
	switch dec.metaProto.Compression {
	case encodingpb.CompressionType_ZSTD:
		compressReader = NewCompressReader(reader)
	default:
		return nil, fmt.Errorf("invalid compression type: %v", dec.metaProto.Compression)
	}

	var iter ForwardStringIterator
	switch dec.metaProto.Encoding {
	case encodingpb.EncodingType_RAW_SIZE:
		iter = dec.decodeRawSize(compressReader)
	case encodingpb.EncodingType_DICTIONARY:
		iter, err = dec.decodeDictionary(compressReader)
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

func (dec *StringDec) decodeDictionary(reader Reader) (*DictionaryBasedStringIterator, error) {
	return NewDictionaryBasedStringIterator(reader, &dec.dictionaryProto, &dec.buf)
}

func (dec *StringDec) decodeRawSize(reader Reader) *RawSizeStringIterator {
	return NewRawSizeStringIterator(reader, &dec.buf)
}
