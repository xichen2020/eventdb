package decoding

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"

	"github.com/xichen2020/eventdb/generated/proto/encodingpb"
	"github.com/xichen2020/eventdb/values"
	"github.com/xichen2020/eventdb/values/iterator"
	xio "github.com/xichen2020/eventdb/x/io"
	xproto "github.com/xichen2020/eventdb/x/proto"
)

// StringDecoder decodes string values.
type StringDecoder interface {
	// DecodeRaw decodes raw encoded bytes into a closeable string values.
	DecodeRaw(data []byte) (values.CloseableStringValues, error)
}

// stringDecoder is a string decoder. It is not thread-safe.
type stringDecoder struct {
	buf              []byte
	stringArrayProto encodingpb.StringArray
}

// NewStringDecoder creates a new string decoder.
func NewStringDecoder() StringDecoder {
	return &stringDecoder{}
}

// DecodeRaw decodes raw encoded bytes into a closeable string values.
func (dec *stringDecoder) DecodeRaw(data []byte) (values.CloseableStringValues, error) {
	var metaProto encodingpb.StringMeta
	bytesRead, err := xproto.DecodeStringMetaRaw(data, &metaProto)
	if err != nil {
		return nil, err
	}

	dec.stringArrayProto.Data = dec.stringArrayProto.Data[:0]
	nRead, err := tryDecodeStringDictionary(metaProto, data[bytesRead:], &dec.stringArrayProto, &dec.buf)
	if err != nil {
		return nil, err
	}

	return newFsBasedStringValues(metaProto, data[bytesRead:], dec.stringArrayProto.Data, nRead), nil
}

func tryDecodeStringDictionary(
	metaProto encodingpb.StringMeta,
	data []byte,
	stringArrayProto *encodingpb.StringArray,
	decodeBuf *[]byte,
) (int, error) {
	switch metaProto.Encoding {
	case encodingpb.EncodingType_RAW_SIZE:
		return 0, nil
	case encodingpb.EncodingType_DICTIONARY:
		reader, err := newStringReaderFromMeta(metaProto, data)
		if err != nil {
			return 0, err
		}
		bytesRead, err := xproto.DecodeStringArray(reader, stringArrayProto, decodeBuf)
		if err != nil {
			return 0, err
		}
		return bytesRead, nil
	default:
		return 0, fmt.Errorf("invalid string encoding type: %v", metaProto.Encoding)
	}
}

// newStringReaderFromMeta creates the appropriate reader from string encoding metadata.
func newStringReaderFromMeta(
	metaProto encodingpb.StringMeta,
	data []byte,
) (xio.Reader, error) {
	var reader xio.Reader = bytes.NewReader(data)
	switch metaProto.Compression {
	case encodingpb.CompressionType_ZSTD:
		return newCompressedReader(reader), nil
	default:
		return nil, fmt.Errorf("invalid compression type: %v", metaProto.Compression)
	}
}

// newStringIteratorFromMeta creates a new string iterator from string metadata.
func newStringIteratorFromMeta(
	metaProto encodingpb.StringMeta,
	encodedBytes []byte,
	extDict []string,
	encodedDictBytes int,
) (iterator.ForwardStringIterator, error) {
	reader, err := newStringReaderFromMeta(metaProto, encodedBytes)
	if err != nil {
		return nil, err
	}

	switch metaProto.Encoding {
	case encodingpb.EncodingType_RAW_SIZE:
		return newRawSizeStringIterator(reader), nil
	case encodingpb.EncodingType_DICTIONARY:
		// Simply discard the bytes used to encode the dictionary since we've already
		// received the dictionary parameter.
		_, err := io.CopyN(ioutil.Discard, reader, int64(encodedDictBytes))
		if err != nil {
			return nil, err
		}
		return newDictionaryBasedStringIterator(reader, extDict), nil
	default:
		return nil, fmt.Errorf("invalid string encoding type: %v", metaProto.Encoding)
	}
}

// newDictionaryIndexIteratorFromMeta creates a new int iterator from string metadata that can be used
// to iterate over dictionary index values.
// NB: EncodingType must be encodingpb.EncodingType_DICTIONARY when calling this function.
func newDictionaryIndexIteratorFromMeta(
	metaProto encodingpb.StringMeta,
	encodedBytes []byte,
	encodedDictBytes int,
) (iterator.ForwardIntIterator, error) {
	reader, err := newStringReaderFromMeta(metaProto, encodedBytes)
	if err != nil {
		return nil, err
	}
	// Simply discard the bytes used to encode the dictionary since we've already
	// received the dictionary parameter.
	_, err = io.CopyN(ioutil.Discard, reader, int64(encodedDictBytes))
	if err != nil {
		return nil, err
	}
	return newVarintIntIterator(reader), nil
}
