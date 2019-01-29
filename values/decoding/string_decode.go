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
	buf                  []byte
	stringArrayProto     encodingpb.StringArray
	stringLengthsProto   encodingpb.StringLengths
	stringLengthsIter    iterator.ForwardIntIterator
	stringLengthsDecoder IntDecoder
}

// NewStringDecoder creates a new string decoder.
func NewStringDecoder() StringDecoder {
	return &stringDecoder{
		stringLengthsDecoder: NewIntDecoder(),
	}
}

// DecodeRaw decodes raw encoded bytes into a closeable string values.
func (dec *stringDecoder) DecodeRaw(data []byte) (values.CloseableStringValues, error) {
	// Uncompressed data.
	var metaProto encodingpb.StringMeta
	bytesRead, err := xproto.DecodeStringMetaRaw(data, &metaProto)
	if err != nil {
		return nil, err
	}
	// Can slice off the metadata but cannot slice off the rest because they rely on ZSTD compression data frames.
	data = data[bytesRead:]

	// Compressed data.
	dec.stringArrayProto.Data = dec.stringArrayProto.Data[:0]
	nReadDictionary, err := tryDecodeStringDictionary(metaProto, data, &dec.stringArrayProto, &dec.buf)
	if err != nil {
		return nil, err
	}
	dec.stringLengthsIter = nil
	dec.stringLengthsProto.Data = dec.stringLengthsProto.Data[:0]
	nReadStringLengths, err := tryDecodeStringLengths(metaProto, data, &dec.stringLengthsProto, &dec.buf, dec.stringLengthsDecoder, &dec.stringLengthsIter)
	if err != nil {
		return nil, err
	}

	return newFsBasedStringValues(metaProto, dec.stringLengthsIter, data, dec.stringArrayProto.Data, nReadDictionary+nReadStringLengths), nil
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

func tryDecodeStringLengths(
	metaProto encodingpb.StringMeta,
	data []byte,
	stringLengthsProto *encodingpb.StringLengths,
	decodeBuf *[]byte,
	stringLengthsDecoder IntDecoder,
	stringLengthsIter *iterator.ForwardIntIterator,
) (int, error) {
	switch metaProto.Encoding {
	case encodingpb.EncodingType_RAW_SIZE:
		reader, err := newStringReaderFromMeta(metaProto, data)
		if err != nil {
			return 0, err
		}
		bytesRead, err := xproto.DecodeStringLengths(reader, stringLengthsProto, decodeBuf)
		if err != nil {
			return 0, err
		}
		values, err := stringLengthsDecoder.DecodeRaw(stringLengthsProto.Data)
		if err != nil {
			return 0, err
		}
		*stringLengthsIter, err = values.Iter()
		if err != nil {
			return 0, err
		}
		return bytesRead, nil
	case encodingpb.EncodingType_DICTIONARY:
		return 0, nil
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
	encodedStringLengths iterator.ForwardIntIterator,
	encodedBytes []byte,
	extDict []string,
	encodedExtraDataBytes int, // Represents # of bytes used to encode data dictionary or string lengths.
) (iterator.ForwardStringIterator, error) {
	reader, err := newStringReaderFromMeta(metaProto, encodedBytes)
	if err != nil {
		return nil, err
	}

	// Simply discard the bytes used to encode the dictionary since we've already
	// received the dictionary parameter.
	_, err = io.CopyN(ioutil.Discard, reader, int64(encodedExtraDataBytes))
	if err != nil {
		return nil, err
	}

	switch metaProto.Encoding {
	case encodingpb.EncodingType_RAW_SIZE:
		// Create an iterator for string lengths.
		return newRawSizeStringIterator(reader, encodedStringLengths), nil
	case encodingpb.EncodingType_DICTIONARY:
		return newDictionaryBasedStringIterator(reader, extDict), nil
	default:
		return nil, fmt.Errorf("invalid string encoding type: %v", metaProto.Encoding)
	}
}
