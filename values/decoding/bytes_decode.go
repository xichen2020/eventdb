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

// BytesDecoder decodes bytes values.
type BytesDecoder interface {
	// DecodeRaw decodes raw encoded bytes into a closeable bytes values.
	DecodeRaw(data []byte) (values.CloseableBytesValues, error)
}

// bytesDecoder is a bytes decoder. It is not thread-safe.
type bytesDecoder struct {
	buf             []byte
	bytesArrayProto encodingpb.BytesArray
}

// NewBytesDecoder creates a new bytes decoder.
func NewBytesDecoder() BytesDecoder {
	return &bytesDecoder{}
}

// DecodeRaw decodes raw encoded bytes into a closeable bytes values.
func (dec *bytesDecoder) DecodeRaw(data []byte) (values.CloseableBytesValues, error) {
	var metaProto encodingpb.BytesMeta
	bytesRead, err := xproto.DecodeBytesMetaRaw(data, &metaProto)
	if err != nil {
		return nil, err
	}

	dec.bytesArrayProto.Data = dec.bytesArrayProto.Data[:0]
	nRead, err := tryDecodeBytesDictionary(metaProto, data[bytesRead:], &dec.bytesArrayProto, &dec.buf)
	if err != nil {
		return nil, err
	}

	return newFsBasedBytesValues(metaProto, data[bytesRead:], dec.bytesArrayProto.Data, nRead), nil
}

func tryDecodeBytesDictionary(
	metaProto encodingpb.BytesMeta,
	data []byte,
	bytesArrayProto *encodingpb.BytesArray,
	decodeBuf *[]byte,
) (int, error) {
	switch metaProto.Encoding {
	case encodingpb.EncodingType_RAW_SIZE:
		return 0, nil
	case encodingpb.EncodingType_DICTIONARY:
		reader, err := newBytesReaderFromMeta(metaProto, data)
		if err != nil {
			return 0, err
		}
		defer reader.Close()
		bytesRead, err := xproto.DecodeBytesArray(reader, bytesArrayProto, decodeBuf)
		if err != nil {
			return 0, err
		}
		return bytesRead, nil
	default:
		return 0, fmt.Errorf("invalid bytes encoding type: %v", metaProto.Encoding)
	}
}

// newBytesReaderFromMeta creates the appropriate reader from bytes encoding metadata.
func newBytesReaderFromMeta(
	metaProto encodingpb.BytesMeta,
	data []byte,
) (xio.SimpleReadCloser, error) {
	var reader xio.Reader = bytes.NewReader(data)
	switch metaProto.Compression {
	case encodingpb.CompressionType_ZSTD:
		return newCompressedReader(reader), nil
	default:
		return nil, fmt.Errorf("invalid compression type: %v", metaProto.Compression)
	}
}

// newBytesIteratorFromMeta creates a new bytes iterator from bytes metadata.
func newBytesIteratorFromMeta(
	metaProto encodingpb.BytesMeta,
	encodedBytes []byte,
	extDict [][]byte,
	encodedDictBytes int,
) (iterator.ForwardBytesIterator, error) {
	reader, err := newBytesReaderFromMeta(metaProto, encodedBytes)
	if err != nil {
		return nil, err
	}

	switch metaProto.Encoding {
	case encodingpb.EncodingType_RAW_SIZE:
		return newRawSizeBytesIterator(reader), nil
	case encodingpb.EncodingType_DICTIONARY:
		// Simply discard the bytes used to encode the dictionary since we've already
		// received the dictionary parameter.
		_, err := io.CopyN(ioutil.Discard, reader, int64(encodedDictBytes))
		if err != nil {
			return nil, err
		}
		return newDictionaryBasedBytesIterator(reader, extDict), nil
	default:
		return nil, fmt.Errorf("invalid bytes encoding type: %v", metaProto.Encoding)
	}
}

// newBytesDictionaryIndexIterator creates a new int iterator from bytes metadata that can be used
// to iterate over dictionary index values.
// NB: EncodingType must be encodingpb.EncodingType_DICTIONARY when calling this function.
func newBytesDictionaryIndexIterator(
	metaProto encodingpb.BytesMeta,
	encodedBytes []byte,
	encodedDictBytes int,
) (iterator.ForwardIntIterator, error) {
	reader, err := newBytesReaderFromMeta(metaProto, encodedBytes)
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
