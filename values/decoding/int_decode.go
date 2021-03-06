package decoding

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"

	"github.com/xichen2020/eventdb/generated/proto/encodingpb"
	"github.com/xichen2020/eventdb/values"
	"github.com/xichen2020/eventdb/values/iterator"
	"github.com/xichen2020/eventdb/x/convert"
	xio "github.com/xichen2020/eventdb/x/io"
	xproto "github.com/xichen2020/eventdb/x/proto"
)

// IntDecoder decodes int values.
type IntDecoder interface {
	// DecodeRaw decodes raw encoded bytes into a closeable int values.
	DecodeRaw(data []byte) (values.CloseableIntValues, error)
}

// intDecoder is an int Decoder.
type intDecoder struct {
	buf             []byte
	dictionaryProto encodingpb.IntDictionary
}

// NewIntDecoder creates a new int Decoder.
func NewIntDecoder() IntDecoder {
	return &intDecoder{}
}

// DecodeRaw decodes raw encoded bytes into a closeable int values.
func (dec *intDecoder) DecodeRaw(data []byte) (values.CloseableIntValues, error) {
	var metaProto encodingpb.IntMeta
	bytesRead, err := xproto.DecodeIntMetaRaw(data, &metaProto)
	if err != nil {
		return nil, err
	}

	dec.dictionaryProto.Data = dec.dictionaryProto.Data[:0]
	nRead, dict, err := tryDecodeIntDictionary(metaProto, data[bytesRead:], &dec.dictionaryProto, &dec.buf)
	if err != nil {
		return nil, err
	}
	return newFsBasedIntValues(metaProto, data[bytesRead:], dict, nRead), nil
}

func tryDecodeIntDictionary(
	metaProto encodingpb.IntMeta,
	data []byte,
	dictionaryProto *encodingpb.IntDictionary,
	decodeBuf *[]byte,
) (int, []int, error) {
	switch metaProto.Encoding {
	case encodingpb.EncodingType_VARINT:
		return 0, nil, nil
	case encodingpb.EncodingType_DELTA:
		return 0, nil, nil
	case encodingpb.EncodingType_DICTIONARY:
		reader := bytes.NewReader(data)
		bytesRead, err := xproto.DecodeIntDictionary(reader, dictionaryProto, decodeBuf)
		if err != nil {
			return 0, nil, err
		}
		var dict []int
		if bytesRead == 0 {
			return 0, nil, nil
		}
		var (
			bytesPerDictVal = int(metaProto.BytesPerDictionaryValue)
			minVal          = int(metaProto.MinValue)
			encodedData     = dictionaryProto.Data
		)
		dict = make([]int, 0, len(encodedData)/bytesPerDictVal)
		for start := 0; start < len(encodedData); start += bytesPerDictVal {
			decodedVal := minVal + int(xio.ReadInt(bytesPerDictVal, encodedData[start:]))
			dict = append(dict, decodedVal)
		}
		return bytesRead, dict, nil
	default:
		return 0, nil, fmt.Errorf("invalid int encoding type: %v", metaProto.Encoding)
	}
}

// newIntIteratorFromMeta creates a new int iterator from int metadata.
func newIntIteratorFromMeta(
	metaProto encodingpb.IntMeta,
	encodedBytes []byte,
	extDict []int,
	encodedDictBytes int,
) (iterator.ForwardIntIterator, error) {
	reader := xio.NewReaderNoopCloser(bytes.NewReader(encodedBytes))
	switch metaProto.Encoding {
	case encodingpb.EncodingType_VARINT:
		return newVarintIntIterator(reader), nil
	case encodingpb.EncodingType_DELTA:
		return newDeltaIntIterator(reader, metaProto.BitsPerEncodedValue, int(metaProto.NumValues), convert.IntAddIntFn), nil
	case encodingpb.EncodingType_DICTIONARY:
		// Simply discard the bytes used to encode the dictionary since we've already
		// received the dictionary parameter.
		_, err := io.CopyN(ioutil.Discard, reader, int64(encodedDictBytes))
		if err != nil {
			return nil, err
		}
		return newDictionaryBasedIntIterator(
			reader,
			extDict,
			int(metaProto.BitsPerEncodedValue),
			int(metaProto.NumValues),
		), nil
	default:
		return nil, fmt.Errorf("invalid int encoding type: %v", metaProto.Encoding)
	}
}

// newIntDictionaryIndexIterator creates a new int iterator that can be used to iterate over
// dictionary index values.
// NB: EncodingType must be encodingpb.EncodingType_DICTIONARY when calling this function.
func newIntDictionaryIndexIterator(
	encodedBytes []byte,
	encodedDictBytes int,
	bitsPerEncodedValue int,
	numEncodedValues int,
) (iterator.ForwardIntIterator, error) {
	reader := xio.NewReaderNoopCloser(bytes.NewReader(encodedBytes))
	// Simply discard the bytes used to encode the dictionary since we've already
	// received the dictionary parameter.
	_, err := io.CopyN(ioutil.Discard, reader, int64(encodedDictBytes))
	if err != nil {
		return nil, err
	}
	return newBitStreamIntIterator(reader, bitsPerEncodedValue, numEncodedValues), nil
}
