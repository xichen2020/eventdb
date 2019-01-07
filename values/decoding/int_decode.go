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
	nRead, err := tryDecodeIntDictionary(metaProto, data[bytesRead:], &dec.dictionaryProto, &dec.buf)
	if err != nil {
		return nil, err
	}

	return newFsBasedIntValues(metaProto, data[bytesRead:], dec.dictionaryProto.Data, nRead), nil
}

func tryDecodeIntDictionary(
	metaProto encodingpb.IntMeta,
	data []byte,
	dictionaryProto *encodingpb.IntDictionary,
	decodeBuf *[]byte,
) (int, error) {
	switch metaProto.Encoding {
	case encodingpb.EncodingType_DELTA:
		return 0, nil
	case encodingpb.EncodingType_DICTIONARY:
		reader := bytes.NewReader(data)
		bytesRead, err := xproto.DecodeIntDictionary(reader, dictionaryProto, decodeBuf)
		if err != nil {
			return 0, err
		}
		return bytesRead, nil
	default:
		return 0, fmt.Errorf("invalid int encoding type: %v", metaProto.Encoding)
	}
}

// newIntIteratorFromMeta creates a new int iterator from int metadata.
func newIntIteratorFromMeta(
	metaProto encodingpb.IntMeta,
	encodedBytes []byte,
	extDict []byte,
	encodedDictBytes int,
) (iterator.ForwardIntIterator, error) {
	reader := bytes.NewReader(encodedBytes)
	switch metaProto.Encoding {
	case encodingpb.EncodingType_DELTA:
		return newDeltaIntIterator(reader, metaProto.BitsPerEncodedValue, convert.IntAddIntFn), nil
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
			int(metaProto.MinValue),
			int(metaProto.BytesPerDictionaryValue),
			int(metaProto.BitsPerEncodedValue),
		), nil
	default:
		return nil, fmt.Errorf("invalid int encoding type: %v", metaProto.Encoding)
	}
}
