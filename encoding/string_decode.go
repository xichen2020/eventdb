package encoding

import (
	"errors"
	"fmt"

	"github.com/xichen2020/eventdb/generated/proto/encodingpb"
	"github.com/xichen2020/eventdb/x/io"
	"github.com/xichen2020/eventdb/x/proto"
)

// Errors.
var (
	errFailedToCastReaderToSkippableReader = errors.New("failed to cast reader to skippable reader")
	errFailedToCastReaderToSeekableReader  = errors.New("failed to cast reader to seekable reader")
)

// StringDecoder decodes string values.
type StringDecoder interface {
	// Decode decodes strings from reader.
	Decode(reader io.SeekableReader) (ForwardStringIterator, error)
}

// StringDec is a string decoder.
type StringDec struct {
	br              *blockReader
	buf             []byte
	metaProto       encodingpb.StringMeta
	blockMetaProto  encodingpb.BlockMeta
	dictionaryProto encodingpb.StringArray
}

// NewStringDecoder creates a new string decoder.
func NewStringDecoder() *StringDec {
	dec := &StringDec{}
	dec.br = newBlockReader(&dec.buf, &dec.blockMetaProto)
	return dec
}

// Decode encoded string data in a streaming fashion.
func (dec *StringDec) Decode(reader io.Reader) (ForwardStringIterator, error) {
	// Reset internal state at the beginning of every `Decode` call.
	dec.reset()

	// Decode metadata first.
	if err := proto.DecodeStringMeta(&dec.metaProto, &dec.buf, reader); err != nil {
		return nil, err
	}

	switch dec.metaProto.Compression {
	case encodingpb.CompressionType_ZSTD:
		if dec.metaProto.UseBlocks {
			sr, ok := reader.(io.SeekableReader)
			if !ok {
				return nil, errFailedToCastReaderToSeekableReader
			}
			// The block reader needs a ptr to the underlying seekable reader to seek ahead.
			dec.br.reset(sr)
			reader = newSkippableCompressReader(dec.br)
		} else {
			reader = newCompressReader(reader)
		}
	default:
		return nil, fmt.Errorf("invalid compression type: %v", dec.metaProto.Compression)
	}

	var (
		iter SeekableForwardStringIterator
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
func (dec *StringDec) reset() {
	dec.metaProto.Reset()
	dec.blockMetaProto.Reset()
	dec.dictionaryProto.Reset()
}

func (dec *StringDec) decodeDictionary(reader io.Reader) (*DictionaryBasedStringIterator, error) {
	return newDictionaryBasedStringIterator(reader, &dec.dictionaryProto, &dec.buf)
}

func (dec *StringDec) decodeRawSize(reader io.Reader) *RawSizeStringIterator {
	if dec.metaProto.UseBlocks {
		return newRawSizeStringIterator(reader, &dec.buf, RawSizeStringIteratorOptions{
			UseBlocks: true,
		})
	}
	return newRawSizeStringIterator(reader, &dec.buf, RawSizeStringIteratorOptions{})
}
