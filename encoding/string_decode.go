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
func (dec *StringDec) Decode(sr io.SeekableReader) (ForwardStringIterator, error) {
	// Reset internal state at the beginning of every `Decode` call.
	dec.reset(sr)

	// Decode metadata first.
	if err := proto.DecodeStringMeta(&dec.metaProto, &dec.buf, sr); err != nil {
		return nil, err
	}

	var skippableReader io.SkippableReader
	switch dec.metaProto.Compression {
	case encodingpb.CompressionType_ZSTD:
		if dec.metaProto.UseBlocks {
			skippableReader = newSkippableCompressReader(dec.br)
			skippableReader.Skip()
		} else {
			skippableReader = NewCompressReader(sr)
		}
	default:
		return nil, fmt.Errorf("invalid compression type: %v", dec.metaProto.Compression)
	}

	var (
		iter ForwardStringIterator
		err  error
	)
	switch dec.metaProto.Encoding {
	case encodingpb.EncodingType_RAW_SIZE:
		iter = dec.decodeRawSize(skippableReader)
	case encodingpb.EncodingType_DICTIONARY:
		iter, err = dec.decodeDictionary(skippableReader)
	default:
		return nil, fmt.Errorf("invalid encoding type: %v", dec.metaProto.Encoding)
	}

	return iter, err
}

// Reset the string encoder between `Encode` calls.
func (dec *StringDec) reset(reader io.SeekableReader) {
	dec.metaProto.Reset()
	dec.dictionaryProto.Reset()
	dec.br.reset(reader)
}

func (dec *StringDec) decodeDictionary(reader io.Reader) (*DictionaryBasedStringIterator, error) {
	return newDictionaryBasedStringIterator(reader, &dec.dictionaryProto, &dec.buf)
}

func (dec *StringDec) decodeRawSize(skippableReader io.SkippableReader) *RawSizeStringIterator {
	return newRawSizeStringIterator(skippableReader, &dec.buf)
}
