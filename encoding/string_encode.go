package encoding

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"

	"github.com/xichen2020/eventdb/generated/proto/encodingpb"
	"github.com/xichen2020/eventdb/x/proto"
	"github.com/xichen2020/eventdb/x/unsafe"

	"github.com/valyala/gozstd"
)

// Default initial size of data slice.
const (
	defaultInitialDataSliceSize = 1 << 8
)

// Maximum cardinality allowed for dictionary encoding.
var (
	dictEncodingMaxCardinalityString = 1 << 16
)

// StringEncoder encodes string values.
type StringEncoder interface {
	// Encode encodes a collection of strings and writes the encoded bytes to the writer.
	// Callers should explicitly call `Reset` before subsequent call to `Encode`.
	Encode(writer io.Writer, valuesIt RewindableStringIterator) error

	// Reset the string encoder between `Encode` calls.
	Reset()
}

// StringEnc is a string encoder.
type StringEnc struct {
	buf             []byte
	metaProto       encodingpb.StringMeta
	dictionaryProto encodingpb.StringArray
	data            []string
}

// NewStringEncoder creates a new string encoder.
func NewStringEncoder() *StringEnc {
	return &StringEnc{
		// Make at least enough room for the binary Varint methods not to panic.
		buf: make([]byte, binary.MaxVarintLen64),
		metaProto: encodingpb.StringMeta{
			// Default compression type.
			Compression: encodingpb.CompressionType_ZSTD,
		},
		data: make([]string, 0, defaultInitialDataSliceSize),
	}
}

// Encode encodes a collection of strings and writes the encoded bytes to the writer.
func (enc *StringEnc) Encode(
	writer io.Writer,
	valuesIt RewindableStringIterator,
) error {
	// TODO(bodu): Do some perf benchmarking to see whether we want to allocate a new map
	// or clear an existing one.
	dictionary := make(map[string]int64, dictEncodingMaxCardinalityString)
	var (
		idx       int64
		curr      string
		maxLength int
	)
	for valuesIt.Next() {
		curr = valuesIt.Current()
		if len(dictionary) < dictEncodingMaxCardinalityString {
			// Only add to dictionary if
			if _, ok := dictionary[curr]; !ok {
				dictionary[curr] = idx
				// Ensure data size or grow slice. Target size is idx+1.
				enc.data = append(enc.data, curr)
				idx++
			}
		}
		maxLength = int(math.Max(float64(len(curr)), float64(maxLength)))
	}

	if err := valuesIt.Err(); err != nil {
		return err
	}

	// Rewind values.
	valuesIt.Rewind()

	// TODO(bodu): This should take into account the total # of items
	// at some point. The total count should exceed the # of uniques by a certain
	// margin to justify table compression.
	if len(dictionary) >= dictEncodingMaxCardinalityString {
		enc.metaProto.Encoding = encodingpb.EncodingType_RAW_SIZE
	} else {
		enc.metaProto.Encoding = encodingpb.EncodingType_DICTIONARY
	}

	if err := proto.EncodeStringMeta(&enc.metaProto, &enc.buf, writer); err != nil {
		return err
	}

	// Compress the bytes.
	switch enc.metaProto.Compression {
	case encodingpb.CompressionType_ZSTD:
		// TODO(bodu): Figure out a cleaner way to do this.
		compressWriter := gozstd.NewWriter(writer)
		defer compressWriter.Close()
		writer = compressWriter
	default:
		return fmt.Errorf("invalid compression type: %v", enc.metaProto.Compression)
	}

	switch enc.metaProto.Encoding {
	case encodingpb.EncodingType_RAW_SIZE:
		return enc.encodeLength(writer, valuesIt)
	case encodingpb.EncodingType_DICTIONARY:
		return enc.encodeDictionary(writer, valuesIt, dictionary, enc.data[:idx])
	default:
		return fmt.Errorf("invalid encoding type: %v", enc.metaProto.Encoding)
	}
}

// Reset the string encoder between `Encode` calls.
func (enc *StringEnc) Reset() {
	enc.metaProto.Reset()
	enc.dictionaryProto.Reset()
	enc.data = enc.data[:0]
}

// Dictionary encoding strategy is to write all unique strings into an array
// and then use the array idx to represent the string value.
func (enc *StringEnc) encodeDictionary(
	writer io.Writer,
	valuesIt RewindableStringIterator,
	dictionary map[string]int64,
	data []string,
) error {
	// Write out the dictionary data first (to be used for decoding).
	enc.dictionaryProto.Data = data
	if err := proto.EncodeStringArray(&enc.dictionaryProto, &enc.buf, writer); err != nil {
		return err
	}

	// Write out the dictionary values.
	for valuesIt.Next() {
		idx := dictionary[valuesIt.Current()]
		n := binary.PutVarint(enc.buf, idx)
		if _, err := writer.Write(enc.buf[:n]); err != nil {
			return err
		}
	}
	return valuesIt.Err()
}

// Length encoding strategy is to write the number of bytes as a VInt
// and then the bytes for the string.
func (enc *StringEnc) encodeLength(
	writer io.Writer,
	valuesIt RewindableStringIterator,
) error {
	for valuesIt.Next() {
		b := unsafe.ToBytes(valuesIt.Current())
		n := binary.PutVarint(enc.buf, int64(len(b)))
		if _, err := writer.Write(enc.buf[:n]); err != nil {
			return err
		}
		if _, err := writer.Write(b); err != nil {
			return err
		}
	}
	return valuesIt.Err()
}
