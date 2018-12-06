package encoding

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"

	"github.com/valyala/gozstd"

	"github.com/xichen2020/eventdb/generated/proto/encodingpb"
	"github.com/xichen2020/eventdb/x/bytes"
	"github.com/xichen2020/eventdb/x/unsafe"
)

// Default initial size of data slice.
const (
	defaultInitalDataSliceSize = 1 << 8
)

// Maximum cardinality allowed for dictionary encoding.
var (
	dictEncodingMaxCardinality = 1 << 16
)

// StringEncoder encodes string valuesIt.
type StringEncoder interface {
	// Encode encodes a collection of strings and writes the encoded bytes to the writer.
	// Callers should explicitly call `Reset` before subsequent call to `Encode`.
	Encode(writer io.Writer, valuesIt RewindableStringIterator) error
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
		buf: make([]byte, 8),
		metaProto: encodingpb.StringMeta{
			// Default compression type.
			Compression: encodingpb.CompressionType_ZSTD,
		},
		data: make([]string, defaultInitalDataSliceSize),
	}
}

// Encode encodes a collection of strings and writes the encoded bytes to the writer.
func (enc *StringEnc) Encode(
	writer io.Writer,
	valuesIt RewindableStringIterator,
) error {
	// TODO(bodu): Do some perf benchmarking to see whether we want to allocate a new map
	// or clear an existing one.
	dictionary := make(map[string]int64)
	var (
		idx       int64
		curr      string
		maxLength int
	)
	for valuesIt.Next() {
		curr = valuesIt.Current()
		if len(dictionary) < dictEncodingMaxCardinality {
			// Only add to dictionary if
			if _, ok := dictionary[curr]; !ok {
				dictionary[curr] = idx
				// Ensure data size or grow slice. Target size is idx+1.
				enc.data = enc.ensureStringSliceSize(enc.data, int(idx)+1)
				enc.data[idx] = curr
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
	if len(dictionary) >= dictEncodingMaxCardinality {
		enc.metaProto.Encoding = encodingpb.EncodingType_RAW_SIZE
	} else {
		enc.metaProto.Encoding = encodingpb.EncodingType_DICTIONARY
	}

	enc.buf = bytes.EnsureBufferSize(enc.buf, enc.metaProto.Size(), bytes.DontCopyData)
	if err := ProtoEncode(&enc.metaProto, enc.buf, writer); err != nil {
		return err
	}

	var compressWriter io.Writer
	// Compress the bytes.
	switch enc.metaProto.Compression {
	case encodingpb.CompressionType_ZSTD:
		// TODO(bodu): Figure out a cleaner way to do this.
		w := gozstd.NewWriter(writer)
		defer w.Close()
		compressWriter = w
	default:
		return fmt.Errorf("invalid compression type: %v", enc.metaProto.Compression)
	}

	switch enc.metaProto.Encoding {
	case encodingpb.EncodingType_RAW_SIZE:
		if err := enc.encodeLength(compressWriter, valuesIt); err != nil {
			return err
		}
	case encodingpb.EncodingType_DICTIONARY:
		if err := enc.encodeDictionary(compressWriter, valuesIt, dictionary, enc.data[:idx]); err != nil {
			return err
		}
	default:
		return fmt.Errorf("invalid encoding type: %v", enc.metaProto.Encoding)
	}

	return nil
}

// Dictionary encoding strategy is to write
// and then the bytes for the string.
func (enc *StringEnc) encodeDictionary(
	writer io.Writer,
	valuesIt RewindableStringIterator,
	dictionary map[string]int64,
	data []string,
) error {
	// Write out the dictionary data first (to be used for decoding).
	enc.dictionaryProto.Data = data
	enc.buf = bytes.EnsureBufferSize(enc.buf, enc.dictionaryProto.Size(), bytes.DontCopyData)
	if err := ProtoEncode(&enc.dictionaryProto, enc.buf, writer); err != nil {
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

func (enc *StringEnc) ensureStringSliceSize(data []string, targetSize int) []string {
	if targetSize < len(data) {
		return data
	}
	newSlice := make([]string, len(data)*2)
	copy(newSlice, data)
	return newSlice
}
