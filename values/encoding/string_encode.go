package encoding

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"

	"github.com/xichen2020/eventdb/generated/proto/encodingpb"
	"github.com/xichen2020/eventdb/values"
	"github.com/xichen2020/eventdb/values/impl"
	"github.com/xichen2020/eventdb/values/iterator"
	"github.com/xichen2020/eventdb/x/pool"
	"github.com/xichen2020/eventdb/x/proto"
	"github.com/xichen2020/eventdb/x/unsafe"

	"github.com/valyala/gozstd"
)

const (
	// In order for string dictionary encoding to be eligible, the max cardinality of the values
	// collection must be no more than `num_values * stringDictEncodingMaxCardinalityPercent`.
	stringDictEncodingMaxCardinalityPercent = 1.0 / 64
)

// StringEncoder encodes string values.
type StringEncoder interface {
	// Encode encodes a collection of strings and writes the encoded bytes to the writer.
	Encode(strVals values.StringValues, writer io.Writer) error
}

// stringEncoder is a string encoder.
type stringEncoder struct {
	buf                  []byte
	stringLengthsEncoder IntEncoder
	dictionaryProto      encodingpb.StringArray
	stringLengthsProto   encodingpb.StringLengths
	intArrayPool         *pool.BucketizedIntArrayPool
}

// NewStringEncoder creates a new string encoder.
func NewStringEncoder(intArrayPool *pool.BucketizedIntArrayPool) StringEncoder {
	if intArrayPool == nil {
		intArrayPool = pool.NewBucketizedIntArrayPool(nil, nil)
		intArrayPool.Init(func(capacity int) []int { return make([]int, 0, capacity) })
	}
	return &stringEncoder{
		buf:                  make([]byte, binary.MaxVarintLen64), // Big enough to write a varint
		stringLengthsEncoder: NewIntEncoder(),
		intArrayPool:         intArrayPool,
	}
}

// Encode encodes a collection of strings and writes the encoded bytes to the writer.
func (enc *stringEncoder) Encode(strVals values.StringValues, writer io.Writer) error {
	// Reset internal state at the beginning of every `Encode` call.
	enc.reset()

	// TODO(bodu): Do some perf benchmarking to see whether we want to allocate a new map
	// or clear an existing one.
	valueMeta := strVals.Metadata()
	maxCardinalityAllowed := int(math.Ceil(1.0 * float64(valueMeta.Size) * stringDictEncodingMaxCardinalityPercent))
	dictionary := make(map[string]int, maxCardinalityAllowed)
	var (
		idx  int
		curr string
		// Used for raw size encoding.
		stringLengths = impl.NewArrayBasedIntValues(enc.intArrayPool)
	)
	valuesIt, err := strVals.Iter()
	if err != nil {
		return err
	}
	for valuesIt.Next() {
		curr = valuesIt.Current()
		stringLengths.Add(len(unsafe.ToBytes(curr)))

		if len(dictionary) <= maxCardinalityAllowed {
			if _, ok := dictionary[curr]; ok {
				continue
			}
			dictionary[curr] = idx
			idx++
		}
	}
	if err = valuesIt.Err(); err != nil {
		valuesIt.Close()
		return err
	}
	valuesIt.Close()

	// Always ZSTD compression for now.
	metaProto := encodingpb.StringMeta{
		Compression: encodingpb.CompressionType_ZSTD,
		NumValues:   int32(valueMeta.Size),
		MinValue:    valueMeta.Min,
		MaxValue:    valueMeta.Max,
	}
	if len(dictionary) > maxCardinalityAllowed {
		metaProto.Encoding = encodingpb.EncodingType_RAW_SIZE
	} else {
		metaProto.Encoding = encodingpb.EncodingType_DICTIONARY
	}
	if err = proto.EncodeStringMeta(&metaProto, &enc.buf, writer); err != nil {
		return err
	}

	// Compress the bytes.
	switch metaProto.Compression {
	case encodingpb.CompressionType_ZSTD:
		// TODO(bodu): Figure out a cleaner way to do this.
		compressWriter := gozstd.NewWriter(writer)
		// NB(xichen): Close flushes and closes the compressed writer but doesn't
		// close the writer wrapped by the compressed writer.
		defer compressWriter.Close()
		writer = compressWriter
	default:
		return fmt.Errorf("invalid compression type: %v", metaProto.Compression)
	}

	valuesIt, err = strVals.Iter()
	if err != nil {
		return err
	}
	defer valuesIt.Close()

	switch metaProto.Encoding {
	case encodingpb.EncodingType_RAW_SIZE:
		return enc.rawSizeEncode(valuesIt, writer, stringLengths.Seal())
	case encodingpb.EncodingType_DICTIONARY:
		return enc.dictionaryEncode(valuesIt, dictionary, writer)
	default:
		return fmt.Errorf("invalid encoding type: %v", metaProto.Encoding)
	}
}

func (enc *stringEncoder) reset() {
	enc.dictionaryProto.Data = enc.dictionaryProto.Data[:0]
	// Dont' need to reset stringLengthsProto data here because we replace it entirely every encode call.
}

// Dictionary encoding strategy is to write all unique strings into an array
// and then use the array idx to represent the string value.
// TODO(xichen): The dictionary values should be sorted to speed up lookup during query execution.
func (enc *stringEncoder) dictionaryEncode(
	valuesIt iterator.ForwardStringIterator,
	dictionary map[string]int,
	writer io.Writer,
) error {
	// Write out the dictionary data first (to be used for decoding).
	if cap(enc.dictionaryProto.Data) >= len(dictionary) {
		enc.dictionaryProto.Data = enc.dictionaryProto.Data[:len(dictionary)]
	} else {
		enc.dictionaryProto.Data = make([]string, len(dictionary))
	}
	for str, idx := range dictionary {
		enc.dictionaryProto.Data[idx] = str
	}

	if err := proto.EncodeStringArray(&enc.dictionaryProto, &enc.buf, writer); err != nil {
		return err
	}

	// Write out the dictionary values.
	for valuesIt.Next() {
		idx := dictionary[valuesIt.Current()]
		n := binary.PutVarint(enc.buf, int64(idx))
		if _, err := writer.Write(enc.buf[:n]); err != nil {
			return err
		}
	}
	return valuesIt.Err()
}

// Raw size encoding strategy is to write the number of bytes as a varint
// and then the bytes for the string.
// TODO(xichen): Encode the string lengths using int encoder.
func (enc *stringEncoder) rawSizeEncode(
	valuesIt iterator.ForwardStringIterator,
	writer io.Writer,
	stringLengths values.CloseableIntValues,
) error {
	// Use encoder buffer as underlying data slice.
	buf := bytes.NewBuffer(enc.buf)
	buf.Reset()
	// Encode the string lengths first using the int encoder.
	if err := enc.stringLengthsEncoder.Encode(stringLengths, buf); err != nil {
		return err
	}
	// Replace the encoder buf w/ newly allocated larger buf if one was allocated.
	if buf.Cap() > len(enc.buf) {
		enc.buf = buf.Bytes()[:buf.Cap()]
	}
	enc.stringLengthsProto.Data = enc.buf

	if err := proto.EncodeStringLengths(&enc.stringLengthsProto, &enc.buf, writer); err != nil {
		return err
	}

	// Write out the actual strings.
	for valuesIt.Next() {
		b := unsafe.ToBytes(valuesIt.Current())
		if _, err := writer.Write(b); err != nil {
			return err
		}
	}
	return valuesIt.Err()
}
