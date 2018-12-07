package encoding

import (
	"io"
	"math"
	"math/bits"

	"github.com/xichen2020/eventdb/generated/proto/encodingpb"
	"github.com/xichen2020/eventdb/x/proto"

	bitstream "github.com/dgryski/go-bitstream"
)

var (
	dictEncodingMaxCardinalityInt = 1 << 8
)

// IntEncoder encodes int values.
type IntEncoder interface {
	// Encode encodes a collection of ints and writes the encoded bytes to the writer.
	// Callers should explicitly call `Reset` before subsequent call to `Encode`.
	Encode(writer io.Writer, valuesIt RewindableIntIterator) error

	// Reset resets the encoder.
	Reset()
}

// IntEnc is a int encoder.
type IntEnc struct {
	metaProto       encodingpb.IntMeta
	dictionaryProto encodingpb.IntDictionary
	buf             []byte
}

// NewIntEncoder creates a new int encoder.
func NewIntEncoder() *IntEnc {
	return &IntEnc{
		// Make buf at least big enough to hold Uint64 values.
		buf: make([]byte, uint64SizeBytes),
	}
}

// Encode encodes a collection of ints and writes the encoded bytes to the writer.
func (enc *IntEnc) Encode(
	writer io.Writer,
	valuesIt RewindableIntIterator,
) error {
	// Determine whether we want to do table compression
	var (
		max  int
		curr int
		idx  int
	)
	min := math.MaxInt64
	dict := make(map[int]int)
	for valuesIt.Next() {
		curr = valuesIt.Current()
		// Only grow the dict map if we are below dictEncodingMaxCardinalityInt.
		// This way we track if we've exceeded the max # of uniques.
		if len(dict) <= dictEncodingMaxCardinalityInt {
			if _, ok := dict[curr]; !ok {
				dict[curr] = idx
				idx++
			}
		}

		// Track max/min.
		max = int(math.Max(float64(curr), float64(max)))
		min = int(math.Min(float64(curr), float64(min)))
	}

	if valuesIt.Err() != nil {
		return valuesIt.Err()
	}

	// Rewind iteration.
	valuesIt.Rewind()

	switch {
	case len(dict) <= dictEncodingMaxCardinalityInt:
		// Table encode if there is less than 256 unique valuesIt.
		enc.metaProto.Encoding = encodingpb.EncodingType_DICTIONARY
		// Min number of bits to encode each actual value into the table.
		enc.metaProto.BytesPerDictionaryValue = int64(math.Ceil(float64(bits.Len(uint(max))) / 8.0))
		// Min number of bits required to encode table indices.
		enc.metaProto.BitsPerEncodedValue = int64(bits.Len(uint(len(dict))))
	default:
		// Default to delta encoding.
		enc.metaProto.Encoding = encodingpb.EncodingType_DELTA
		enc.metaProto.BitsPerEncodedValue = int64(bits.Len(uint(max - min)))
	}

	if err := proto.EncodeIntMeta(&enc.metaProto, &enc.buf, writer); err != nil {
		return err
	}

	switch enc.metaProto.Encoding {
	case encodingpb.EncodingType_DICTIONARY:
		if err := enc.encodeDictionary(writer, enc.metaProto.BytesPerDictionaryValue, enc.metaProto.BitsPerEncodedValue, dict, valuesIt); err != nil {
			return err
		}
	case encodingpb.EncodingType_DELTA:
		if err := enc.encodeDelta(writer, enc.metaProto.BitsPerEncodedValue, valuesIt); err != nil {
			return err
		}
	}

	return nil
}

// Reset resets the encoder.
func (enc *IntEnc) Reset() {
	enc.metaProto.Reset()
	enc.dictionaryProto.Reset()
}

func (enc *IntEnc) encodeDelta(
	writer io.Writer,
	bitsPerEncodedValue int64,
	valuesIt RewindableIntIterator,
) error {
	// Get first record and set as the start.
	valuesIt.Next()
	curr := valuesIt.Current()
	enc.metaProto.DeltaStart = int64(curr)

	// Encode the first value which is always a delta of 0.
	bitWriter := bitstream.NewWriter(writer)
	// Write an extra bit to encode the sign of the delta.
	if err := bitWriter.WriteBits(uint64(0), int(bitsPerEncodedValue)+1); err != nil {
		return err
	}

	var delta int
	last := curr
	for valuesIt.Next() {
		curr = valuesIt.Current()
		delta = curr - last
		if delta < 0 {
			// Flip the sign.
			delta *= -1
			// Set the MSB if the sign is negative.
			delta |= 1 << uint(bitsPerEncodedValue)
		}
		if err := bitWriter.WriteBits(uint64(delta), int(bitsPerEncodedValue)+1); err != nil {
			return err
		}
		// Housekeeping.
		last = curr
	}

	return valuesIt.Err()
}

func (enc *IntEnc) encodeDictionary(
	writer io.Writer,
	bytesPerDictionaryValue int64,
	bitsPerEncodedValue int64,
	dict map[int]int,
	valuesIt RewindableIntIterator,
) error {
	sortedDict := make([]uint64, len(dict))
	for value, idx := range dict {
		sortedDict[idx] = uint64(value)
	}

	var start int
	enc.dictionaryProto.Data = make([]byte, len(sortedDict)*int(bytesPerDictionaryValue))
	for idx, value := range sortedDict {
		endianness.PutUint64(enc.buf, value)
		start = idx * int(bytesPerDictionaryValue)
		copy(enc.dictionaryProto.Data[start:start+int(bytesPerDictionaryValue)], enc.buf[:bytesPerDictionaryValue])
	}

	if err := proto.EncodeIntDictionary(&enc.dictionaryProto, &enc.buf, writer); err != nil {
		return err
	}

	bitWriter := bitstream.NewWriter(writer)
	var encodedValue uint64
	for valuesIt.Next() {
		encodedValue = uint64(dict[valuesIt.Current()])
		if err := bitWriter.WriteBits(encodedValue, int(bitsPerEncodedValue)); err != nil {
			return err
		}
	}
	return valuesIt.Err()
}
