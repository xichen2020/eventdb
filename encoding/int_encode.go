package encoding

import (
	"fmt"
	"io"
	"math"
	"math/bits"

	"github.com/xichen2020/eventdb/generated/proto/encodingpb"
	xbytes "github.com/xichen2020/eventdb/x/bytes"
	xio "github.com/xichen2020/eventdb/x/io"
	"github.com/xichen2020/eventdb/x/proto"

	bitstream "github.com/dgryski/go-bitstream"
)

var (
	dictEncodingMaxCardinalityInt = 1 << 8
)

// IntEncoder encodes int values.
type IntEncoder interface {
	// Encode encodes a collection of ints and writes the encoded bytes to the writer.
	Encode(writer io.Writer, valuesIt RewindableIntIterator) error
}

// IntEnc is a int encoder.
type IntEnc struct {
	metaProto           encodingpb.IntMeta
	dictionaryProto     encodingpb.IntDictionary
	dictionaryProtoData []byte
	bitWriter           *bitstream.BitWriter
	buf                 []byte
}

// NewIntEncoder creates a new int encoder.
func NewIntEncoder() *IntEnc {
	return &IntEnc{
		// Make buf at least big enough to hold Uint64 values.
		buf: make([]byte, uint64SizeBytes),
		// Make a bitWriter w/ a nil write buffer that will be re-used for every Encode call.
		bitWriter: bitstream.NewWriter(nil),
	}
}

// Encode encodes a collection of ints and writes the encoded bytes to the writer.
func (enc *IntEnc) Encode(
	writer io.Writer,
	valuesIt RewindableIntIterator,
) error {
	// Reset internal state at the beginning of every `Encode` call.
	enc.reset(writer)

	// Determine whether we want to do table compression.
	var (
		firstVal int64
		max      = math.MinInt64
		min      = math.MaxInt64
		idx      int
		dict     = make(map[int]int, dictEncodingMaxCardinalityInt)
	)
	for valuesIt.Next() {
		curr := valuesIt.Current()

		if idx == 0 {
			firstVal = int64(curr)
		}

		// Only grow the dict map if we are below dictEncodingMaxCardinalityInt.
		// This way we track if we've exceeded the max # of uniques.
		if len(dict) <= dictEncodingMaxCardinalityInt {
			if _, ok := dict[curr]; !ok {
				dict[curr] = idx
				idx++
			}
		}

		// Track max/min.
		if curr < min {
			min = curr
		}
		if curr > max {
			max = curr
		}
	}

	if valuesIt.Err() != nil {
		return valuesIt.Err()
	}

	// Rewind iteration.
	valuesIt.Rewind()

	if len(dict) <= dictEncodingMaxCardinalityInt {
		// Table encode if there is less than 256 unique valuesIt.
		enc.metaProto.Encoding = encodingpb.EncodingType_DICTIONARY
		// Min value used to calculate dictionary values.
		//Each dictionary value is a positive number added to the min value.
		enc.metaProto.MinValue = int64(min)
		// Min number of bytes to encode each value into the int dict.
		enc.metaProto.BytesPerDictionaryValue = int64(math.Ceil(float64(bits.Len(uint(max-min))) / 8.0))
		// Min number of bits required to encode table indices.
		enc.metaProto.BitsPerEncodedValue = int64(bits.Len(uint(len(dict))))
	} else {
		// Default to delta encoding.
		enc.metaProto.Encoding = encodingpb.EncodingType_DELTA
		enc.metaProto.DeltaStart = firstVal
		enc.metaProto.BitsPerEncodedValue = int64(bits.Len(uint((max - min) + 1))) // Add 1 for the sign bit.
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
		if err := enc.encodeDelta(enc.metaProto.BitsPerEncodedValue, valuesIt); err != nil {
			return err
		}
	}

	return nil
}

// Reset resets the encoder.
func (enc *IntEnc) reset(writer io.Writer) {
	// Reset the bitWriter at the start of every `Encode` call.
	enc.bitWriter.Reset(writer)
	enc.metaProto.Reset()
	enc.dictionaryProto.Reset()
}

func (enc *IntEnc) encodeDelta(
	bitsPerEncodedValue int64,
	valuesIt RewindableIntIterator,
) error {
	// Encode the first value which is always a delta of 0.
	if !valuesIt.Next() {
		return valuesIt.Err()
	}

	// Write an extra bit to encode the sign of the delta.
	if err := enc.bitWriter.WriteBits(uint64(0), int(bitsPerEncodedValue)); err != nil {
		return err
	}

	negativeBit := 1 << uint(bitsPerEncodedValue)
	// Set last to be the first value and start iterating.
	last := valuesIt.Current()
	for valuesIt.Next() {
		curr := valuesIt.Current()
		delta := curr - last
		if delta < 0 {
			// Flip the sign.
			delta = -delta
			// Set the MSB if the sign is negative.
			delta |= negativeBit
		}
		if err := enc.bitWriter.WriteBits(uint64(delta), int(bitsPerEncodedValue)); err != nil {
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
	sortedDict := make([]int, len(dict))
	for value, idx := range dict {
		sortedDict[idx] = value
	}

	// Having a separate reference to dictionaryProtoData allows us to re-use the same byte slice across `Encode` calls.
	dictionaryProtoSize := len(sortedDict) * int(bytesPerDictionaryValue)
	enc.dictionaryProtoData = xbytes.EnsureBufferSize(enc.dictionaryProtoData, dictionaryProtoSize, xbytes.DontCopyData)
	start := 0
	for idx, value := range sortedDict {
		// The dictionary value is encoded as a positive number to be added to the minimum value.
		dictValue := value - int(enc.metaProto.MinValue)
		// Sanity check.
		if dictValue < 0 {
			return fmt.Errorf("dictionary values (%d) should not be less than 0", dictValue)
		}
		xio.WriteInt(uint64(dictValue), int(bytesPerDictionaryValue), enc.buf)
		if idx > 0 {
			start += int(bytesPerDictionaryValue)
		}
		copy(enc.dictionaryProtoData[start:start+int(bytesPerDictionaryValue)], enc.buf[:bytesPerDictionaryValue])
	}
	// Only store a slice up to dictionaryProtoSize.
	enc.dictionaryProto.Data = enc.dictionaryProtoData[:dictionaryProtoSize]

	if err := proto.EncodeIntDictionary(&enc.dictionaryProto, &enc.buf, writer); err != nil {
		return err
	}

	for valuesIt.Next() {
		encodedValue := uint64(dict[valuesIt.Current()])
		if err := enc.bitWriter.WriteBits(encodedValue, int(bitsPerEncodedValue)); err != nil {
			return err
		}
	}
	return valuesIt.Err()
}
