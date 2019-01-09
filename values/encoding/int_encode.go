package encoding

import (
	"fmt"
	"io"
	"math"
	"math/bits"

	"github.com/xichen2020/eventdb/generated/proto/encodingpb"
	"github.com/xichen2020/eventdb/values"
	"github.com/xichen2020/eventdb/values/iterator"
	"github.com/xichen2020/eventdb/x/convert"
	xio "github.com/xichen2020/eventdb/x/io"
	"github.com/xichen2020/eventdb/x/proto"

	bitstream "github.com/dgryski/go-bitstream"
)

const (
	uint64SizeBytes = 8

	// In order for int dictionary encoding to be eligible, the max cardinality of the values
	// collection must be no more than `num_values * intDictEncodingMaxCardinalityPercent`.
	intDictEncodingMaxCardinalityPercent = 1.0 / 256
)

// IntEncoder encodes int values.
type IntEncoder interface {
	// Encode encodes a collection of ints and writes the encoded bytes to the writer.
	Encode(intVals values.IntValues, writer io.Writer) error
}

// intEncoder is a int encoder.
// TODO(xichen): Support encoding the ints as is.
type intEncoder struct {
	buf             []byte
	dictionaryProto encodingpb.IntDictionary
	sortedDict      []int
	bitWriter       *bitstream.BitWriter
}

// NewIntEncoder creates a new int encoder.
func NewIntEncoder() IntEncoder {
	return &intEncoder{
		// Make buf at least big enough to hold Uint64 values.
		buf: make([]byte, uint64SizeBytes),
		// Create a bitWriter w/ a nil write buffer that will be re-used for every Encode call.
		bitWriter: bitstream.NewWriter(nil),
	}
}

// Encode encodes a collection of ints and writes the encoded bytes to the writer.
func (enc *intEncoder) Encode(intVals values.IntValues, writer io.Writer) error {
	// Reset internal state at the beginning of every `Encode` call.
	enc.reset(writer)

	// Determine whether we want to do dictionary compression.
	var (
		valueMeta             = intVals.Metadata()
		maxCardinalityAllowed = int(math.Ceil(1.0 * float64(valueMeta.Size) * intDictEncodingMaxCardinalityPercent))
		dictionary            = make(map[int]int, maxCardinalityAllowed)
		idx                   int
	)
	valuesIt, err := intVals.Iter()
	if err != nil {
		return err
	}
	for valuesIt.Next() {
		curr := valuesIt.Current()
		if _, ok := dictionary[curr]; ok {
			continue
		}
		dictionary[curr] = idx
		idx++
		if len(dictionary) > maxCardinalityAllowed {
			break
		}
	}
	if err = valuesIt.Err(); err != nil {
		valuesIt.Close()
		return err
	}
	valuesIt.Close()

	// TODO(xichen): Estimate how many bytes we are going to write out and compare that
	// against the bytes needed if we simply write out the ints unchanged.
	metaProto := encodingpb.IntMeta{
		NumValues: int32(valueMeta.Size),
		MinValue:  int64(valueMeta.Min),
		MaxValue:  int64(valueMeta.Max),
	}
	if len(dictionary) <= maxCardinalityAllowed {
		metaProto.Encoding = encodingpb.EncodingType_DICTIONARY
		// Min number of bytes to encode each value into the int dictionary.
		metaProto.BytesPerDictionaryValue = int64(math.Ceil(float64(bits.Len(uint(valueMeta.Max-valueMeta.Min))) / 8.0))
		// Min number of bits required to encode dictionary indices. If there is only one value, use 1 bit.
		metaProto.BitsPerEncodedValue = int64(math.Max(float64(bits.Len(uint(len(dictionary)-1))), 1))
	} else {
		metaProto.Encoding = encodingpb.EncodingType_DELTA
		// Add 1 for the sign bit.
		metaProto.BitsPerEncodedValue = int64(bits.Len(uint(valueMeta.Max-valueMeta.Min)) + 1)
	}

	if err = proto.EncodeIntMeta(&metaProto, &enc.buf, writer); err != nil {
		return err
	}

	valuesIt, err = intVals.Iter()
	if err != nil {
		return err
	}
	defer valuesIt.Close()

	switch metaProto.Encoding {
	case encodingpb.EncodingType_DICTIONARY:
		if err := enc.dictionaryEncode(
			valuesIt,
			dictionary,
			int(metaProto.MinValue),
			int(metaProto.BytesPerDictionaryValue),
			int(metaProto.BitsPerEncodedValue),
			writer,
		); err != nil {
			return err
		}
	case encodingpb.EncodingType_DELTA:
		if err := deltaIntEncode(
			valuesIt,
			enc.bitWriter,
			int(metaProto.BitsPerEncodedValue),
			convert.IntSubIntFn,
			convert.IntAsUint64Fn,
		); err != nil {
			return err
		}
	}

	return nil
}

// Reset resets the encoder.
func (enc *intEncoder) reset(writer io.Writer) {
	// Reset the bitWriter at the start of every `Encode` call.
	enc.bitWriter.Reset(writer)
	enc.dictionaryProto.Data = enc.dictionaryProto.Data[:0]
}

// TODO(xichen): The dictionary values should be sorted to speed up lookup during query execution.
func (enc *intEncoder) dictionaryEncode(
	valuesIt iterator.ForwardIntIterator,
	dictionary map[int]int,
	minValue int,
	bytesPerDictionaryValue int,
	bitsPerEncodedValue int,
	writer io.Writer,
) error {
	if cap(enc.sortedDict) >= len(dictionary) {
		enc.sortedDict = enc.sortedDict[:len(dictionary)]
	} else {
		enc.sortedDict = make([]int, len(dictionary))
	}
	for value, idx := range dictionary {
		enc.sortedDict[idx] = value
	}

	// Ensure we reserve enough space.
	dictionaryProtoSize := len(enc.sortedDict) * bytesPerDictionaryValue
	if cap(enc.dictionaryProto.Data) >= dictionaryProtoSize {
		enc.dictionaryProto.Data = enc.dictionaryProto.Data[:dictionaryProtoSize]
	} else {
		enc.dictionaryProto.Data = make([]byte, dictionaryProtoSize)
	}

	currIdx := 0
	for _, value := range enc.sortedDict {
		// The dictionary value is encoded as a positive number to be added to the minimum value.
		dictValue := value - minValue
		// Sanity check.
		if dictValue < 0 {
			return fmt.Errorf("dictionary values %d should not be less than min value %d", value, minValue)
		}
		xio.WriteInt(uint64(dictValue), bytesPerDictionaryValue, enc.dictionaryProto.Data[currIdx:])
		currIdx += bytesPerDictionaryValue
	}

	if err := proto.EncodeIntDictionary(&enc.dictionaryProto, &enc.buf, writer); err != nil {
		return err
	}

	for valuesIt.Next() {
		encodedValue := dictionary[valuesIt.Current()]
		if err := enc.bitWriter.WriteBits(uint64(encodedValue), bitsPerEncodedValue); err != nil {
			return err
		}
	}

	if err := valuesIt.Err(); err != nil {
		return err
	}

	// Flush the bit writer and pad with zero bits if necessary.
	return enc.bitWriter.Flush(bitstream.Zero)
}
