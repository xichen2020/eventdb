package encoding

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"

	"github.com/xichen2020/eventdb/generated/proto/encodingpb"
	xio "github.com/xichen2020/eventdb/x/io"
	"github.com/xichen2020/eventdb/x/proto"
	"github.com/xichen2020/eventdb/x/unsafe"

	"github.com/valyala/gozstd"
)

const (
	// Default initial size of data slice.
	defaultInitialDataSliceSize = 1 << 8
	// Default max size in bytes per block.
	defaultMaxBytesPerBlock = 32 * (1 << 20) // 32 MB
)

// Maximum cardinality allowed for dictionary encoding.
var (
	dictEncodingMaxCardinalityString = 1 << 16
)

// StringEncoderOptions informs the encoding process.
type StringEncoderOptions struct {
	UseBlocks        bool
	MaxBytesPerBlock int
}

// StringEncoder encodes string values.
type StringEncoder interface {
	// Encode encodes a collection of strings and writes the encoded bytes to the writer.
	Encode(writer io.Writer, valuesIt RewindableStringIterator) error
}

// StringEnc is a string encoder.
type StringEnc struct {
	// blockWriter is used for writing data to a file in size delimited blocks.
	bw *blockWriter
	// buf is used for all other uses.
	buf             []byte
	metaProto       encodingpb.StringMeta
	blockMetaProto  encodingpb.BlockMeta
	dictionaryProto encodingpb.StringArray
	data            []string
	numBytesWritten int
}

// NewStringEncoder creates a new string encoder.
func NewStringEncoder() *StringEnc {
	enc := &StringEnc{
		// Make at least enough room for the binary Varint methods not to panic.
		buf:  make([]byte, binary.MaxVarintLen64),
		data: make([]string, 0, defaultInitialDataSliceSize),
	}
	enc.bw = newBlockWriter(&enc.buf, &enc.blockMetaProto)
	return enc
}

// Encode encodes a collection of strings and writes the encoded bytes to the writer.
func (enc *StringEnc) Encode(
	writer io.Writer,
	valuesIt RewindableStringIterator,
	opts *StringEncoderOptions,
) error {
	if opts == nil {
		// Apply default options.
		opts = &StringEncoderOptions{
			UseBlocks:        false,
			MaxBytesPerBlock: defaultMaxBytesPerBlock,
		}
	}

	// Reset internal state at the beginning of every `Encode` call.
	enc.reset(writer)

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

	// Always ZSTD compression for now.
	enc.metaProto.Compression = encodingpb.CompressionType_ZSTD
	enc.metaProto.UseBlocks = opts.UseBlocks

	if err := proto.EncodeStringMeta(&enc.metaProto, &enc.buf, writer); err != nil {
		return err
	}

	// Use the block writer if configured to do so.
	if enc.metaProto.UseBlocks {
		writer = enc.bw
		defer enc.bw.flush()
	}

	// Compress the bytes.
	var fw xio.FlushableWriter
	switch enc.metaProto.Compression {
	case encodingpb.CompressionType_ZSTD:
		// TODO(bodu): Figure out a cleaner way to do this.
		compressWriter := gozstd.NewWriter(writer)
		defer compressWriter.Close()
		fw = compressWriter
	default:
		return fmt.Errorf("invalid compression type: %v", enc.metaProto.Compression)
	}

	switch enc.metaProto.Encoding {
	case encodingpb.EncodingType_RAW_SIZE:
		return enc.encodeLength(fw, valuesIt, opts.MaxBytesPerBlock)
	case encodingpb.EncodingType_DICTIONARY:
		return enc.encodeDictionary(fw, valuesIt, dictionary, enc.data[:idx])
	default:
		return fmt.Errorf("invalid encoding type: %v", enc.metaProto.Encoding)
	}
}

// Reset the string encoder between `Encode` calls.
func (enc *StringEnc) reset(writer io.Writer) {
	enc.metaProto.Reset()
	enc.dictionaryProto.Reset()
	enc.data = enc.data[:0]
	enc.bw.reset(writer)
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
		enc.blockMetaProto.NumOfEvents++
	}
	return valuesIt.Err()
}

// Length encoding strategy is to write the number of bytes as a VInt
// and then the bytes for the string.
func (enc *StringEnc) encodeLength(
	writer xio.FlushableWriter,
	valuesIt RewindableStringIterator,
	maxBytesPerBlock int,
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

		enc.blockMetaProto.NumOfEvents++
		if enc.metaProto.UseBlocks {
			enc.numBytesWritten += n + len(b)
			if enc.numBytesWritten > maxBytesPerBlock {
				if err := writer.Flush(); err != nil {
					return err
				}
				if err := enc.bw.flush(); err != nil {
					return err
				}
				enc.numBytesWritten = 0
			}
		}
	}
	return valuesIt.Err()
}
