package encoding

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"

	"github.com/xichen2020/eventdb/generated/proto/encodingpb"
	"github.com/xichen2020/eventdb/values"
	"github.com/xichen2020/eventdb/values/iterator"
	"github.com/xichen2020/eventdb/x/hashmap"
	"github.com/xichen2020/eventdb/x/proto"

	xerrors "github.com/m3db/m3x/errors"
	"github.com/valyala/gozstd"
)

const (
	// In order for bytes dictionary encoding to be eligible, the max cardinality of the values
	// collection must be no more than `num_values * bytesDictEncodingMaxCardinalityPercent`.
	bytesDictEncodingMaxCardinalityPercent = 1.0 / 64
)

var (
	errValueNotFoundInValueDict = errors.New("value not found in value dict")
)

// BytesEncoder encodes bytes values.
type BytesEncoder interface {
	// Encode encodes a collection of bytes and writes the encoded bytes to the writer.
	Encode(strVals values.BytesValues, writer io.Writer) error
}

// bytesEncoder is a bytes encoder.
type bytesEncoder struct {
	buf             []byte
	dictionaryProto encodingpb.BytesArray
}

// NewBytesEncoder creates a new bytes encoder.
func NewBytesEncoder() BytesEncoder {
	return &bytesEncoder{
		buf: make([]byte, binary.MaxVarintLen64), // Big enough to write a varint
	}
}

// Encode encodes a collection of bytess and writes the encoded bytes to the writer.
func (enc *bytesEncoder) Encode(strVals values.BytesValues, writer io.Writer) error {
	// Reset internal state at the beginning of every `Encode` call.
	enc.reset()

	// TODO(bodu): Do some perf benchmarking to see whether we want to allocate a new map
	// or clear an existing one.
	valueMeta := strVals.Metadata()
	maxCardinalityAllowed := int(math.Ceil(1.0 * float64(valueMeta.Size) * bytesDictEncodingMaxCardinalityPercent))
	dictionary := hashmap.NewBytesIntHashMap(hashmap.BytesIntHashMapOptions{
		InitialSize: maxCardinalityAllowed,
	})
	valuesIt, err := strVals.Iter()
	if err != nil {
		return err
	}
	idx := 0
	for valuesIt.Next() {
		curr := valuesIt.Current()
		if _, ok := dictionary.Get(curr.Bytes()); ok {
			continue
		}
		dictionary.Set(curr.SafeBytes(), idx)
		idx++
		if dictionary.Len() > maxCardinalityAllowed {
			break
		}
	}
	if err = valuesIt.Err(); err != nil {
		valuesIt.Close()
		return err
	}
	valuesIt.Close()

	// Always ZSTD compression for now.
	metaProto := encodingpb.BytesMeta{
		Compression: encodingpb.CompressionType_ZSTD,
		NumValues:   int32(valueMeta.Size),
		MinValue:    valueMeta.Min,
		MaxValue:    valueMeta.Max,
	}
	if dictionary.Len() > maxCardinalityAllowed {
		metaProto.Encoding = encodingpb.EncodingType_RAW_SIZE
	} else {
		metaProto.Encoding = encodingpb.EncodingType_DICTIONARY
	}
	if err = proto.EncodeBytesMeta(&metaProto, &enc.buf, writer); err != nil {
		return err
	}

	// Compress the bytes.
	var compressWriter *gozstd.Writer
	switch metaProto.Compression {
	case encodingpb.CompressionType_ZSTD:
		compressWriter = gozstd.NewWriter(writer)
		// Release all resources occupied by compressWriter.
		defer compressWriter.Release()
		writer = compressWriter
	default:
		return fmt.Errorf("invalid compression type: %v", metaProto.Compression)
	}

	valuesIt, err = strVals.Iter()
	if err != nil {
		return err
	}
	defer valuesIt.Close()

	var multiErr xerrors.MultiError
	switch metaProto.Encoding {
	case encodingpb.EncodingType_RAW_SIZE:
		err = enc.rawSizeEncode(valuesIt, writer)
	case encodingpb.EncodingType_DICTIONARY:
		err = enc.dictionaryEncode(valuesIt, dictionary, writer)
	default:
		err = fmt.Errorf("invalid encoding type: %v", metaProto.Encoding)
	}
	multiErr = multiErr.Add(err)

	// Close the compressWriter if its present.
	if compressWriter != nil {
		// NB(xichen): Close flushes and closes the compressed writer but doesn't
		// close the writer wrapped by the compressed writer.
		multiErr = multiErr.Add(compressWriter.Close())
	}

	return multiErr.FinalError()
}

func (enc *bytesEncoder) reset() {
	enc.dictionaryProto.Data = enc.dictionaryProto.Data[:0]
}

// Dictionary encoding strategy is to write all unique bytess into an array
// and then use the array idx to represent the bytes value.
// TODO(xichen): The dictionary values should be sorted to speed up lookup during query execution.
func (enc *bytesEncoder) dictionaryEncode(
	valuesIt iterator.ForwardBytesIterator,
	dictionary *hashmap.BytesIntHashMap,
	writer io.Writer,
) error {
	// Write out the dictionary data first (to be used for decoding).
	if cap(enc.dictionaryProto.Data) >= dictionary.Len() {
		enc.dictionaryProto.Data = enc.dictionaryProto.Data[:dictionary.Len()]
	} else {
		enc.dictionaryProto.Data = make([][]byte, dictionary.Len())
	}
	for _, entry := range dictionary.Iter() {
		enc.dictionaryProto.Data[entry.Value()] = entry.Key()
	}

	if err := proto.EncodeBytesArray(&enc.dictionaryProto, &enc.buf, writer); err != nil {
		return err
	}

	// Write out the dictionary values.
	for valuesIt.Next() {
		idx, ok := dictionary.Get(valuesIt.Current().Bytes())
		// NB(bodu): This should not happen but perform a sanity check anyways.
		if !ok {
			return errValueNotFoundInValueDict
		}
		n := binary.PutVarint(enc.buf, int64(idx))
		if _, err := writer.Write(enc.buf[:n]); err != nil {
			return err
		}
	}
	return valuesIt.Err()
}

// Raw size encoding strategy is to write the number of bytes as a varint
// and then the bytes for the bytes.
// TODO(xichen): Encode the bytes lengths using int encoder.
func (enc *bytesEncoder) rawSizeEncode(
	valuesIt iterator.ForwardBytesIterator,
	writer io.Writer,
) error {
	for valuesIt.Next() {
		b := valuesIt.Current().Bytes()
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
