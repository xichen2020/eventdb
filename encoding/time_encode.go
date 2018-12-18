package encoding

import (
	"fmt"
	"io"
	"math"
	"math/bits"
	"time"

	bitstream "github.com/dgryski/go-bitstream"
	"github.com/xichen2020/eventdb/generated/proto/encodingpb"
	"github.com/xichen2020/eventdb/x/proto"
)

// TimeEncoder encodes times values.
type TimeEncoder interface {
	// Encode encodes a collection of time values and writes the encoded bytes to the writer.
	// Callers should explicitly call `Reset` before subsequent call to `Encode`.
	Encode(writer io.Writer, values RewindableTimeIterator) error
}

// TimeEnc is a int encoder.
type TimeEnc struct {
	metaProto  encodingpb.TimeMeta
	resolution int64
	resType    encodingpb.ResolutionType
	bitWriter  *bitstream.BitWriter
	buf        []byte
}

// NewTimeEncoder creates a new time encoder.
func NewTimeEncoder(resolution time.Duration) (*TimeEnc, error) {
	var resType encodingpb.ResolutionType
	switch resolution {
	case time.Nanosecond:
		resType = encodingpb.ResolutionType_NANOSECOND
	case time.Microsecond:
		resType = encodingpb.ResolutionType_MICROSECOND
	case time.Millisecond:
		resType = encodingpb.ResolutionType_MILLISECOND
	case time.Second:
		resType = encodingpb.ResolutionType_SECOND
	default:
		return nil, fmt.Errorf("resolution (%v) is not a valid resolution", resolution)
	}
	return &TimeEnc{
		resolution: int64(resolution),
		resType:    resType,
		bitWriter:  bitstream.NewWriter(nil),
	}, nil
}

// Encode encodes a collection of time values and writes the encoded bytes to the writer.
func (enc *TimeEnc) Encode(
	writer io.Writer,
	valuesIt RewindableTimeIterator,
) error {
	// Reset the internal state at the beginning of every `Encode` call.
	enc.reset(writer)

	var (
		firstVal int64
		max      = int64(math.MinInt64)
		min      = int64(math.MaxInt64)
	)
	for idx := 0; valuesIt.Next(); idx++ {
		curr := valuesIt.Current()
		if idx == 0 {
			firstVal = curr
		}
		if curr < min {
			min = curr
		}
		if curr > max {
			max = curr
		}
	}
	if err := valuesIt.Err(); err != nil {
		return err
	}
	valuesIt.Rewind()

	enc.metaProto.Encoding = encodingpb.EncodingType_DELTA
	enc.metaProto.Resolution = enc.resType
	enc.metaProto.DeltaStart = firstVal
	enc.metaProto.BitsPerEncodedValue = int64(bits.Len(uint((max - min) + 1))) // Add 1 for the sign bit.

	if err := proto.EncodeTimeMeta(&enc.metaProto, &enc.buf, writer); err != nil {
		return err
	}

	// Only delta encoding for now.
	return encodeDeltaTime(enc.bitWriter, enc.metaProto.BitsPerEncodedValue, valuesIt, int64SubInt64Fn)
}

func (enc *TimeEnc) reset(writer io.Writer) {
	enc.bitWriter.Reset(writer)
	enc.metaProto.Reset()
}
