package encoding

import (
	"fmt"
	"io"
	"math"
	"math/bits"
	"time"

	"github.com/xichen2020/eventdb/generated/proto/encodingpb"
	"github.com/xichen2020/eventdb/x/proto"

	bitstream "github.com/dgryski/go-bitstream"
)

// EncodeTimeOptions informs time encoding operation.
type EncodeTimeOptions struct {
	Resolution time.Duration
}

// TimeEncoder encodes times values.
type TimeEncoder interface {
	// Encode encodes a collection of time values and writes the encoded bytes to the writer.
	// Callers should explicitly call `Reset` before subsequent call to `Encode`.
	Encode(writer io.Writer, values RewindableTimeIterator) error
}

// TimeEnc is a int encoder.
type TimeEnc struct {
	metaProto encodingpb.TimeMeta
	bitWriter *bitstream.BitWriter
	buf       []byte
}

// NewTimeEncoder creates a new time encoder.
func NewTimeEncoder() *TimeEnc {
	return &TimeEnc{
		bitWriter: bitstream.NewWriter(nil),
	}
}

// Encode encodes a collection of time values and writes the encoded bytes to the writer.
func (enc *TimeEnc) Encode(
	writer io.Writer,
	valuesIt RewindableTimeIterator,
	opts EncodeTimeOptions,
) error {
	// Reset the internal state at the beginning of every `Encode` call.
	enc.reset(writer)

	var resType encodingpb.ResolutionType
	switch opts.Resolution {
	case time.Nanosecond:
		resType = encodingpb.ResolutionType_NANOSECOND
	case time.Microsecond:
		resType = encodingpb.ResolutionType_MICROSECOND
	case time.Millisecond:
		resType = encodingpb.ResolutionType_MILLISECOND
	case time.Second:
		resType = encodingpb.ResolutionType_SECOND
	default:
		return fmt.Errorf("resolution (%v) is not a valid resolution", opts.Resolution)
	}

	var (
		firstVal   int64
		max        = int64(math.MinInt64)
		min        = int64(math.MaxInt64)
		resolution = int64(opts.Resolution)
	)
	for idx := 0; valuesIt.Next(); idx++ {
		curr := valuesIt.Current() / resolution

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
	enc.metaProto.Resolution = resType
	enc.metaProto.DeltaStart = firstVal
	enc.metaProto.BitsPerEncodedValue = int64(bits.Len(uint(max - min)))

	if err := proto.EncodeTimeMeta(&enc.metaProto, &enc.buf, writer); err != nil {
		return err
	}

	// Only delta encoding for now.
	return encodeDeltaTime(enc.bitWriter, enc.metaProto.BitsPerEncodedValue, newScaledTimeIterator(opts.Resolution, valuesIt, enc.scaleDownFn), int64SubInt64Fn)
}

func (enc *TimeEnc) reset(writer io.Writer) {
	enc.bitWriter.Reset(writer)
	enc.metaProto.Reset()
}

func (enc *TimeEnc) scaleDownFn(v int64, resolution time.Duration) int64 {
	return v / int64(resolution)
}
