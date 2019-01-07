package encoding

import (
	"fmt"
	"io"
	"math/bits"
	"time"

	"github.com/xichen2020/eventdb/generated/proto/encodingpb"
	"github.com/xichen2020/eventdb/values"
	"github.com/xichen2020/eventdb/values/iterator"
	"github.com/xichen2020/eventdb/x/convert"
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
	Encode(timeVals values.TimeValues, writer io.Writer, opts EncodeTimeOptions) error
}

// timeEncoder is a int encoder.
type timeEncoder struct {
	bitWriter *bitstream.BitWriter
	buf       []byte
}

// NewTimeEncoder creates a new time encoder.
func NewTimeEncoder() TimeEncoder {
	return &timeEncoder{
		bitWriter: bitstream.NewWriter(nil),
	}
}

// Encode encodes a collection of time values and writes the encoded bytes to the writer.
func (enc *timeEncoder) Encode(
	timeVals values.TimeValues,
	writer io.Writer,
	opts EncodeTimeOptions,
) error {
	// Reset the internal state at the beginning of every `Encode` call.
	enc.reset(writer)

	resType, err := durationToProtoResolution(opts.Resolution)
	if err != nil {
		return err
	}

	var (
		valuesMeta      = timeVals.Metadata()
		resolutionNanos = opts.Resolution.Nanoseconds()
		normalizedMax   = valuesMeta.Max / resolutionNanos
		normalizedMin   = valuesMeta.Min / resolutionNanos
	)
	metaProto := encodingpb.TimeMeta{
		Encoding:            encodingpb.EncodingType_DELTA,
		Resolution:          resType,
		BitsPerEncodedValue: int64(bits.Len(uint(normalizedMax-normalizedMin)) + 1), // Add 1 for the sign bit
		NumValues:           int32(valuesMeta.Size),
		MinValue:            valuesMeta.Min,
		MaxValue:            valuesMeta.Max,
	}
	if err = proto.EncodeTimeMeta(&metaProto, &enc.buf, writer); err != nil {
		return err
	}

	valuesIt, err := timeVals.Iter()
	if err != nil {
		return err
	}
	defer valuesIt.Close()

	// Only delta encoding for now.
	return deltaTimeEncode(
		iterator.NewScaledTimeIterator(valuesIt, opts.Resolution, convert.ScaleDownTimeFn),
		enc.bitWriter,
		int(metaProto.BitsPerEncodedValue),
		convert.Int64SubInt64Fn,
		convert.Int64AsUint64Fn,
	)
}

func (enc *timeEncoder) reset(writer io.Writer) {
	enc.bitWriter.Reset(writer)
}

func durationToProtoResolution(resolution time.Duration) (encodingpb.ResolutionType, error) {
	switch resolution {
	case time.Nanosecond:
		return encodingpb.ResolutionType_NANOSECOND, nil
	case time.Microsecond:
		return encodingpb.ResolutionType_MICROSECOND, nil
	case time.Millisecond:
		return encodingpb.ResolutionType_MILLISECOND, nil
	case time.Second:
		return encodingpb.ResolutionType_SECOND, nil
	default:
		return encodingpb.ResolutionType_UNKNOWN_RESOLUTION, fmt.Errorf("resolution (%v) is not a valid resolution", resolution)
	}
}
