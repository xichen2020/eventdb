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
	resolution time.Duration,
) error {
	// Reset the internal state at the beginning of every `Encode` call.
	enc.reset(writer)

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
		return fmt.Errorf("resolution (%v) is not a valid resolution", resolution)
	}

	var (
		firstVal int64
		max      = int64(math.MinInt64)
		min      = int64(math.MaxInt64)
	)
	for idx := 0; valuesIt.Next(); idx++ {
		curr := valuesIt.Current() / int64(resolution)

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
	enc.metaProto.BitsPerEncodedValue = int64(bits.Len(uint((max - min) + 1))) // Add 1 for the sign bit.

	if err := proto.EncodeTimeMeta(&enc.metaProto, &enc.buf, writer); err != nil {
		return err
	}

	// Only delta encoding for now.
	return encodeDeltaTime(enc.bitWriter, enc.metaProto.BitsPerEncodedValue, newscaledDownTimeIterator(resolution, valuesIt), int64SubInt64Fn)
}

func (enc *TimeEnc) reset(writer io.Writer) {
	enc.bitWriter.Reset(writer)
	enc.metaProto.Reset()
}

// scaledDownTimeIterator scales down the time values to the specified resolution.
type scaledDownTimeIterator struct {
	resolution time.Duration
	valuesIt   RewindableTimeIterator
	err        error
	curr       int64
	closed     bool
}

func newscaledDownTimeIterator(
	resolution time.Duration,
	valuesIt RewindableTimeIterator,
) *scaledDownTimeIterator {
	return &scaledDownTimeIterator{
		resolution: resolution,
		valuesIt:   valuesIt,
	}
}

// Next iteration.
func (it *scaledDownTimeIterator) Next() bool {
	if it.closed || it.err != nil {
		return false
	}
	if !it.valuesIt.Next() {
		it.err = it.valuesIt.Err()
		return false
	}
	// Scale down the current value to the specified resolution.
	it.curr = it.valuesIt.Current() / int64(it.resolution)
	return true
}

// Current returns the current int64.
func (it *scaledDownTimeIterator) Current() int64 { return it.curr }

// Err returns any error recorded while iterating.
func (it *scaledDownTimeIterator) Err() error { return it.err }

// Close the iterator.
func (it *scaledDownTimeIterator) Close() error {
	it.closed = true
	it.err = nil
	valuesIt := it.valuesIt
	it.valuesIt = nil
	return valuesIt.Close()
}
