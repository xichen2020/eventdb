package encoding

import (
	"fmt"
	"time"

	"github.com/xichen2020/eventdb/generated/proto/encodingpb"
	"github.com/xichen2020/eventdb/x/io"
	"github.com/xichen2020/eventdb/x/proto"

	bitstream "github.com/dgryski/go-bitstream"
)

// TimeDecoder decodes time values.
type TimeDecoder interface {
	// Decode decodes times from reader.
	Decode(reader io.Reader) (ForwardTimeIterator, error)
}

// TimeDec is a time Decoder.
type TimeDec struct {
	bitReader *bitstream.BitReader
	metaProto encodingpb.TimeMeta
	buf       []byte
}

// NewTimeDecoder creates a new time Decoder.
func NewTimeDecoder() *TimeDec {
	return &TimeDec{
		// Make buf at least big enough to hold Utime64 values.
		buf: make([]byte, uint64SizeBytes),
		// Make a bitWriter w/ a nil buffer that will be re-used for every `Decode` call.
		bitReader: bitstream.NewReader(nil),
	}
}

// Decode encoded time data in a streaming fashion.
func (dec *TimeDec) Decode(reader io.Reader) (ForwardTimeIterator, error) {
	// Reset internal state at the beginning of every `Decode` call.
	dec.reset(reader)

	// Decode metadata first.
	if err := proto.DecodeTimeMeta(&dec.metaProto, &dec.buf, reader); err != nil {
		return nil, err
	}

	switch dec.metaProto.Encoding {
	case encodingpb.EncodingType_DELTA:
		return dec.decodeDelta()
	default:
		return nil, fmt.Errorf("Invalid encoding type: %v", dec.metaProto.Encoding)
	}
}

// Reset the time decoder.
func (dec *TimeDec) reset(reader io.Reader) {
	// Reset the BitReader at the start of each `Decode` call.
	dec.bitReader.Reset(reader)
	dec.metaProto.Reset()
}

func (dec *TimeDec) decodeDelta() (*scaledTimeIterator, error) {
	deltaIter := newDeltaTimeIterator(
		dec.bitReader,
		dec.metaProto.BitsPerEncodedValue,
		int64SubIntFn,
		int64AddIntFn,
	)
	var resolution time.Duration
	switch dec.metaProto.Resolution {
	case encodingpb.ResolutionType_NANOSECOND:
		resolution = time.Nanosecond
	case encodingpb.ResolutionType_MICROSECOND:
		resolution = time.Microsecond
	case encodingpb.ResolutionType_MILLISECOND:
		resolution = time.Millisecond
	case encodingpb.ResolutionType_SECOND:
		resolution = time.Second
	default:
		return nil, fmt.Errorf("resolution (%v) is not a valid resolution", resolution)
	}
	return newScaledTimeIterator(resolution, deltaIter, scaleUpFn), nil
}
