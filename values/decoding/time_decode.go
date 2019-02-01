package decoding

import (
	"bytes"
	"errors"
	"fmt"
	"time"

	"github.com/xichen2020/eventdb/generated/proto/encodingpb"
	"github.com/xichen2020/eventdb/values"
	"github.com/xichen2020/eventdb/values/iterator"
	iterimpl "github.com/xichen2020/eventdb/values/iterator/impl"
	"github.com/xichen2020/eventdb/x/convert"
	xproto "github.com/xichen2020/eventdb/x/proto"
)

type scaleMode int

const (
	noScale scaleMode = iota
	withScale
)

var (
	errInvalidScaleMode = errors.New("invalid scale mode")
)

// TimeDecoder decodes time values.
type TimeDecoder interface {
	// DecodeRaw decodes raw encoded bytes into a closeable time values.
	DecodeRaw(data []byte) (values.CloseableTimeValues, error)
}

// timeDecoder is a time decoder.
type timeDecoder struct {
	buf []byte
}

// NewTimeDecoder creates a new time decoder.
func NewTimeDecoder() TimeDecoder {
	return &timeDecoder{
		// Make buf at least big enough to hold Uint64 values.
		buf: make([]byte, uint64SizeBytes),
	}
}

// Decode decodes time data in a streaming fashion.
func (dec *timeDecoder) DecodeRaw(data []byte) (values.CloseableTimeValues, error) {
	var metaProto encodingpb.TimeMeta
	bytesRead, err := xproto.DecodeTimeMetaRaw(data, &metaProto)
	if err != nil {
		return nil, err
	}
	return newFsBasedTimeValues(metaProto, data[bytesRead:]), nil
}

func newTimeIteratorFromMeta(
	metaProto encodingpb.TimeMeta,
	encodedBytes []byte,
	scaleMode scaleMode,
) (iterator.ForwardTimeIterator, error) {
	resolution, err := protoResolutionToDuration(metaProto.Resolution)
	if err != nil {
		return nil, err
	}

	switch metaProto.Encoding {
	case encodingpb.EncodingType_DELTA:
		break
	default:
		return nil, fmt.Errorf("invalid encoding type: %v", metaProto.Encoding)
	}

	reader := bytes.NewReader(encodedBytes)
	deltaIter := newDeltaTimeIterator(reader, metaProto.BitsPerEncodedValue, convert.Int64AddIntFn)
	switch scaleMode {
	case noScale:
		return deltaIter, nil
	case withScale:
		return iterimpl.NewScaledTimeIterator(deltaIter, resolution, convert.ScaleUpTimeFn), nil
	default:
		return nil, errInvalidScaleMode
	}
}

func protoResolutionToDuration(resType encodingpb.ResolutionType) (time.Duration, error) {
	switch resType {
	case encodingpb.ResolutionType_NANOSECOND:
		return time.Nanosecond, nil
	case encodingpb.ResolutionType_MICROSECOND:
		return time.Microsecond, nil
	case encodingpb.ResolutionType_MILLISECOND:
		return time.Millisecond, nil
	case encodingpb.ResolutionType_SECOND:
		return time.Second, nil
	default:
		return 0, fmt.Errorf("resolution (%v) is not a valid resolution", resType)
	}
}
