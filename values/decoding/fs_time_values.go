package decoding

import (
	"github.com/xichen2020/eventdb/generated/proto/encodingpb"
	"github.com/xichen2020/eventdb/values"
	"github.com/xichen2020/eventdb/values/iterator"
)

// fsBasedTimeValues is a time values collection backed by encoded data on the filesystem.
type fsBasedTimeValues struct {
	metaProto     encodingpb.TimeMeta
	encodedValues []byte

	closed bool
}

// newFsBasedTimeValues creates a new fs based bool values.
func newFsBasedTimeValues(
	metaProto encodingpb.TimeMeta,
	encodedValues []byte, // Encoded values not including bool meta
) values.CloseableTimeValues {
	return &fsBasedTimeValues{
		metaProto:     metaProto,
		encodedValues: encodedValues,
	}
}

func (v *fsBasedTimeValues) Metadata() values.TimeValuesMetadata {
	return values.TimeValuesMetadata{
		Size: int(v.metaProto.NumValues),
		Min:  v.metaProto.MinValue,
		Max:  v.metaProto.MaxValue,
	}
}

func (v *fsBasedTimeValues) Iter() (iterator.ForwardTimeIterator, error) {
	return newTimeIteratorFromMeta(v.metaProto, v.encodedValues)
}

func (v *fsBasedTimeValues) Close() {
	if v.closed {
		return
	}
	v.closed = true
	v.encodedValues = nil
}
