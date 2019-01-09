package decoding

import (
	"github.com/xichen2020/eventdb/document/field"
	"github.com/xichen2020/eventdb/filter"
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

// TODO(xichen): Filter implementation should take advantage of the metadata
// to do more intelligent filtering, e.g., checking if the value is within the
// value range, and translate the filtering operation against the time values
// into filtering operation against the underlying int values to take advantage
// of a more optimized int value filter implementation.
func (v *fsBasedTimeValues) Filter(
	op filter.Op,
	filterValue *field.ValueUnion,
) (iterator.PositionIterator, error) {
	return defaultFilteredFsBasedTimeValueIterator(v, op, filterValue)
}

func (v *fsBasedTimeValues) Close() {
	if v.closed {
		return
	}
	v.closed = true
	v.encodedValues = nil
}
