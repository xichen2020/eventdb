package decoding

import (
	"github.com/xichen2020/eventdb/document/field"
	"github.com/xichen2020/eventdb/filter"
	"github.com/xichen2020/eventdb/generated/proto/encodingpb"
	"github.com/xichen2020/eventdb/values"
	"github.com/xichen2020/eventdb/values/iterator"
	"github.com/xichen2020/eventdb/values/iterator/impl"
	"github.com/xichen2020/eventdb/x/convert"
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
	return newTimeIteratorFromMeta(v.metaProto, v.encodedValues, withScale)
}

func (v *fsBasedTimeValues) Filter(
	op filter.Op,
	filterValue *field.ValueUnion,
) (iterator.PositionIterator, error) {
	if filterValue == nil {
		return nil, errNilFilterValue
	}
	if filterValue.Type != field.TimeType {
		return nil, errUnexpectedFilterValueType
	}
	if !op.TimeMaybeInRange(v.metaProto.MinValue, v.metaProto.MaxValue, filterValue.TimeNanosVal) {
		return impl.NewEmptyPositionIterator(), nil
	}
	// NB(wjang): newTimeIteratorFromMeta below also calls protoResolutionToDuration. The overhead shouldn't
	// be too bad compared to the time spent iterating.
	resolution, err := protoResolutionToDuration(v.metaProto.Resolution)
	if err != nil {
		return nil, err
	}
	timeIterator, err := newTimeIteratorFromMeta(v.metaProto, v.encodedValues, noScale)
	if err != nil {
		return nil, err
	}
	// Rather than scaling up every time value in the iterator and comparing it to the filterValue,
	// instead scale down the filterValue and compare it against the time values sans scaling.
	scaledDownFilterValue := field.NewTimeUnion(convert.ScaleDownTimeFn(filterValue.TimeNanosVal, resolution))
	timeFlt, err := op.TimeFilter(&scaledDownFilterValue)
	if err != nil {
		return nil, err
	}
	return impl.NewFilteredTimeIterator(timeIterator, timeFlt), nil
}

func (v *fsBasedTimeValues) Close() {
	if v.closed {
		return
	}
	v.closed = true
	v.encodedValues = nil
}
