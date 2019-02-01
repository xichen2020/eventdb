package decoding

import (
	"github.com/xichen2020/eventdb/document/field"
	"github.com/xichen2020/eventdb/filter"
	"github.com/xichen2020/eventdb/generated/proto/encodingpb"
	"github.com/xichen2020/eventdb/values"
	"github.com/xichen2020/eventdb/values/iterator"
	"github.com/xichen2020/eventdb/values/iterator/impl"
)

// fsBasedDoubleValues is a double values collection backed by encoded data on the filesystem.
type fsBasedDoubleValues struct {
	metaProto     encodingpb.DoubleMeta
	encodedValues []byte

	closed bool
}

// newFsBasedDoubleValues creates a new fs based double values.
func newFsBasedDoubleValues(
	metaProto encodingpb.DoubleMeta,
	encodedValues []byte, // Encoded values not including double meta
) values.CloseableDoubleValues {
	return &fsBasedDoubleValues{
		metaProto:     metaProto,
		encodedValues: encodedValues,
	}
}

func (v *fsBasedDoubleValues) Metadata() values.DoubleValuesMetadata {
	return values.DoubleValuesMetadata{
		Size: int(v.metaProto.NumValues),
		Min:  v.metaProto.MinValue,
		Max:  v.metaProto.MaxValue,
	}
}

func (v *fsBasedDoubleValues) Iter() (iterator.ForwardDoubleIterator, error) {
	return newDoubleIteratorFromMeta(v.metaProto, v.encodedValues)
}

func (v *fsBasedDoubleValues) Filter(
	op filter.Op,
	filterValue *field.ValueUnion,
) (iterator.PositionIterator, error) {
	if filterValue == nil {
		return nil, errNilFilterValue
	}
	if filterValue.Type != field.DoubleType {
		return nil, errUnexpectedFilterValueType
	}
	if !op.DoubleMaybeInRange(v.metaProto.MinValue, v.metaProto.MaxValue, filterValue.DoubleVal) {
		return impl.NewEmptyPositionIterator(), nil
	}
	return defaultFilteredFsBasedDoubleValueIterator(v, op, filterValue)
}

func (v *fsBasedDoubleValues) Close() {
	if v.closed {
		return
	}
	v.closed = true
	v.encodedValues = nil
}
