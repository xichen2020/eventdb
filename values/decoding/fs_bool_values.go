package decoding

import (
	"github.com/xichen2020/eventdb/document/field"
	"github.com/xichen2020/eventdb/filter"
	"github.com/xichen2020/eventdb/generated/proto/encodingpb"
	"github.com/xichen2020/eventdb/values"
	"github.com/xichen2020/eventdb/values/iterator"
)

// fsBasedBoolValues is a bool values collection backed by encoded data on the filesystem.
type fsBasedBoolValues struct {
	metaProto     encodingpb.BoolMeta
	encodedValues []byte

	closed bool
}

// newFsBasedBoolValues creates a new fs based bool values.
func newFsBasedBoolValues(
	metaProto encodingpb.BoolMeta,
	encodedValues []byte, // Encoded values not including bool meta
) values.CloseableBoolValues {
	return &fsBasedBoolValues{
		metaProto:     metaProto,
		encodedValues: encodedValues,
	}
}

func (v *fsBasedBoolValues) Metadata() values.BoolValuesMetadata {
	return values.BoolValuesMetadata{
		NumTrues:  int(v.metaProto.NumTrues),
		NumFalses: int(v.metaProto.NumFalses),
	}
}

func (v *fsBasedBoolValues) Iter() (iterator.ForwardBoolIterator, error) {
	return newBoolIteratorFromMeta(v.metaProto, v.encodedValues)
}

// TODO(xichen): Filter implementation should take advantage of the metadata
// to do more intelligent filtering, e.g., checking if the value is within the
// value range.
func (v *fsBasedBoolValues) Filter(
	op filter.Op,
	filterValue *field.ValueUnion,
) (iterator.PositionIterator, error) {
	return defaultFilteredFsBasedBoolValueIterator(v, op, filterValue)
}

func (v *fsBasedBoolValues) Close() {
	if v.closed {
		return
	}
	v.closed = true
	v.encodedValues = nil
}
