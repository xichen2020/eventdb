package decoding

import (
	"github.com/xichen2020/eventdb/document/field"
	"github.com/xichen2020/eventdb/filter"
	"github.com/xichen2020/eventdb/generated/proto/encodingpb"
	"github.com/xichen2020/eventdb/values"
	"github.com/xichen2020/eventdb/values/iterator"
)

// fsBasedIntValues is a int values collection backed by encoded data on the filesystem.
type fsBasedIntValues struct {
	metaProto        encodingpb.IntMeta
	encodedValues    []byte
	encodedDict      []byte
	encodedDictBytes int
	closed           bool
}

// newFsBasedIntValues creates a new fs based int values.
func newFsBasedIntValues(
	metaProto encodingpb.IntMeta,
	encodedValues []byte, // Encoded values not including int meta but includes dictionary if applicable
	encodedDict []byte, // If values are dict encoded, this is the bit-packed encoded dictionary, otherwise nil. This is not cached.
	encodedDictBytes int, // Number of encoded bytes for decoding the dictionary in `data` if applicable, or 0 otherwise.
) values.CloseableIntValues {
	clonedDict := make([]byte, len(encodedDict))
	copy(clonedDict, encodedDict)

	return &fsBasedIntValues{
		metaProto:        metaProto,
		encodedValues:    encodedValues,
		encodedDict:      clonedDict,
		encodedDictBytes: encodedDictBytes,
	}
}

func (v *fsBasedIntValues) Metadata() values.IntValuesMetadata {
	return values.IntValuesMetadata{
		Min:  int(v.metaProto.MinValue),
		Max:  int(v.metaProto.MaxValue),
		Size: int(v.metaProto.NumValues),
	}
}

func (v *fsBasedIntValues) Iter() (iterator.ForwardIntIterator, error) {
	return newIntIteratorFromMeta(v.metaProto, v.encodedValues, v.encodedDict, v.encodedDictBytes)
}

// TODO(xichen): Filter implementation should take advantage of the metadata
// to do more intelligent filtering, e.g., checking if the value is within the
// value range, and intelligently look up filter values and bail early if not found.
func (v *fsBasedIntValues) Filter(
	op filter.Op,
	filterValue *field.ValueUnion,
) (iterator.PositionIterator, error) {
	return defaultFilteredFsBasedIntValueIterator(v, op, filterValue)
}

func (v *fsBasedIntValues) Close() {
	if v.closed {
		return
	}
	v.closed = true
	v.encodedValues = nil
	v.encodedDict = nil
}
