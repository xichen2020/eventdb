package decoding

import (
	"github.com/xichen2020/eventdb/document/field"
	"github.com/xichen2020/eventdb/filter"
	"github.com/xichen2020/eventdb/generated/proto/encodingpb"
	"github.com/xichen2020/eventdb/values"
	"github.com/xichen2020/eventdb/values/iterator"
)

// fsBasedStringValues is a string values collection backed by encoded data on the filesystem.
type fsBasedStringValues struct {
	metaProto        encodingpb.StringMeta
	encodedValues    []byte
	dictArr          []string       // For fast iteration
	dictMap          map[string]int // For fast lookup
	encodedDictBytes int
	closed           bool
}

// newFsBasedStringValues creates a new fs based string values.
func newFsBasedStringValues(
	metaProto encodingpb.StringMeta,
	encodedValues []byte, // Encoded values not including string meta but includes dictionary if applicable
	dict []string, // If values are dict encoded, this is the dictionary, otherwise nil. This is not cached.
	encodedDictBytes int, // Number of encoded bytes for decoding the dictionary in `data` if applicable, or 0 otherwise.
) values.CloseableStringValues {
	var (
		dictArr []string
		dictMap map[string]int
	)
	if len(dict) > 0 {
		dictArr = make([]string, len(dict))
		copy(dictArr, dict)

		dictMap = make(map[string]int, len(dictArr))
		for i, str := range dictArr {
			dictMap[str] = i
		}
	}

	return &fsBasedStringValues{
		metaProto:        metaProto,
		encodedValues:    encodedValues,
		dictArr:          dictArr,
		dictMap:          dictMap,
		encodedDictBytes: encodedDictBytes,
	}
}

func (v *fsBasedStringValues) Metadata() values.StringValuesMetadata {
	return values.StringValuesMetadata{
		Min:  v.metaProto.MinValue,
		Max:  v.metaProto.MaxValue,
		Size: int(v.metaProto.NumValues),
	}
}

func (v *fsBasedStringValues) Iter() (iterator.ForwardStringIterator, error) {
	return newStringIteratorFromMeta(v.metaProto, v.encodedValues, v.dictArr, v.encodedDictBytes)
}

// TODO(xichen): Filter implementation should take advantage of the metadata
// to do more intelligent filtering, e.g., checking if the value is within the
// value range, intelligently look up filter values and bail early if not found,
// perform filtering directly against the index to avoid string comparisons.
func (v *fsBasedStringValues) Filter(
	op filter.Op,
	filterValue *field.ValueUnion,
) (iterator.PositionIterator, error) {
	return defaultFilteredFsBasedStringValueIterator(v, op, filterValue)
}

func (v *fsBasedStringValues) Close() {
	if v.closed {
		return
	}
	v.closed = true
	v.dictArr = nil
	v.dictMap = nil
	v.encodedValues = nil
}
