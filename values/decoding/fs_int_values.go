package decoding

import (
	"fmt"

	"github.com/xichen2020/eventdb/document/field"
	"github.com/xichen2020/eventdb/filter"
	"github.com/xichen2020/eventdb/generated/proto/encodingpb"
	"github.com/xichen2020/eventdb/values"
	"github.com/xichen2020/eventdb/values/iterator"
	"github.com/xichen2020/eventdb/values/iterator/impl"
)

// fsBasedIntValues is a int values collection backed by encoded data on the filesystem.
type fsBasedIntValues struct {
	metaProto        encodingpb.IntMeta
	encodedValues    []byte
	dictArr          []int
	dictMap          map[int]int // For fast lookup
	encodedDictBytes int
	closed           bool
}

// newFsBasedIntValues creates a new fs based int values.
func newFsBasedIntValues(
	metaProto encodingpb.IntMeta,
	encodedValues []byte, // Encoded values not including int meta but includes dictionary if applicable
	dict []int, // If values are dict encoded, this is the dictionary, otherwise nil. This is not cached.
	encodedDictBytes int, // Number of encoded bytes for decoding the dictionary in `data` if applicable, or 0 otherwise.
) values.CloseableIntValues {
	var (
		dictArr []int
		dictMap map[int]int
	)
	if len(dict) > 0 {
		dictArr = make([]int, len(dict))
		copy(dictArr, dict)

		dictMap = make(map[int]int, len(dictArr))
		for i, v := range dictArr {
			dictMap[v] = i
		}
	}

	return &fsBasedIntValues{
		metaProto:        metaProto,
		encodedValues:    encodedValues,
		dictArr:          dictArr,
		dictMap:          dictMap,
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
	return newIntIteratorFromMeta(v.metaProto, v.encodedValues, v.dictArr, v.encodedDictBytes)
}

func (v *fsBasedIntValues) Filter(
	op filter.Op,
	filterValue *field.ValueUnion,
) (iterator.PositionIterator, error) {
	if filterValue == nil {
		return nil, errNilFilterValue
	}
	var (
		intVal       int
		maybeInRange bool
	)
	switch filterValue.Type {
	case field.DoubleType:
		var err error
		intVal, maybeInRange, err = op.DoubleMaybeInIntRange(int(v.metaProto.MinValue), int(v.metaProto.MaxValue), filterValue.DoubleVal)
		if err != nil {
			return nil, err
		}
	case field.IntType:
		intVal = filterValue.IntVal
		maybeInRange = op.IntMaybeInRange(int(v.metaProto.MinValue), int(v.metaProto.MaxValue), filterValue.IntVal)
	default:
		return nil, fmt.Errorf("int values filter expects double or int filter value type but got %v filter value type", filterValue.Type)
	}
	if !maybeInRange {
		return impl.NewEmptyPositionIterator(), nil
	}
	if v.metaProto.Encoding != encodingpb.EncodingType_DICTIONARY {
		return defaultFilteredFsBasedIntValueIterator(v, op, filterValue)
	}
	idx, ok := v.dictMap[intVal]
	if !ok {
		return impl.NewEmptyPositionIterator(), nil
	}
	// Rather than comparing the filterValue against every value in the iterator, perform
	// filtering directly against the dictionary indexes; this saves the lookup on every iteration.
	idxIterator, err := newIntDictionaryIndexIterator(
		v.encodedValues,
		v.encodedDictBytes,
		int(v.metaProto.BitsPerEncodedValue),
		int(v.metaProto.NumValues),
	)
	if err != nil {
		return nil, err
	}
	idxFilterValue := field.NewIntUnion(idx)
	intFlt, err := op.IntFilter(&idxFilterValue)
	if err != nil {
		return nil, err
	}
	return impl.NewFilteredIntIterator(idxIterator, intFlt), nil
}

func (v *fsBasedIntValues) Close() {
	if v.closed {
		return
	}
	v.closed = true
	v.encodedValues = nil
	v.dictArr = nil
	v.dictMap = nil
}
