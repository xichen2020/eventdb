package decoding

import (
	"github.com/xichen2020/eventdb/document/field"
	"github.com/xichen2020/eventdb/filter"
	"github.com/xichen2020/eventdb/generated/proto/encodingpb"
	"github.com/xichen2020/eventdb/values"
	"github.com/xichen2020/eventdb/values/iterator"
	"github.com/xichen2020/eventdb/values/iterator/impl"
	"github.com/xichen2020/eventdb/x/hashmap"
)

// fsBasedBytesValues is a bytes values collection backed by encoded data on the filesystem.
type fsBasedBytesValues struct {
	metaProto        encodingpb.BytesMeta
	encodedValues    []byte
	dictArr          [][]byte                 // For fast iteration
	dictMap          *hashmap.BytesIntHashMap // For fast lookup
	encodedDictBytes int
	closed           bool
}

// newFsBasedBytesValues creates a new fs based bytes values.
func newFsBasedBytesValues(
	metaProto encodingpb.BytesMeta,
	encodedValues []byte, // Encoded values not including bytes meta but includes dictionary if applicable
	dict [][]byte, // If values are dict encoded, this is the dictionary, otherwise nil. This is not cached.
	encodedDictBytes int, // Number of encoded bytes for decoding the dictionary in `data` if applicable, or 0 otherwise.
) values.CloseableBytesValues {
	var (
		dictArr [][]byte
		dictMap *hashmap.BytesIntHashMap
	)
	if len(dict) > 0 {
		dictArr = make([][]byte, len(dict))
		copy(dictArr, dict)

		dictMap = hashmap.NewBytesIntHashMap(hashmap.BytesIntHashMapOptions{
			InitialSize: len(dictArr),
		})
		for i, b := range dictArr {
			dictMap.Set(b, i)
		}
	}

	return &fsBasedBytesValues{
		metaProto:        metaProto,
		encodedValues:    encodedValues,
		dictArr:          dictArr,
		dictMap:          dictMap,
		encodedDictBytes: encodedDictBytes,
	}
}

func (v *fsBasedBytesValues) Metadata() values.BytesValuesMetadata {
	return values.BytesValuesMetadata{
		Min:  v.metaProto.MinValue,
		Max:  v.metaProto.MaxValue,
		Size: int(v.metaProto.NumValues),
	}
}

func (v *fsBasedBytesValues) Iter() (iterator.ForwardBytesIterator, error) {
	return newBytesIteratorFromMeta(v.metaProto, v.encodedValues, v.dictArr, v.encodedDictBytes)
}

func (v *fsBasedBytesValues) Filter(
	op filter.Op,
	filterValue *field.ValueUnion,
) (iterator.PositionIterator, error) {
	if filterValue == nil {
		return nil, errNilFilterValue
	}
	if filterValue.Type != field.BytesType {
		return nil, errUnexpectedFilterValueType
	}
	if !op.BytesMaybeInRange(v.metaProto.MinValue, v.metaProto.MaxValue, filterValue.BytesVal.Bytes()) {
		return impl.NewEmptyPositionIterator(), nil
	}
	if v.metaProto.Encoding != encodingpb.EncodingType_DICTIONARY {
		return defaultFilteredFsBasedBytesValueIterator(v, op, filterValue)
	}
	idx, ok := v.dictMap.Get(filterValue.BytesVal.Bytes())
	if !ok {
		return impl.NewEmptyPositionIterator(), nil
	}
	// Rather than comparing the filterValue against every bytes in the iterator, perform
	// filtering directly against the dictionary indexes to avoid bytes comparisons.
	idxIterator, err := newBytesDictionaryIndexIterator(v.metaProto, v.encodedValues, v.encodedDictBytes)
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

func (v *fsBasedBytesValues) Close() {
	if v.closed {
		return
	}
	v.closed = true
	v.dictArr = nil
	v.dictMap = nil
	v.encodedValues = nil
}
