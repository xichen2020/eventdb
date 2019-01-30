package decoding

import (
	"github.com/xichen2020/eventdb/document/field"
	"github.com/xichen2020/eventdb/filter"
	"github.com/xichen2020/eventdb/generated/proto/encodingpb"
	"github.com/xichen2020/eventdb/values"
	"github.com/xichen2020/eventdb/values/iterator"
	"github.com/xichen2020/eventdb/values/iterator/impl"
	xio "github.com/xichen2020/eventdb/x/io"
)

// fsBasedIntValues is a int values collection backed by encoded data on the filesystem.
type fsBasedIntValues struct {
	metaProto        encodingpb.IntMeta
	encodedValues    []byte
	encodedDict      []byte
	dictSet          map[int]struct{} // For fast lookup
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

	var (
		dictSet         map[int]struct{}
		bytesPerDictVal = int(metaProto.BytesPerDictionaryValue)
		minVal          = int(metaProto.MinValue)
	)
	if len(clonedDict) > 0 {
		dictSet = make(map[int]struct{}, len(clonedDict)/bytesPerDictVal)
		for start := 0; start < len(clonedDict); start += bytesPerDictVal {
			decodedVal := minVal + int(xio.ReadInt(bytesPerDictVal, clonedDict[start:]))
			dictSet[decodedVal] = struct{}{}
		}
	}

	return &fsBasedIntValues{
		metaProto:        metaProto,
		encodedValues:    encodedValues,
		encodedDict:      clonedDict,
		dictSet:          dictSet,
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

func (v *fsBasedIntValues) Filter(
	op filter.Op,
	filterValue *field.ValueUnion,
) (iterator.PositionIterator, error) {
	if filterValue == nil {
		return nil, errNilFilterValue
	}
	if filterValue.Type != field.IntType {
		return nil, errUnexpectedFilterValueType
	}
	if !op.IntIsInRange(v.metaProto, filterValue.IntVal) {
		return impl.NewEmptyPositionIterator(), nil
	}
	if v.metaProto.Encoding == encodingpb.EncodingType_DICTIONARY {
		if _, ok := v.dictSet[filterValue.IntVal]; !ok {
			return impl.NewEmptyPositionIterator(), nil
		}
	}
	return defaultFilteredFsBasedIntValueIterator(v, op, filterValue)
}

func (v *fsBasedIntValues) Close() {
	if v.closed {
		return
	}
	v.closed = true
	v.encodedValues = nil
	v.encodedDict = nil
	v.dictSet = nil
}
