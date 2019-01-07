package decoding

import (
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

func (v *fsBasedStringValues) Close() {
	if v.closed {
		return
	}
	v.closed = true
	v.dictArr = nil
	v.dictMap = nil
	v.encodedValues = nil
}
