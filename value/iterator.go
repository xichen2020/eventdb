package value

import (
	"github.com/xichen2020/eventdb/event/field"
	"github.com/xichen2020/eventdb/x/convert"
)

// jsonIterator iterates over fields in a JSON value.
type jsonIterator struct {
	objs  []objectIndex
	path  []string
	value field.Value
}

// NewFieldIterator creates a new field iterator from a JSON value.
// NB(xichen): This iterator currently skips all nested JSON arrays.
func NewFieldIterator(v *Value) field.Iterator {
	var (
		objs []objectIndex
		path = make([]string, 0, 4)
	)
	if v.Type() == ObjectType {
		obj := v.MustObject()
		objs = append(objs, objectIndex{obj: obj, idx: -1})
		path = append(path, "")
	}
	return &jsonIterator{
		objs: objs,
		path: path,
	}
}

func (it *jsonIterator) Next() bool {
	for len(it.objs) > 0 {
		lastIdx := len(it.objs) - 1
		for it.objs[lastIdx].idx++; it.objs[lastIdx].idx < it.objs[lastIdx].obj.Len(); it.objs[lastIdx].idx++ {
			kv := it.objs[lastIdx].obj.At(it.objs[lastIdx].idx)
			v := kv.Value()
			switch v.Type() {
			case NullType:
				it.path[lastIdx] = kv.Key()
				it.value.Type = field.NullType
				return true
			case BoolType:
				it.path[lastIdx] = kv.Key()
				it.value.Type = field.BoolType
				it.value.BoolVal = v.MustBool()
				return true
			case StringType:
				it.path[lastIdx] = kv.Key()
				it.value.Type = field.StringType
				it.value.StringVal = v.MustString()
				return true
			case NumberType:
				it.path[lastIdx] = kv.Key()
				n := kv.Value().MustNumber()
				if iv, ok := convert.TryAsInt(n); ok {
					it.value.Type = field.IntType
					it.value.IntVal = iv
				} else {
					it.value.Type = field.DoubleType
					it.value.DoubleVal = n
				}
				return true
			case ObjectType:
				it.path[lastIdx] = kv.Key()
				it.path = append(it.path, "")
				it.objs = append(it.objs, objectIndex{obj: v.MustObject(), idx: -1})
				return it.Next()
			default:
				// NB: Skip arrays.
				continue
			}
		}
		it.objs = it.objs[:lastIdx]
		it.path = it.path[:len(it.path)-1]
	}
	return false
}

func (it *jsonIterator) Current() field.Field {
	return field.Field{Path: it.path, Value: it.value}
}

func (it *jsonIterator) Close() {}

type objectIndex struct {
	obj Object
	idx int
}
