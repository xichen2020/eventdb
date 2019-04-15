package value

import (
	"github.com/xichen2020/eventdb/document/field"
	"github.com/xichen2020/eventdb/x/bytes"
	"github.com/xichen2020/eventdb/x/convert"
)

// ArrayAwareIterator ...
type ArrayAwareIterator interface {
	field.Iterator

	Arr() []*Value
}

// jsonIterator iterates over fields in a JSON value.
type jsonIterator struct {
	objs  []objectIndex
	path  []string
	value field.ValueUnion
	arr   []*Value
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
			case BytesType:
				it.path[lastIdx] = kv.Key()
				it.value.Type = field.BytesType
				it.value.BytesVal = bytes.NewImmutableBytes(v.MustBytes())
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
			case ArrayType:
				it.path[lastIdx] = kv.Key()
				it.value.Type = field.ArrayType
				it.arr = v.MustArray().raw
				return true
			default:
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

func (it *jsonIterator) Arr() []*Value {
	ret := it.arr
	it.arr = nil
	return ret
}

func (it *jsonIterator) Close() {}

type objectIndex struct {
	obj Object
	idx int
}

type dumper struct {
	dumps   []Dump
	array   []*Value
	kvArray []KV
	idx     int
}

// Dump ...
type Dump struct {
	Path  []string
	Value field.ValueUnion
}

// ArrayDump ...
func ArrayDump(path []string, a []*Value) []Dump {
	d := &dumper{
		dumps: []Dump{},
		array: a,
	}
	d.recurseArr(path)
	return d.dumps
}

func (d *dumper) recurseArr(path []string) {
	for ; d.idx < len(d.array); d.idx++ {
		v := d.array[d.idx]
		cpy := make([]string, len(path))
		copy(cpy, path)
		switch v.Type() {
		case NullType:
			d.dumps = append(d.dumps, Dump{Path: cpy, Value: field.ValueUnion{Type: field.NullType}})
		case BoolType:
			d.dumps = append(d.dumps, Dump{Path: cpy, Value: field.ValueUnion{Type: field.BoolType, BoolVal: v.MustBool()}})
		case BytesType:
			d.dumps = append(d.dumps, Dump{Path: cpy, Value: field.ValueUnion{Type: field.BytesType, BytesVal: bytes.NewImmutableBytes(v.MustBytes())}})
		case NumberType:
			n := v.MustNumber()
			if iv, ok := convert.TryAsInt(n); ok {
				d.dumps = append(d.dumps, Dump{Path: cpy, Value: field.ValueUnion{Type: field.IntType, IntVal: iv}})
			} else {
				d.dumps = append(d.dumps, Dump{Path: cpy, Value: field.ValueUnion{Type: field.DoubleType, DoubleVal: n}})
			}
		case ObjectType:
			nD := &dumper{dumps: []Dump{}, kvArray: v.MustObject().kvs.raw}
			nD.recurseObj(cpy)
			d.dumps = append(d.dumps, nD.dumps...)
		case ArrayType:
			nD := &dumper{dumps: []Dump{}, array: v.MustArray().raw}
			nD.recurseArr(cpy)
			d.dumps = append(d.dumps, nD.dumps...)
		default:
			continue
		}
	}
}

func (d *dumper) recurseObj(path []string) {
	for ; d.idx < len(d.kvArray); d.idx++ {
		kv := d.kvArray[d.idx]
		cpy := make([]string, len(path))
		copy(cpy, path)
		cpy = append(cpy, kv.Key())
		v := kv.Value()
		switch v.Type() {
		case NullType:
			d.dumps = append(d.dumps, Dump{Path: cpy, Value: field.ValueUnion{Type: field.NullType}})
		case BoolType:
			d.dumps = append(d.dumps, Dump{Path: cpy, Value: field.ValueUnion{Type: field.BoolType, BoolVal: v.MustBool()}})
		case BytesType:
			d.dumps = append(d.dumps, Dump{Path: cpy, Value: field.ValueUnion{Type: field.BytesType, BytesVal: bytes.NewImmutableBytes(v.MustBytes())}})
		case NumberType:
			n := v.MustNumber()
			if iv, ok := convert.TryAsInt(n); ok {
				d.dumps = append(d.dumps, Dump{Path: cpy, Value: field.ValueUnion{Type: field.IntType, IntVal: iv}})
			} else {
				d.dumps = append(d.dumps, Dump{Path: cpy, Value: field.ValueUnion{Type: field.DoubleType, DoubleVal: n}})
			}
		case ObjectType:
			nD := dumper{dumps: []Dump{}, kvArray: v.MustObject().kvs.raw}
			nD.recurseObj(cpy)
			d.dumps = append(d.dumps, nD.dumps...)
		case ArrayType:
			nD := dumper{dumps: []Dump{}, array: v.MustArray().raw}
			nD.recurseArr(cpy)
			d.dumps = append(d.dumps, nD.dumps...)
		default:
			continue
		}
	}
}
