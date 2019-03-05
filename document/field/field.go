package field

import (
	"github.com/xichen2020/eventdb/generated/proto/servicepb"
)

// Field is an event field.
type Field struct {
	Path  []string
	Value ValueUnion
}

// Clone clones a field.
func (f *Field) Clone() Field {
	pathClone := make([]string, len(f.Path))
	copy(pathClone, f.Path)
	return Field{Path: pathClone, Value: f.Value}
}

// ToProto converts a field to a field protobuf message.
func (f *Field) ToProto() (servicepb.Field, error) {
	pbValue, err := f.Value.ToProto()
	if err != nil {
		return servicepb.Field{}, err
	}
	return servicepb.Field{
		Path:  f.Path,
		Value: pbValue,
	}, nil
}

// Reset resets a field.
func (f *Field) Reset() {
	f.Path = nil
	f.Value = ValueUnion{}
}

// ReturnArrayToPool returns an field array to pool.
func ReturnArrayToPool(fields []Field, p *BucketizedFieldArrayPool) {
	if p == nil {
		return
	}
	for i := 0; i < len(fields); i++ {
		fields[i].Reset()
	}
	fields = fields[:0]
	p.Put(fields, cap(fields))
}

// Fields is a list of fields.
type Fields []Field

// ToProto converts a list of fields to a list of fields in protobuf message.
func (f Fields) ToProto() ([]servicepb.Field, error) {
	if len(f) == 0 {
		return nil, nil
	}
	res := make([]servicepb.Field, 0, len(f))
	for _, field := range f {
		pbField, err := field.ToProto()
		if err != nil {
			return nil, err
		}
		res = append(res, pbField)
	}
	return res, nil
}

// Iterator iterate over a set of fields.
type Iterator interface {
	// Next returns true if there are more fields to be iterated over,
	// and false otherwise.
	Next() bool

	// Current returns the current field. The field remains valid
	// until the next Next() call.
	Current() Field

	// Close closes the iterator.
	Close()
}

// arrayBasedIterator iterates over a field array.
type arrayBasedIterator struct {
	arr []Field
	p   *BucketizedFieldArrayPool

	closed bool
	idx    int
}

// NewArrayBasedIterator creates an array based iterator.
// If the array pool is nil, the field array should be returned to pool on close.
func NewArrayBasedIterator(arr []Field, p *BucketizedFieldArrayPool) Iterator {
	return &arrayBasedIterator{
		arr: arr,
		p:   p,
		idx: -1,
	}
}

func (it *arrayBasedIterator) Next() bool {
	if it.idx >= len(it.arr) {
		return false
	}
	it.idx++
	return it.idx < len(it.arr)
}

func (it *arrayBasedIterator) Current() Field { return it.arr[it.idx] }

func (it *arrayBasedIterator) Close() {
	if it.closed {
		return
	}
	it.closed = true
	for i := 0; i < len(it.arr); i++ {
		it.arr[i].Reset()
	}
	it.arr = it.arr[:0]
	if it.p != nil {
		it.p.Put(it.arr, cap(it.arr))
	}
}
