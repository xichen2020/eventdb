package field

// Field is an event field.
type Field struct {
	Path  []string
	Value ValueUnion
}

// Reset resets a field.
func (f *Field) Reset() {
	f.Path = nil
	f.Value = ValueUnion{}
}

// ReturnArrayToPool returns a document array to pool.
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

func (it *arrayBasedIterator) Next() bool     { return it.idx >= len(it.arr) }
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
