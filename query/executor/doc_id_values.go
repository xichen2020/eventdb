package executor

import "github.com/xichen2020/eventdb/document/field"

type docIDValues struct {
	DocID  int32
	Values field.Values
}

// cloneDocIDValues clones the incoming (doc ID, values) pair.
func cloneDocIDValues(v docIDValues) docIDValues {
	// TODO(xichen): Should pool and reuse the value array here.
	v.Values = v.Values.Clone(field.ValueCloneOptions{DeepCloneBytes: true})
	return v
}

// cloneDocIDValuesTo clones the (doc ID, values) pair from `src` to `target`.
// Precondition: `target` values are guaranteed to have the same length as `src` and can be reused.
func cloneDocIDValuesTo(src docIDValues, target *docIDValues) {
	target.DocID = src.DocID
	for i := 0; i < len(src.Values); i++ {
		cloned := src.Values[i].Clone(field.ValueCloneOptions{DeepCloneBytes: true})
		target.Values[i] = cloned
	}
}

type docIDValuesByDocIDAsc []docIDValues

func (a docIDValuesByDocIDAsc) Len() int           { return len(a) }
func (a docIDValuesByDocIDAsc) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a docIDValuesByDocIDAsc) Less(i, j int) bool { return a[i].DocID < a[j].DocID }

// docIDValuesIterator iterates over a sequence of (doc ID, values) pairs.
type docIDValuesIterator struct {
	data []docIDValues

	currIdx int
}

// newDocIDValuesIterator creates a new doc ID values iterator.
func newDocIDValuesIterator(data []docIDValues) *docIDValuesIterator {
	return &docIDValuesIterator{
		data:    data,
		currIdx: -1,
	}
}

// Next returns true if there are more pairs to be iterated over.
func (it *docIDValuesIterator) Next() bool {
	if it.currIdx >= len(it.data) {
		return false
	}
	it.currIdx++
	return it.currIdx < len(it.data)
}

// DocID returns the doc ID of the current pair.
func (it *docIDValuesIterator) DocID() int32 { return it.data[it.currIdx].DocID }

// Values returns the current collection of values.
func (it *docIDValuesIterator) Values() field.Values { return it.data[it.currIdx].Values }

// Err returns errors if any.
func (it *docIDValuesIterator) Err() error { return nil }

// Close closes the iterator.
func (it *docIDValuesIterator) Close() { it.data = nil }

type docIDValuesLessThanFn func(v1, v2 docIDValues) bool

func newDocIDValuesLessThanFn(fn field.ValuesLessThanFn) docIDValuesLessThanFn {
	return func(v1, v2 docIDValues) bool {
		return fn(v1.Values, v2.Values)
	}
}
