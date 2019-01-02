package iterator

import (
	"github.com/xichen2020/eventdb/index"

	"github.com/mauricelam/genny/generic"
)

// GenericValue is the value type.
type GenericValue generic.Type

// ValueIterator is a value iterator.
type ValueIterator interface {
	generic.Type

	Next() bool
	// TODO(xichen): Change this to Value.
	Current() GenericValue
	Close()
}

// DocIDWithValueIterator iterates over a collection of (doc ID, value) pairs.
type DocIDWithValueIterator struct {
	dit index.DocIDSetIterator
	vit ValueIterator
}

// NewDocIDWithValueIterator creates a new iterator.
func NewDocIDWithValueIterator(
	dit index.DocIDSetIterator,
	vit ValueIterator,
) *DocIDWithValueIterator {
	return &DocIDWithValueIterator{
		dit: dit,
		vit: vit,
	}
}

// Next returns true if there are more pairs to be iterated over.
func (it *DocIDWithValueIterator) Next() bool { return it.dit.Next() && it.vit.Next() }

// DocID returns the current doc ID.
func (it *DocIDWithValueIterator) DocID() int32 { return it.dit.DocID() }

// Value returns the current value.
func (it *DocIDWithValueIterator) Value() GenericValue { return it.vit.Current() }

// Close closes the iterator.
func (it *DocIDWithValueIterator) Close() {
	it.dit.Close()
	it.vit.Close()
}

// DocIDValuePairIterator iterates over a collection of (doc ID, value) pairs.
type DocIDValuePairIterator interface {
	generic.Type
	index.DocIDSetIterator

	Value() GenericValue
}

// ValueFilter performs filtering against values.
type ValueFilter interface {
	generic.Type

	Match(v GenericValue) bool
}

// FilteredDocIDWithValueIterator is a pair iterator with a value filter.
type FilteredDocIDWithValueIterator struct {
	pit DocIDValuePairIterator
	f   ValueFilter

	docID int32
	value GenericValue
}

// NewFilteredDocIDWithValueIterator creates a new filtering iterator.
func NewFilteredDocIDWithValueIterator(
	pit DocIDValuePairIterator,
	f ValueFilter,
) *FilteredDocIDWithValueIterator {
	return &FilteredDocIDWithValueIterator{
		pit: pit,
		f:   f,
	}
}

// Next returns true if there are more values to be iterated over.
func (it *FilteredDocIDWithValueIterator) Next() bool {
	for it.pit.Next() {
		it.docID = it.pit.DocID()
		it.value = it.pit.Value()
		if it.f.Match(it.value) {
			return true
		}
	}
	return false
}

// DocID returns the current doc ID.
func (it *FilteredDocIDWithValueIterator) DocID() int32 { return it.docID }

// Value returns the current value.
func (it *FilteredDocIDWithValueIterator) Value() GenericValue { return it.value }

// Close closes the iterator.
func (it *FilteredDocIDWithValueIterator) Close() { it.pit.Close() }
