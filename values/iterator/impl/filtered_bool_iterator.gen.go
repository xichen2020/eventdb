// This file was automatically generated by genny.
// Any changes will be lost if this file is regenerated.
// see https://github.com/mauricelam/genny

package impl

import "github.com/xichen2020/eventdb/values/iterator"
import "github.com/xichen2020/eventdb/filter"

// FilteredBoolIterator is a position iterator that outputs the positions
// of values in the value sequence matching a given filter. The position starts at 0.
type FilteredBoolIterator struct {
	vit iterator.ForwardBoolIterator
	f   filter.BoolFilter

	done    bool
	currPos int
}

// NewFilteredBoolIterator creates a new filtering iterator.
func NewFilteredBoolIterator(
	vit iterator.ForwardBoolIterator,
	f filter.BoolFilter,
) *FilteredBoolIterator {
	return &FilteredBoolIterator{
		vit:     vit,
		f:       f,
		currPos: -1,
	}
}

// Next returns true if there are more values to be iterated over.
func (it *FilteredBoolIterator) Next() bool {
	if it.done {
		return false
	}
	for it.vit.Next() {
		it.currPos++
		if it.f.Match(it.vit.Current()) {
			return true
		}
	}
	it.done = true
	return false
}

// Position returns the current position.
func (it *FilteredBoolIterator) Position() int { return it.currPos }

// Close closes the iterator.
func (it *FilteredBoolIterator) Close() {
	it.vit.Close()
	it.vit = nil
	it.f = nil
}
