// This file was automatically generated by genny.
// Any changes will be lost if this file is regenerated.
// see https://github.com/mauricelam/genny

package impl

import "github.com/xichen2020/eventdb/values/iterator"
import "github.com/xichen2020/eventdb/filter"

// FilteredTimeIterator is a position iterator that outputs the positions
// of values in the value sequence matching a given filter. The position starts at 0.
type FilteredTimeIterator struct {
	vit iterator.ForwardTimeIterator
	f   filter.TimeFilter

	done    bool
	currPos int
}

// NewFilteredTimeIterator creates a new filtering iterator.
func NewFilteredTimeIterator(
	vit iterator.ForwardTimeIterator,
	f filter.TimeFilter,
) *FilteredTimeIterator {
	return &FilteredTimeIterator{
		vit:     vit,
		f:       f,
		currPos: -1,
	}
}

// Next returns true if there are more values to be iterated over.
func (it *FilteredTimeIterator) Next() bool {
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
func (it *FilteredTimeIterator) Position() int { return it.currPos }

// Close closes the iterator.
func (it *FilteredTimeIterator) Close() {
	it.vit.Close()
	it.vit = nil
	it.f = nil
}
