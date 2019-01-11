// Copyright (c) 2019 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

// This file was automatically generated by genny.
// Any changes will be lost if this file is regenerated.
// see https://github.com/mauricelam/genny

package impl

import "github.com/xichen2020/eventdb/values/iterator"
import "github.com/xichen2020/eventdb/filter"

// FilteredDoubleIterator is a position iterator that outputs the positions
// of values in the value sequence matching a given filter. The position starts at 0.
type FilteredDoubleIterator struct {
	vit iterator.ForwardDoubleIterator
	f   filter.DoubleFilter

	done    bool
	currPos int
}

// NewFilteredDoubleIterator creates a new filtering iterator.
func NewFilteredDoubleIterator(
	vit iterator.ForwardDoubleIterator,
	f filter.DoubleFilter,
) *FilteredDoubleIterator {
	return &FilteredDoubleIterator{
		vit:     vit,
		f:       f,
		currPos: -1,
	}
}

// Next returns true if there are more values to be iterated over.
func (it *FilteredDoubleIterator) Next() bool {
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
func (it *FilteredDoubleIterator) Position() int { return it.currPos }

// Close closes the iterator.
func (it *FilteredDoubleIterator) Close() {
	it.vit.Close()
	it.vit = nil
	it.f = nil
}
