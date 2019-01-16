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

package field

import "github.com/xichen2020/eventdb/values/iterator"
import (
	"github.com/xichen2020/eventdb/document/field"

	"github.com/xichen2020/eventdb/index"

	xerrors "github.com/m3db/m3x/errors"
)

type boolFieldIterator struct {
	docIt        index.DocIDSetIterator
	valIt        iterator.ForwardBoolIterator
	valAsUnionFn field.BoolAsUnionFn

	done      bool
	err       error
	currDocID int32
	currValue bool
}

func newBoolFieldIterator(
	docIt index.DocIDSetIterator,
	valIt iterator.ForwardBoolIterator,
	valAsUnionFn field.BoolAsUnionFn,
) *boolFieldIterator {
	return &boolFieldIterator{
		docIt:        docIt,
		valIt:        valIt,
		valAsUnionFn: valAsUnionFn,
	}
}

func (it *boolFieldIterator) Next() bool {
	if it.done || it.err != nil {
		return false
	}
	if !it.docIt.Next() || !it.valIt.Next() {
		it.done = true
		// TODO(xichen): Add doc it errors as well.
		var multiErr xerrors.MultiError
		if err := it.valIt.Err(); err != nil {
			multiErr = multiErr.Add(err)
		}
		it.err = multiErr.FinalError()
		return false
	}
	it.currDocID = it.docIt.DocID()
	it.currValue = it.valIt.Current()
	return true
}

func (it *boolFieldIterator) DocID() int32 { return it.currDocID }

func (it *boolFieldIterator) Value() bool { return it.currValue }

func (it *boolFieldIterator) ValueUnion() field.ValueUnion {
	// NB(xichen): This should be inlined.
	return it.valAsUnionFn(it.currValue)
}

func (it *boolFieldIterator) Err() error { return it.err }

func (it *boolFieldIterator) Close() {
	it.docIt.Close()
	it.docIt = nil
	it.valIt.Close()
	it.valIt = nil
	it.err = nil
}
