package template

import (
	"github.com/xichen2020/eventdb/index"

	xerrors "github.com/m3db/m3x/errors"
)

type valueFieldIterator struct {
	docIt index.DocIDSetIterator
	valIt ForwardValueIterator

	done      bool
	err       error
	currDocID int32
	currValue GenericValue
}

func newValueFieldIterator(
	docIt index.DocIDSetIterator,
	valIt ForwardValueIterator,
) *valueFieldIterator {
	return &valueFieldIterator{
		docIt: docIt,
		valIt: valIt,
	}
}

func (it *valueFieldIterator) Next() bool {
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

func (it *valueFieldIterator) DocID() int32 { return it.currDocID }

func (it *valueFieldIterator) Value() GenericValue { return it.currValue }

func (it *valueFieldIterator) Err() error { return it.err }

func (it *valueFieldIterator) Close() {
	it.docIt.Close()
	it.docIt = nil
	it.valIt.Close()
	it.valIt = nil
	it.err = nil
}
