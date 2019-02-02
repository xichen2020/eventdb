package template

import (
	"errors"

	"github.com/xichen2020/eventdb/document/field"
	"github.com/xichen2020/eventdb/index"

	xerrors "github.com/m3db/m3x/errors"
)

type valueFieldIterator struct {
	docIt        index.DocIDSetIterator
	valIt        ForwardValueIterator
	valAsUnionFn valueAsUnionFn

	done      bool
	err       error
	currDocID int32
	currValue GenericValue
}

func newValueFieldIterator(
	docIt index.DocIDSetIterator,
	valIt ForwardValueIterator,
	valAsUnionFn valueAsUnionFn,
) *valueFieldIterator {
	return &valueFieldIterator{
		docIt:        docIt,
		valIt:        valIt,
		valAsUnionFn: valAsUnionFn,
	}
}

func (it *valueFieldIterator) Next() bool {
	if it.done || it.err != nil {
		return false
	}
	if !it.docIt.Next() {
		it.done = true
		var (
			multiErr xerrors.MultiError
			docItErr = it.docIt.Err()
		)
		if !it.valIt.Next() {
			multiErr = multiErr.Add(docItErr)
			multiErr = multiErr.Add(it.valIt.Err())
		} else if docItErr != nil {
			multiErr = multiErr.Add(docItErr)
		} else {
			multiErr = multiErr.Add(errors.New("field iterator doc id iteration done but value iteration is not"))
		}
		it.err = multiErr.FinalError()
		return false
	}
	if !it.valIt.Next() {
		it.done = true
		var (
			multiErr xerrors.MultiError
			valItErr = it.valIt.Err()
		)
		if valItErr != nil {
			multiErr = multiErr.Add(valItErr)
		} else {
			multiErr = multiErr.Add(errors.New("field iterator value iteration done but doc id iteration is not"))
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

func (it *valueFieldIterator) ValueUnion() field.ValueUnion {
	// NB(xichen): This should be inlined.
	return it.valAsUnionFn(it.currValue)
}

func (it *valueFieldIterator) Err() error { return it.err }

func (it *valueFieldIterator) Close() {
	it.docIt.Close()
	it.docIt = nil
	it.valIt.Close()
	it.valIt = nil
	it.err = nil
}
