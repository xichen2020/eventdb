package field

import (
	"github.com/xichen2020/eventdb/document/field"
	"github.com/xichen2020/eventdb/index"
)

type nullFieldIterator struct {
	docIt index.DocIDSetIterator

	done      bool
	err       error
	currDocID int32
}

func newNullFieldIterator(
	docIt index.DocIDSetIterator,
) *nullFieldIterator {
	return &nullFieldIterator{
		docIt: docIt,
	}
}

func (it *nullFieldIterator) Next() bool {
	if it.done || it.err != nil {
		return false
	}
	if !it.docIt.Next() {
		it.done = true
		// TODO(xichen): Add doc it errors as well.
		return false
	}
	it.currDocID = it.docIt.DocID()
	return true
}

func (it *nullFieldIterator) DocID() int32 { return it.currDocID }

func (it *nullFieldIterator) ValueUnion() field.ValueUnion { return field.NullUnion }

func (it *nullFieldIterator) Err() error { return it.err }

func (it *nullFieldIterator) Close() {
	it.docIt.Close()
	it.docIt = nil
	it.err = nil
}

type atPositionNullFieldIterator struct {
	*nullFieldIterator

	docIDPosIt index.DocIDPositionIterator
}

func newAtPositionNullFieldIterator(
	docIDPosIt index.DocIDPositionIterator,
) *atPositionNullFieldIterator {
	return &atPositionNullFieldIterator{
		nullFieldIterator: newNullFieldIterator(docIDPosIt),
		docIDPosIt:        docIDPosIt,
	}
}

func (it *atPositionNullFieldIterator) MaskingPosition() int {
	return it.docIDPosIt.MaskingPosition()
}
