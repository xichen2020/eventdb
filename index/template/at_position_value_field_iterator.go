package template

import (
	"errors"

	"github.com/xichen2020/eventdb/index"
)

var (
	errPositionIterValueIterCountMismatch = errors.New("value iterator and the doc ID position iterator count mismatch")
)

type atPositionValueFieldIterator struct {
	docIDPosIt     index.DocIDPositionIterator
	valsIt         ForwardValueIterator
	seekableValsIt SeekableValueIterator

	done      bool
	err       error
	firstTime bool
	currPos   int
	currDocID int32
	currVal   GenericValue
}

func newAtPositionValueFieldIterator(
	docIDPosIt index.DocIDPositionIterator,
	valsIt ForwardValueIterator,
) *atPositionValueFieldIterator {
	seekableValsIt, _ := valsIt.(SeekableValueIterator)
	if seekableValsIt != nil {
		valsIt = nil
	}
	return &atPositionValueFieldIterator{
		docIDPosIt:     docIDPosIt,
		valsIt:         valsIt,
		seekableValsIt: seekableValsIt,
		firstTime:      true,
	}
}

func (it *atPositionValueFieldIterator) Next() bool {
	if it.done || it.err != nil {
		return false
	}
	if !it.docIDPosIt.Next() {
		it.done = true
		return false
	}
	nextPos := it.docIDPosIt.Position()
	distance := nextPos - it.currPos

	// We have a next position, now advance the values iterator for the first time.
	if it.firstTime {
		it.firstTime = false
		if hasNoValues :=
			(it.seekableValsIt != nil && !it.seekableValsIt.Next()) ||
				(it.valsIt != nil && !it.valsIt.Next()); hasNoValues {
			it.err = errPositionIterValueIterCountMismatch
			return false
		}
	}

	if it.seekableValsIt != nil {
		if it.err = it.seekableValsIt.SeekForward(distance); it.err != nil {
			return false
		}
		it.currVal = it.seekableValsIt.Current()
	} else {
		for i := 0; i < distance; i++ {
			if !it.valsIt.Next() {
				it.err = errPositionIterValueIterCountMismatch
				return false
			}
		}
		it.currVal = it.valsIt.Current()
	}

	it.currPos = nextPos
	it.currDocID = it.docIDPosIt.DocID()
	return true
}

func (it *atPositionValueFieldIterator) DocID() int32 { return it.currDocID }

func (it *atPositionValueFieldIterator) Value() GenericValue { return it.currVal }

func (it *atPositionValueFieldIterator) Err() error { return it.err }

func (it *atPositionValueFieldIterator) Close() {
	it.docIDPosIt.Close()
	it.docIDPosIt = nil
	if it.valsIt != nil {
		it.valsIt.Close()
		it.valsIt = nil
	} else {
		it.seekableValsIt.Close()
		it.seekableValsIt = nil
	}
	it.err = nil
}
