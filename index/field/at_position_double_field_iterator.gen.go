// This file was automatically generated by genny.
// Any changes will be lost if this file is regenerated.
// see https://github.com/mauricelam/genny

package field

import "github.com/xichen2020/eventdb/values/iterator"
import (
	"errors"

	"github.com/xichen2020/eventdb/document/field"

	"github.com/xichen2020/eventdb/index"
)

var (
	errPositionIterDoubleIterCountMismatch = errors.New("value iterator and the doc ID position iterator count mismatch")
)

type atPositionDoubleFieldIterator struct {
	docIDPosIt     index.DocIDPositionIterator
	valsIt         iterator.ForwardDoubleIterator
	seekableValsIt iterator.SeekableDoubleIterator
	valAsUnionFn   field.DoubleAsUnionFn

	done           bool
	err            error
	firstTime      bool
	currPos        int
	currMaskingPos int
	currDocID      int32
	currVal        float64
}

func newAtPositionDoubleFieldIterator(
	docIDPosIt index.DocIDPositionIterator,
	valsIt iterator.ForwardDoubleIterator,
	valAsUnionFn field.DoubleAsUnionFn,
) *atPositionDoubleFieldIterator {
	seekableValsIt, _ := valsIt.(iterator.SeekableDoubleIterator)
	if seekableValsIt != nil {
		valsIt = nil
	}
	return &atPositionDoubleFieldIterator{
		docIDPosIt:     docIDPosIt,
		valsIt:         valsIt,
		valAsUnionFn:   valAsUnionFn,
		seekableValsIt: seekableValsIt,
		firstTime:      true,
	}
}

func (it *atPositionDoubleFieldIterator) Next() bool {
	if it.done || it.err != nil {
		return false
	}
	if !it.docIDPosIt.Next() {
		it.done = true
		it.err = it.docIDPosIt.Err()
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
			it.err = errPositionIterDoubleIterCountMismatch
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
				it.err = errPositionIterDoubleIterCountMismatch
				return false
			}
		}
		it.currVal = it.valsIt.Current()
	}

	it.currPos = nextPos
	it.currMaskingPos = it.docIDPosIt.MaskingPosition()
	it.currDocID = it.docIDPosIt.DocID()
	return true
}

func (it *atPositionDoubleFieldIterator) DocID() int32 { return it.currDocID }

func (it *atPositionDoubleFieldIterator) Value() float64 { return it.currVal }

func (it *atPositionDoubleFieldIterator) ValueUnion() field.ValueUnion {
	return it.valAsUnionFn(it.currVal)
}

func (it *atPositionDoubleFieldIterator) MaskingPosition() int { return it.currMaskingPos }

func (it *atPositionDoubleFieldIterator) Err() error { return it.err }

func (it *atPositionDoubleFieldIterator) Close() {
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
