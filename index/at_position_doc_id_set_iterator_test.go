package index

import (
	"testing"

	"github.com/xichen2020/eventdb/values/iterator"

	"github.com/golang/mock/gomock"
	"github.com/pilosa/pilosa/roaring"
	"github.com/stretchr/testify/require"
)

func TestAtPositionDocIDSetIteratorForwardOnly(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	bm := roaring.NewBitmap()
	bm.DirectAdd(2)
	bm.DirectAdd(5)
	bm.DirectAdd(7)
	bm.DirectAdd(20)
	bm.DirectAdd(54)
	bm.DirectAdd(89)
	bm.DirectAdd(90)
	bm.DirectAdd(107)

	docIDSetIter := newBitmapBasedDocIDIterator(bm.Iterator())
	mockPositionIt := iterator.NewMockPositionIterator(ctrl)
	gomock.InOrder(
		mockPositionIt.EXPECT().Next().Return(true),
		mockPositionIt.EXPECT().Position().Return(2),
		mockPositionIt.EXPECT().Next().Return(true),
		mockPositionIt.EXPECT().Position().Return(4),
		mockPositionIt.EXPECT().Next().Return(true),
		mockPositionIt.EXPECT().Position().Return(7),
		mockPositionIt.EXPECT().Next().Return(false),
		mockPositionIt.EXPECT().Err().Return(nil),
	)
	atPositionIt := NewAtPositionDocIDSetIterator(docIDSetIter, mockPositionIt)
	defer atPositionIt.Close()

	expected := []int32{7, 54, 107}
	var actual []int32
	for atPositionIt.Next() {
		actual = append(actual, atPositionIt.DocID())
	}
	require.NoError(t, atPositionIt.Err())
	require.Equal(t, expected, actual)
}

func TestAtPositionDocIDSetIteratorSeekable(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	docIDSetIter := NewMockSeekableDocIDSetIterator(ctrl)
	gomock.InOrder(
		docIDSetIter.EXPECT().Next().Return(true),
		docIDSetIter.EXPECT().SeekForward(2).Return(nil),
		docIDSetIter.EXPECT().DocID().Return(int32(7)),
		docIDSetIter.EXPECT().SeekForward(2).Return(nil),
		docIDSetIter.EXPECT().DocID().Return(int32(54)),
		docIDSetIter.EXPECT().SeekForward(3).Return(nil),
		docIDSetIter.EXPECT().DocID().Return(int32(107)),
		docIDSetIter.EXPECT().Close(),
	)

	mockPositionIt := iterator.NewMockPositionIterator(ctrl)
	gomock.InOrder(
		mockPositionIt.EXPECT().Next().Return(true),
		mockPositionIt.EXPECT().Position().Return(2),
		mockPositionIt.EXPECT().Next().Return(true),
		mockPositionIt.EXPECT().Position().Return(4),
		mockPositionIt.EXPECT().Next().Return(true),
		mockPositionIt.EXPECT().Position().Return(7),
		mockPositionIt.EXPECT().Next().Return(false),
		mockPositionIt.EXPECT().Err().Return(nil),
	)
	atPositionIt := NewAtPositionDocIDSetIterator(docIDSetIter, mockPositionIt)
	defer atPositionIt.Close()

	expected := []int32{7, 54, 107}
	var actual []int32
	for atPositionIt.Next() {
		actual = append(actual, atPositionIt.DocID())
	}
	require.NoError(t, atPositionIt.Err())
	require.Equal(t, expected, actual)
}
