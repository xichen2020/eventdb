package index

import (
	"testing"

	"github.com/xichen2020/eventdb/values/iterator"

	"github.com/golang/mock/gomock"
	"github.com/pilosa/pilosa/roaring"
	"github.com/stretchr/testify/require"
)

func TestAtPositionDocIDSetIterator(t *testing.T) {
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

	docIDSetIter := newbitmapBasedDocIDIterator(bm.Iterator())
	mockPositionIt := iterator.NewMockPositionIterator(ctrl)
	gomock.InOrder(
		mockPositionIt.EXPECT().Next().Return(true),
		mockPositionIt.EXPECT().Current().Return(2),
		mockPositionIt.EXPECT().Next().Return(true),
		mockPositionIt.EXPECT().Current().Return(4),
		mockPositionIt.EXPECT().Next().Return(true),
		mockPositionIt.EXPECT().Current().Return(7),
		mockPositionIt.EXPECT().Next().Return(false),
	)
	atPositionIt := NewAtPositionDocIDSetIterator(docIDSetIter, mockPositionIt)

	expected := []int32{5, 20, 90}
	var actual []int32
	for atPositionIt.Next() {
		actual = append(actual, atPositionIt.DocID())
	}
	require.Equal(t, expected, actual)
}
