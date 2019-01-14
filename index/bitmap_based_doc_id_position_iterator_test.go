package index

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/pilosa/pilosa/roaring"
	"github.com/stretchr/testify/require"
)

func TestBitmapBasedDocIDPositionIterator(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	bm := roaring.NewBitmap(0, 3, 5, 9, 12, 19, 23)

	maskingIt := NewMockDocIDSetIterator(ctrl)
	gomock.InOrder(
		maskingIt.EXPECT().Next().Return(true),
		maskingIt.EXPECT().DocID().Return(int32(0)).MinTimes(1),
		maskingIt.EXPECT().Next().Return(true),
		maskingIt.EXPECT().DocID().Return(int32(7)).MinTimes(1),
		maskingIt.EXPECT().Next().Return(true),
		maskingIt.EXPECT().DocID().Return(int32(12)).MinTimes(1),
		maskingIt.EXPECT().Next().Return(false).AnyTimes(),
	)

	var (
		docIDs                   []int32
		backingPositions         []int
		maskingPositions         []int
		expectedDocIDs           = []int32{0, 12}
		expectedBackingPositions = []int{0, 4}
		expectedMaskingPositions = []int{0, 2}
	)
	it := newBitmapBasedDocIDPositionIterator(bm, maskingIt)
	for it.Next() {
		docIDs = append(docIDs, it.DocID())
		backingPositions = append(backingPositions, it.Position())
		maskingPositions = append(maskingPositions, it.MaskingPosition())
	}
	require.Equal(t, expectedDocIDs, docIDs)
	require.Equal(t, expectedBackingPositions, backingPositions)
	require.Equal(t, expectedMaskingPositions, maskingPositions)
}
