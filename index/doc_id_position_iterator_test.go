package index

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/golang/mock/gomock"
)

func TestDocIDPositionIteratorNoOverlap(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	backingIt := NewMockDocIDSetIterator(ctrl)
	gomock.InOrder(
		backingIt.EXPECT().Next().Return(true),
		backingIt.EXPECT().DocID().Return(int32(3)).MinTimes(1),
		backingIt.EXPECT().Next().Return(true),
		backingIt.EXPECT().DocID().Return(int32(4)).MinTimes(1),
		backingIt.EXPECT().Next().Return(true),
		backingIt.EXPECT().DocID().Return(int32(7)).MinTimes(1),
		backingIt.EXPECT().Next().Return(true),
		backingIt.EXPECT().DocID().Return(int32(10)).MinTimes(1),
		backingIt.EXPECT().Next().Return(true),
		backingIt.EXPECT().DocID().Return(int32(15)).MinTimes(1),
		backingIt.EXPECT().Next().Return(false).AnyTimes(),
		backingIt.EXPECT().Close(),
	)

	maskingIt := NewMockDocIDSetIterator(ctrl)
	gomock.InOrder(
		maskingIt.EXPECT().Next().Return(true),
		maskingIt.EXPECT().DocID().Return(int32(2)).MinTimes(1),
		maskingIt.EXPECT().Next().Return(true),
		maskingIt.EXPECT().DocID().Return(int32(6)).MinTimes(1),
		maskingIt.EXPECT().Next().Return(true),
		maskingIt.EXPECT().DocID().Return(int32(13)).MinTimes(1),
		maskingIt.EXPECT().Next().Return(false).AnyTimes(),
		maskingIt.EXPECT().Err().Return(nil),
		maskingIt.EXPECT().Close(),
	)

	var (
		docIDs           []int32
		backingPositions []int
		maskingPositions []int
	)
	it := NewDocIDPositionIterator(backingIt, maskingIt)
	defer it.Close()

	for it.Next() {
		docIDs = append(docIDs, it.DocID())
		backingPositions = append(backingPositions, it.Position())
		maskingPositions = append(maskingPositions, it.MaskingPosition())
	}
	require.NoError(t, it.Err())
	require.Equal(t, 0, len(docIDs))
	require.Equal(t, 0, len(backingPositions))
	require.Equal(t, 0, len(maskingPositions))
}

func TestDocIDPositionIteratorAllOverlap(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	backingIt := NewMockDocIDSetIterator(ctrl)
	gomock.InOrder(
		backingIt.EXPECT().Next().Return(true),
		backingIt.EXPECT().DocID().Return(int32(3)).MinTimes(1),
		backingIt.EXPECT().Next().Return(true),
		backingIt.EXPECT().DocID().Return(int32(4)).MinTimes(1),
		backingIt.EXPECT().Next().Return(true),
		backingIt.EXPECT().DocID().Return(int32(7)).MinTimes(1),
		backingIt.EXPECT().Next().Return(true),
		backingIt.EXPECT().DocID().Return(int32(10)).MinTimes(1),
		backingIt.EXPECT().Next().Return(true),
		backingIt.EXPECT().DocID().Return(int32(15)).MinTimes(1),
		backingIt.EXPECT().Next().Return(false).AnyTimes(),
		backingIt.EXPECT().Err().Return(nil),
		backingIt.EXPECT().Close(),
	)

	maskingIt := NewMockDocIDSetIterator(ctrl)
	gomock.InOrder(
		maskingIt.EXPECT().Next().Return(true),
		maskingIt.EXPECT().DocID().Return(int32(3)).MinTimes(1),
		maskingIt.EXPECT().Next().Return(true),
		maskingIt.EXPECT().DocID().Return(int32(4)).MinTimes(1),
		maskingIt.EXPECT().Next().Return(true),
		maskingIt.EXPECT().DocID().Return(int32(7)).MinTimes(1),
		maskingIt.EXPECT().Next().Return(true),
		maskingIt.EXPECT().DocID().Return(int32(10)).MinTimes(1),
		maskingIt.EXPECT().Next().Return(true),
		maskingIt.EXPECT().DocID().Return(int32(15)).MinTimes(1),
		maskingIt.EXPECT().Next().Return(false).AnyTimes(),
		maskingIt.EXPECT().Close(),
	)

	var (
		docIDs                   []int32
		backingPositions         []int
		maskingPositions         []int
		expectedDocIDs           = []int32{3, 4, 7, 10, 15}
		expectedBackingPositions = []int{0, 1, 2, 3, 4}
		expectedMaskingPositions = []int{0, 1, 2, 3, 4}
	)
	it := NewDocIDPositionIterator(backingIt, maskingIt)
	defer it.Close()

	for it.Next() {
		docIDs = append(docIDs, it.DocID())
		backingPositions = append(backingPositions, it.Position())
		maskingPositions = append(maskingPositions, it.MaskingPosition())
	}
	require.NoError(t, it.Err())
	require.Equal(t, expectedDocIDs, docIDs)
	require.Equal(t, expectedBackingPositions, backingPositions)
	require.Equal(t, expectedMaskingPositions, maskingPositions)
}

func TestDocIDPositionIteratorPartialOverlap(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	backingIt := NewMockDocIDSetIterator(ctrl)
	gomock.InOrder(
		backingIt.EXPECT().Next().Return(true),
		backingIt.EXPECT().DocID().Return(int32(3)).MinTimes(1),
		backingIt.EXPECT().Next().Return(true),
		backingIt.EXPECT().DocID().Return(int32(4)).MinTimes(1),
		backingIt.EXPECT().Next().Return(true),
		backingIt.EXPECT().DocID().Return(int32(7)).MinTimes(1),
		backingIt.EXPECT().Next().Return(true),
		backingIt.EXPECT().DocID().Return(int32(10)).MinTimes(1),
		backingIt.EXPECT().Next().Return(true),
		backingIt.EXPECT().DocID().Return(int32(15)).MinTimes(1),
		backingIt.EXPECT().Next().Return(false).AnyTimes(),
		backingIt.EXPECT().Close(),
	)

	maskingIt := NewMockDocIDSetIterator(ctrl)
	gomock.InOrder(
		maskingIt.EXPECT().Next().Return(true),
		maskingIt.EXPECT().DocID().Return(int32(3)).MinTimes(1),
		maskingIt.EXPECT().Next().Return(true),
		maskingIt.EXPECT().DocID().Return(int32(8)).MinTimes(1),
		maskingIt.EXPECT().Next().Return(true),
		maskingIt.EXPECT().DocID().Return(int32(10)).MinTimes(1),
		maskingIt.EXPECT().Next().Return(true),
		maskingIt.EXPECT().DocID().Return(int32(12)).MinTimes(1),
		maskingIt.EXPECT().Next().Return(false).AnyTimes(),
		maskingIt.EXPECT().Err().Return(nil),
		maskingIt.EXPECT().Close(),
	)

	var (
		docIDs                   []int32
		backingPositions         []int
		maskingPositions         []int
		expectedDocIDs           = []int32{3, 10}
		expectedBackingPositions = []int{0, 3}
		expectedMaskingPositions = []int{0, 2}
	)
	it := NewDocIDPositionIterator(backingIt, maskingIt)
	defer it.Close()

	for it.Next() {
		docIDs = append(docIDs, it.DocID())
		backingPositions = append(backingPositions, it.Position())
		maskingPositions = append(maskingPositions, it.MaskingPosition())
	}
	require.NoError(t, it.Err())
	require.Equal(t, expectedDocIDs, docIDs)
	require.Equal(t, expectedBackingPositions, backingPositions)
	require.Equal(t, expectedMaskingPositions, maskingPositions)
}
