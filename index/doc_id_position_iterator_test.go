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
	)

	var (
		docIDs    []int32
		positions []int
	)
	it := NewDocIDPositionIterator(backingIt, maskingIt)
	for it.Next() {
		docIDs = append(docIDs, it.DocID())
		positions = append(positions, it.Position())
	}
	require.Equal(t, 0, len(docIDs))
	require.Equal(t, 0, len(positions))
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
	)

	var (
		docIDs            []int32
		positions         []int
		expectedDocIDs    = []int32{3, 4, 7, 10, 15}
		expectedPositions = []int{0, 1, 2, 3, 4}
	)
	it := NewDocIDPositionIterator(backingIt, maskingIt)
	for it.Next() {
		docIDs = append(docIDs, it.DocID())
		positions = append(positions, it.Position())
	}
	require.Equal(t, expectedDocIDs, docIDs)
	require.Equal(t, expectedPositions, positions)
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
	)

	var (
		docIDs            []int32
		positions         []int
		expectedDocIDs    = []int32{3, 10}
		expectedPositions = []int{0, 3}
	)
	it := NewDocIDPositionIterator(backingIt, maskingIt)
	for it.Next() {
		docIDs = append(docIDs, it.DocID())
		positions = append(positions, it.Position())
	}
	require.Equal(t, expectedDocIDs, docIDs)
	require.Equal(t, expectedPositions, positions)
}
