package index

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestFullDocIDPositionIteratorWithinRange(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

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
		numTotalDocs      = 14
		docIDs            []int32
		positions         []int
		expectedDocIDs    = []int32{2, 6, 13}
		expectedPositions = []int{0, 1, 2}
	)
	it := newFullDocIDPositionIterator(int32(numTotalDocs), maskingIt)
	for it.Next() {
		docIDs = append(docIDs, it.DocID())
		positions = append(positions, it.Position())
	}
	require.Equal(t, expectedDocIDs, docIDs)
	require.Equal(t, expectedPositions, positions)
}

func TestFullDocIDPositionIteratorOutsideRange(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

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
		numTotalDocs      = 13
		docIDs            []int32
		positions         []int
		expectedDocIDs    = []int32{2, 6}
		expectedPositions = []int{0, 1}
	)
	it := newFullDocIDPositionIterator(int32(numTotalDocs), maskingIt)
	for it.Next() {
		docIDs = append(docIDs, it.DocID())
		positions = append(positions, it.Position())
	}
	require.Equal(t, expectedDocIDs, docIDs)
	require.Equal(t, expectedPositions, positions)
}
