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
		numTotalDocs             = 14
		docIDs                   []int32
		backingPositions         []int
		maskingPositions         []int
		expectedDocIDs           = []int32{2, 6, 13}
		expectedBackingPositions = []int{2, 6, 13}
		expectedMaskingPositions = []int{0, 1, 2}
	)
	it := newFullDocIDPositionIterator(int32(numTotalDocs), maskingIt)
	for it.Next() {
		docIDs = append(docIDs, it.DocID())
		backingPositions = append(backingPositions, it.Position())
		maskingPositions = append(maskingPositions, it.MaskingPosition())
	}
	require.Equal(t, expectedDocIDs, docIDs)
	require.Equal(t, expectedBackingPositions, backingPositions)
	require.Equal(t, expectedMaskingPositions, maskingPositions)
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
		numTotalDocs             = 13
		docIDs                   []int32
		backingPositions         []int
		maskingPositions         []int
		expectedDocIDs           = []int32{2, 6}
		expectedBackingPositions = []int{2, 6}
		expectedMaskingPositions = []int{0, 1}
	)
	it := newFullDocIDPositionIterator(int32(numTotalDocs), maskingIt)
	for it.Next() {
		docIDs = append(docIDs, it.DocID())
		backingPositions = append(backingPositions, it.Position())
		maskingPositions = append(maskingPositions, it.MaskingPosition())
	}
	require.Equal(t, expectedDocIDs, docIDs)
	require.Equal(t, expectedBackingPositions, backingPositions)
	require.Equal(t, expectedMaskingPositions, maskingPositions)
}
