package document

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestInAllDocIDSetIterIterator(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	it1 := NewMockDocIDSetIterator(ctrl)
	gomock.InOrder(
		it1.EXPECT().Next().Return(true),
		it1.EXPECT().DocID().Return(int32(12)),
		it1.EXPECT().Next().Return(true),
		it1.EXPECT().DocID().Return(int32(20)),
		it1.EXPECT().Next().Return(true),
		it1.EXPECT().DocID().Return(int32(24)),
		it1.EXPECT().Next().Return(true),
		it1.EXPECT().DocID().Return(int32(100)),
		it1.EXPECT().Next().Return(true).AnyTimes(),
	)

	it2 := NewMockDocIDSetIterator(ctrl)
	gomock.InOrder(
		it2.EXPECT().Next().Return(true),
		it2.EXPECT().DocID().Return(int32(20)),
		it2.EXPECT().Next().Return(true),
		it2.EXPECT().DocID().Return(int32(21)),
		it2.EXPECT().Next().Return(true),
		it2.EXPECT().DocID().Return(int32(24)),
		it2.EXPECT().Next().Return(true),
		it2.EXPECT().DocID().Return(int32(30)),
		it2.EXPECT().Next().Return(false).AnyTimes(),
	)

	it3 := NewMockDocIDSetIterator(ctrl)
	gomock.InOrder(
		it3.EXPECT().Next().Return(true),
		it3.EXPECT().DocID().Return(int32(15)),
		it3.EXPECT().Next().Return(true),
		it3.EXPECT().DocID().Return(int32(18)),
		it3.EXPECT().Next().Return(true),
		it3.EXPECT().DocID().Return(int32(20)),
		it3.EXPECT().Next().Return(true),
		it3.EXPECT().DocID().Return(int32(24)),
		it3.EXPECT().Next().Return(true),
		it3.EXPECT().DocID().Return(int32(38)),
		it3.EXPECT().Next().Return(false).AnyTimes(),
	)

	var docIDs []int32
	arr := []DocIDSetIterator{it1, it2, it3}
	it := newInAllDocIDSetIterIterator(arr...)
	for it.Next() {
		docIDs = append(docIDs, it.DocID())
	}
	require.Equal(t, []int32{20, 24}, docIDs)
}
