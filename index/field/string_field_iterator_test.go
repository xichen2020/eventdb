package field

import (
	"testing"

	"github.com/xichen2020/eventdb/index"
	"github.com/xichen2020/eventdb/values/iterator"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestStringFieldIterator(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	docIt := index.NewMockDocIDSetIterator(ctrl)
	gomock.InOrder(
		docIt.EXPECT().Next().Return(true),
		docIt.EXPECT().DocID().Return(int32(3)),
		docIt.EXPECT().Next().Return(true),
		docIt.EXPECT().DocID().Return(int32(4)),
		docIt.EXPECT().Next().Return(true),
		docIt.EXPECT().DocID().Return(int32(7)),
		docIt.EXPECT().Next().Return(true),
		docIt.EXPECT().DocID().Return(int32(10)),
		docIt.EXPECT().Next().Return(true),
		docIt.EXPECT().DocID().Return(int32(15)),
		docIt.EXPECT().Next().Return(false).AnyTimes(),
		docIt.EXPECT().Close(),
	)

	valsIt := iterator.NewMockForwardStringIterator(ctrl)
	gomock.InOrder(
		valsIt.EXPECT().Next().Return(true),
		valsIt.EXPECT().Current().Return("a"),
		valsIt.EXPECT().Next().Return(true),
		valsIt.EXPECT().Current().Return("b"),
		valsIt.EXPECT().Next().Return(true),
		valsIt.EXPECT().Current().Return("c"),
		valsIt.EXPECT().Next().Return(true),
		valsIt.EXPECT().Current().Return("d"),
		valsIt.EXPECT().Next().Return(true),
		valsIt.EXPECT().Current().Return("e"),
		valsIt.EXPECT().Err().Return(nil),
		valsIt.EXPECT().Close(),
	)

	it := newStringFieldIterator(docIt, valsIt)
	defer it.Close()

	var (
		expectedDocIDs = []int32{3, 4, 7, 10, 15}
		expectedVals   = []string{"a", "b", "c", "d", "e"}
		actualDocIDs   []int32
		actualVals     []string
	)
	for it.Next() {
		actualDocIDs = append(actualDocIDs, it.DocID())
		actualVals = append(actualVals, it.Value())
	}
	require.NoError(t, it.Err())
	require.Equal(t, expectedDocIDs, actualDocIDs)
	require.Equal(t, expectedVals, actualVals)
}
