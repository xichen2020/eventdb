package field

import (
	"testing"

	"github.com/xichen2020/eventdb/document/field"
	"github.com/xichen2020/eventdb/index"
	"github.com/xichen2020/eventdb/values/iterator"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestBytesFieldIterator(t *testing.T) {
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
		docIt.EXPECT().Err().Return(nil),
		docIt.EXPECT().Close(),
	)

	valsIt := iterator.NewMockForwardBytesIterator(ctrl)
	gomock.InOrder(
		valsIt.EXPECT().Next().Return(true),
		valsIt.EXPECT().Current().Return([]byte("a")),
		valsIt.EXPECT().Next().Return(true),
		valsIt.EXPECT().Current().Return([]byte("b")),
		valsIt.EXPECT().Next().Return(true),
		valsIt.EXPECT().Current().Return([]byte("c")),
		valsIt.EXPECT().Next().Return(true),
		valsIt.EXPECT().Current().Return([]byte("d")),
		valsIt.EXPECT().Next().Return(true),
		valsIt.EXPECT().Current().Return([]byte("e")),
		valsIt.EXPECT().Next().Return(false),
		valsIt.EXPECT().Err().Return(nil),
		valsIt.EXPECT().Close(),
	)

	it := newBytesFieldIterator(docIt, valsIt, field.NewBytesUnion)
	defer it.Close()

	var (
		expectedDocIDs = []int32{3, 4, 7, 10, 15}
		expectedVals   = [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e")}
		actualDocIDs   []int32
		actualVals     [][]byte
	)
	for it.Next() {
		actualDocIDs = append(actualDocIDs, it.DocID())
		actualVals = append(actualVals, it.Value().Data)
	}
	require.NoError(t, it.Err())
	require.Equal(t, expectedDocIDs, actualDocIDs)
	require.Equal(t, expectedVals, actualVals)
}
