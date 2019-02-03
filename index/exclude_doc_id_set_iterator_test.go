package index

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestExcludeDocIDSetIterator(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	toExclude := NewMockDocIDSetIterator(ctrl)
	gomock.InOrder(
		toExclude.EXPECT().Next().Return(true),
		toExclude.EXPECT().DocID().Return(int32(3)).MinTimes(1),
		toExclude.EXPECT().Next().Return(true),
		toExclude.EXPECT().DocID().Return(int32(4)).MinTimes(1),
		toExclude.EXPECT().Next().Return(true),
		toExclude.EXPECT().DocID().Return(int32(7)).MinTimes(1),
		toExclude.EXPECT().Next().Return(true),
		toExclude.EXPECT().DocID().Return(int32(10)).MinTimes(1),
		toExclude.EXPECT().Next().Return(false).AnyTimes(),
		toExclude.EXPECT().Err().Return(nil),
		toExclude.EXPECT().Close(),
	)

	var docIDs []int32
	it := NewExcludeDocIDSetIterator(11, toExclude)
	defer it.Close()

	for it.Next() {
		docIDs = append(docIDs, it.DocID())
	}
	require.NoError(t, it.Err())
	require.Equal(t, []int32{0, 1, 2, 5, 6, 8, 9}, docIDs)
}
