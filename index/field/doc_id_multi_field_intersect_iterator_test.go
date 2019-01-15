package field

import (
	"testing"

	"github.com/xichen2020/eventdb/document/field"
	"github.com/xichen2020/eventdb/index"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestDocIDMultiFieldIntersectIterator(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	docIt := index.NewMockDocIDSetIterator(ctrl)
	gomock.InOrder(
		docIt.EXPECT().Next().Return(true),
		docIt.EXPECT().DocID().Return(int32(12)),
		docIt.EXPECT().Next().Return(true),
		docIt.EXPECT().DocID().Return(int32(20)),
		docIt.EXPECT().Next().Return(true),
		docIt.EXPECT().DocID().Return(int32(24)),
		docIt.EXPECT().Next().Return(true),
		docIt.EXPECT().DocID().Return(int32(100)),
		docIt.EXPECT().Close(),
	)

	it1 := NewMockBaseFieldIterator(ctrl)
	gomock.InOrder(
		it1.EXPECT().Next().Return(true),
		it1.EXPECT().DocID().Return(int32(12)),
		it1.EXPECT().Next().Return(true),
		it1.EXPECT().DocID().Return(int32(20)),
		it1.EXPECT().ValueUnion().Return(field.ValueUnion{Type: field.BoolType, BoolVal: true}),
		it1.EXPECT().Next().Return(true),
		it1.EXPECT().DocID().Return(int32(24)),
		it1.EXPECT().ValueUnion().Return(field.ValueUnion{Type: field.BoolType, BoolVal: false}),
		it1.EXPECT().Next().Return(true),
		it1.EXPECT().DocID().Return(int32(100)),
		it1.EXPECT().Close(),
	)

	it2 := NewMockBaseFieldIterator(ctrl)
	gomock.InOrder(
		it2.EXPECT().Next().Return(true),
		it2.EXPECT().DocID().Return(int32(20)),
		it2.EXPECT().ValueUnion().Return(field.ValueUnion{Type: field.IntType, IntVal: 123}),
		it2.EXPECT().Next().Return(true),
		it2.EXPECT().DocID().Return(int32(21)),
		it2.EXPECT().Next().Return(true),
		it2.EXPECT().DocID().Return(int32(24)),
		it2.EXPECT().ValueUnion().Return(field.ValueUnion{Type: field.IntType, IntVal: 456}),
		it2.EXPECT().Next().Return(true),
		it2.EXPECT().DocID().Return(int32(30)),
		it2.EXPECT().Next().Return(false),
		it2.EXPECT().Err().Return(nil),
		it2.EXPECT().Close(),
	)
	multiFieldIt := NewMultiFieldIntersectIterator([]BaseFieldIterator{it1, it2})
	intersectIt := NewDocIDMultiFieldIntersectIterator(docIt, multiFieldIt)
	defer intersectIt.Close()

	expectedDocIDs := []int32{20, 24}
	expectedVals := [][]field.ValueUnion{
		{
			field.ValueUnion{Type: field.BoolType, BoolVal: true},
			field.ValueUnion{Type: field.IntType, IntVal: 123},
		},
		{
			field.ValueUnion{Type: field.BoolType, BoolVal: false},
			field.ValueUnion{Type: field.IntType, IntVal: 456},
		},
	}
	var (
		actualDocIDs []int32
		actualVals   [][]field.ValueUnion
	)
	for intersectIt.Next() {
		actualDocIDs = append(actualDocIDs, intersectIt.DocID())
		values := make([]field.ValueUnion, len(intersectIt.Values()))
		copy(values, intersectIt.Values())
		actualVals = append(actualVals, values)
	}
	require.NoError(t, intersectIt.Err())
	require.Equal(t, expectedDocIDs, actualDocIDs)
	require.Equal(t, expectedVals, actualVals)
}
