package field

import (
	"testing"

	"github.com/xichen2020/eventdb/document/field"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestMultiFieldIntersectIterator(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

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

	it3 := NewMockBaseFieldIterator(ctrl)
	gomock.InOrder(
		it3.EXPECT().Next().Return(true),
		it3.EXPECT().DocID().Return(int32(15)),
		it3.EXPECT().Next().Return(true),
		it3.EXPECT().DocID().Return(int32(18)),
		it3.EXPECT().Next().Return(true),
		it3.EXPECT().DocID().Return(int32(20)),
		it3.EXPECT().ValueUnion().Return(field.ValueUnion{Type: field.BytesType, BytesVal: []byte("foo")}),
		it3.EXPECT().Next().Return(true),
		it3.EXPECT().DocID().Return(int32(24)),
		it3.EXPECT().ValueUnion().Return(field.ValueUnion{Type: field.BytesType, BytesVal: []byte("bar")}),
		it3.EXPECT().Next().Return(true),
		it3.EXPECT().DocID().Return(int32(38)),
		it3.EXPECT().Close(),
	)

	expectedDocIDs := []int32{20, 24}
	expectedVals := [][]field.ValueUnion{
		{
			field.ValueUnion{Type: field.BoolType, BoolVal: true},
			field.ValueUnion{Type: field.IntType, IntVal: 123},
			field.ValueUnion{Type: field.BytesType, BytesVal: []byte("foo")},
		},
		{
			field.ValueUnion{Type: field.BoolType, BoolVal: false},
			field.ValueUnion{Type: field.IntType, IntVal: 456},
			field.ValueUnion{Type: field.BytesType, BytesVal: []byte("bar")},
		},
	}
	var (
		actualDocIDs []int32
		actualVals   [][]field.ValueUnion
	)

	it := NewMultiFieldIntersectIterator([]BaseFieldIterator{it1, it2, it3})
	defer it.Close()

	for it.Next() {
		actualDocIDs = append(actualDocIDs, it.DocID())
		values := it.Values()
		clone := make([]field.ValueUnion, len(values))
		copy(clone, values)
		actualVals = append(actualVals, clone)
	}
	require.NoError(t, it.Err())
	require.Equal(t, expectedDocIDs, actualDocIDs)
	require.Equal(t, expectedVals, actualVals)
}
