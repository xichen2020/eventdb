package field

import (
	"testing"

	"github.com/xichen2020/eventdb/document/field"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestMultiFieldUnionIterator(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	it1 := NewMockBaseFieldIterator(ctrl)
	gomock.InOrder(
		it1.EXPECT().Next().Return(true),
		it1.EXPECT().DocID().Return(int32(12)).MinTimes(1),
		it1.EXPECT().ValueUnion().Return(field.ValueUnion{Type: field.BoolType, BoolVal: true}),
		it1.EXPECT().Next().Return(true),
		it1.EXPECT().DocID().Return(int32(20)).MinTimes(1),
		it1.EXPECT().ValueUnion().Return(field.ValueUnion{Type: field.BoolType, BoolVal: false}),
		it1.EXPECT().Next().Return(true),
		it1.EXPECT().DocID().Return(int32(24)).MinTimes(1),
		it1.EXPECT().ValueUnion().Return(field.ValueUnion{Type: field.BoolType, BoolVal: false}),
		it1.EXPECT().Next().Return(true),
		it1.EXPECT().DocID().Return(int32(100)).MinTimes(1),
		it1.EXPECT().ValueUnion().Return(field.ValueUnion{Type: field.BoolType, BoolVal: true}),
		it1.EXPECT().Next().Return(false).MinTimes(1),
		it1.EXPECT().Err().Return(nil),
		it1.EXPECT().Close(),
	)

	it2 := NewMockBaseFieldIterator(ctrl)
	gomock.InOrder(
		it2.EXPECT().Next().Return(true),
		it2.EXPECT().DocID().Return(int32(20)).MinTimes(1),
		it2.EXPECT().ValueUnion().Return(field.ValueUnion{Type: field.IntType, IntVal: 123}),
		it2.EXPECT().Next().Return(true),
		it2.EXPECT().DocID().Return(int32(21)).MinTimes(1),
		it2.EXPECT().ValueUnion().Return(field.ValueUnion{Type: field.IntType, IntVal: 234}),
		it2.EXPECT().Next().Return(true),
		it2.EXPECT().DocID().Return(int32(24)).MinTimes(1),
		it2.EXPECT().ValueUnion().Return(field.ValueUnion{Type: field.IntType, IntVal: 456}),
		it2.EXPECT().Next().Return(true),
		it2.EXPECT().DocID().Return(int32(30)).MinTimes(1),
		it2.EXPECT().ValueUnion().Return(field.ValueUnion{Type: field.IntType, IntVal: 567}),
		it2.EXPECT().Next().Return(false),
		it2.EXPECT().Err().Return(nil),
		it2.EXPECT().Close(),
	)

	it3 := NewMockBaseFieldIterator(ctrl)
	gomock.InOrder(
		it3.EXPECT().Next().Return(true),
		it3.EXPECT().DocID().Return(int32(15)).MinTimes(1),
		it3.EXPECT().ValueUnion().Return(field.ValueUnion{Type: field.BytesType, BytesVal: []byte("a")}),
		it3.EXPECT().Next().Return(true),
		it3.EXPECT().DocID().Return(int32(18)).MinTimes(1),
		it3.EXPECT().ValueUnion().Return(field.ValueUnion{Type: field.BytesType, BytesVal: []byte("b")}),
		it3.EXPECT().Next().Return(true),
		it3.EXPECT().DocID().Return(int32(20)).MinTimes(1),
		it3.EXPECT().ValueUnion().Return(field.ValueUnion{Type: field.BytesType, BytesVal: []byte("c")}),
		it3.EXPECT().Next().Return(true),
		it3.EXPECT().DocID().Return(int32(24)).MinTimes(1),
		it3.EXPECT().ValueUnion().Return(field.ValueUnion{Type: field.BytesType, BytesVal: []byte("d")}),
		it3.EXPECT().Next().Return(true),
		it3.EXPECT().DocID().Return(int32(38)).MinTimes(1),
		it3.EXPECT().ValueUnion().Return(field.ValueUnion{Type: field.BytesType, BytesVal: []byte("e")}),
		it3.EXPECT().Next().Return(false).MinTimes(1),
		it3.EXPECT().Err().Return(nil),
		it3.EXPECT().Close(),
	)

	it := NewMultiFieldUnionIterator([]BaseFieldIterator{it1, it2, it3})
	defer it.Close()

	expectedResults := []struct {
		docID     int32
		hasValues []bool
		values    []field.ValueUnion
	}{
		{
			docID:     12,
			hasValues: []bool{true, false, false},
			values: []field.ValueUnion{
				field.ValueUnion{Type: field.BoolType, BoolVal: true},
				field.ValueUnion{},
				field.ValueUnion{},
			},
		},
		{
			docID:     15,
			hasValues: []bool{false, false, true},
			values: []field.ValueUnion{
				field.ValueUnion{},
				field.ValueUnion{},
				field.ValueUnion{Type: field.BytesType, BytesVal: []byte("a")},
			},
		},
		{
			docID:     18,
			hasValues: []bool{false, false, true},
			values: []field.ValueUnion{
				field.ValueUnion{},
				field.ValueUnion{},
				field.ValueUnion{Type: field.BytesType, BytesVal: []byte("b")},
			},
		},
		{
			docID:     20,
			hasValues: []bool{true, true, true},
			values: []field.ValueUnion{
				field.ValueUnion{Type: field.BoolType, BoolVal: false},
				field.ValueUnion{Type: field.IntType, IntVal: 123},
				field.ValueUnion{Type: field.BytesType, BytesVal: []byte("c")},
			},
		},
		{
			docID:     21,
			hasValues: []bool{false, true, false},
			values: []field.ValueUnion{
				field.ValueUnion{},
				field.ValueUnion{Type: field.IntType, IntVal: 234},
				field.ValueUnion{},
			},
		},
		{
			docID:     24,
			hasValues: []bool{true, true, true},
			values: []field.ValueUnion{
				field.ValueUnion{Type: field.BoolType, BoolVal: false},
				field.ValueUnion{Type: field.IntType, IntVal: 456},
				field.ValueUnion{Type: field.BytesType, BytesVal: []byte("d")},
			},
		},
		{
			docID:     30,
			hasValues: []bool{false, true, false},
			values: []field.ValueUnion{
				field.ValueUnion{},
				field.ValueUnion{Type: field.IntType, IntVal: 567},
				field.ValueUnion{},
			},
		},
		{
			docID:     38,
			hasValues: []bool{false, false, true},
			values: []field.ValueUnion{
				field.ValueUnion{},
				field.ValueUnion{},
				field.ValueUnion{Type: field.BytesType, BytesVal: []byte("e")},
			},
		},
		{
			docID:     100,
			hasValues: []bool{true, false, false},
			values: []field.ValueUnion{
				field.ValueUnion{Type: field.BoolType, BoolVal: true},
				field.ValueUnion{},
				field.ValueUnion{},
			},
		},
	}

	currIdx := 0
	for it.Next() {
		require.Equal(t, expectedResults[currIdx].docID, it.DocID())
		vals, hasVals := it.Values()
		require.Equal(t, expectedResults[currIdx].hasValues, hasVals)
		for i := 0; i < len(hasVals); i++ {
			if hasVals[i] {
				require.Equal(t, expectedResults[currIdx].values[i], vals[i])
			}
		}
		currIdx++
	}
	require.NoError(t, it.Err())
	require.Equal(t, len(expectedResults), currIdx)
}
