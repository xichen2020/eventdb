package encoding

import (
	"bytes"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

const (
	numUniqueInt = 10
)

func produceMockIntData(data []int, iter *MockRewindableIntIterator) {
	for _, s := range data {
		iter.EXPECT().Next().Return(true).Times(1)
		iter.EXPECT().Current().Return(s).Times(1)
	}
	iter.EXPECT().Next().Return(false).Times(1)
	iter.EXPECT().Err().Return(nil).Times(1)
}

func TestIntDictionaryEncodeAndDecode(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	data := make([]int, numUniqueInt)
	// we need > than max uniques to trigger default encoding
	for i := 0; i < numUniqueInt; i++ {
		data[i] = i
	}

	mockIter := NewMockRewindableIntIterator(ctrl)
	// Call produce data twice since the first pass of encode captures metadata.
	produceMockIntData(data, mockIter)
	mockIter.EXPECT().Rewind().Return().Times(1)
	produceMockIntData(data, mockIter)

	var buf bytes.Buffer

	enc := NewIntEncoder()
	err := enc.Encode(&buf, mockIter)
	require.Nil(t, err)

	dec := NewIntDecoder()
	iter, err := dec.Decode(bytes.NewBuffer(buf.Bytes()))
	require.Nil(t, err)

	for idx := 0; iter.Next(); idx++ {
		require.Equal(t, data[idx], iter.Current())
	}
}

func TestIntDeltaEncodeAndDecode(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	data := make([]int, numUniqueInt)
	// we need > than max uniques to trigger default encoding
	for i := 0; i < numUniqueInt; i++ {
		data[i] = i
	}

	mockIter := NewMockRewindableIntIterator(ctrl)
	// Call produce data twice since the first pass of encode captures metadata.
	produceMockIntData(data, mockIter)
	mockIter.EXPECT().Rewind().Return().Times(1)
	produceMockIntData(data, mockIter)

	// Force delta encoding condition by making max unique ints < num of unique ints.
	dictEncodingMaxCardinalityInt = 5

	var buf bytes.Buffer

	enc := NewIntEncoder()
	err := enc.Encode(&buf, mockIter)
	require.Nil(t, err)

	dec := NewIntDecoder()
	iter, err := dec.Decode(bytes.NewBuffer(buf.Bytes()))
	require.Nil(t, err)

	for idx := 0; iter.Next(); idx++ {
		require.Equal(t, data[idx], iter.Current())
	}
}
