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

// Ensure that encoding/decoding test data gives the same result.
func ensureEncodeAndDecode(t *testing.T, data []int) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

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

func TestPositiveIntDictionaryEncodeAndDecode(t *testing.T) {
	data := make([]int, numUniqueInt)
	// we need > than max uniques to trigger default encoding
	for i := 0; i < numUniqueInt; i++ {
		data[i] = i
	}

	ensureEncodeAndDecode(t, data)
}

func TestPositiveIntDeltaEncodeAndDecode(t *testing.T) {
	// Force delta encoding condition by making max unique ints < num of unique ints.
	dictEncodingMaxCardinalityInt = 5

	data := make([]int, numUniqueInt)
	// we need > than max uniques to trigger default encoding
	for i := 0; i < numUniqueInt; i++ {
		data[i] = i
	}

	ensureEncodeAndDecode(t, data)
}

func TestNegativeIntDictionaryEncodeAndDecode(t *testing.T) {
	data := make([]int, numUniqueInt)
	// we need > than max uniques to trigger default encoding
	for i := 0; i < numUniqueInt; i++ {
		data[i] = -i
	}

	ensureEncodeAndDecode(t, data)
}

func TestNegativeIntDeltaEncodeAndDecode(t *testing.T) {
	// Force delta encoding condition by making max unique ints < num of unique ints.
	dictEncodingMaxCardinalityInt = 5

	data := make([]int, numUniqueInt)
	// we need > than max uniques to trigger default encoding
	for i := 0; i < numUniqueInt; i++ {
		data[i] = -i
	}

	ensureEncodeAndDecode(t, data)
}

func TestMixedIntDictionaryEncodeAndDecode(t *testing.T) {
	data := make([]int, numUniqueInt)
	// we need > than max uniques to trigger default encoding
	for i := 0; i < numUniqueInt; i++ {
		if i%2 == 0 {
			data[i] = -i
		} else {
			data[i] = i
		}
	}

	ensureEncodeAndDecode(t, data)
}

func TestMixedIntDeltaEncodeAndDecode(t *testing.T) {
	// Force delta encoding condition by making max unique ints < num of unique ints.
	dictEncodingMaxCardinalityInt = 5

	data := make([]int, numUniqueInt)
	// we need > than max uniques to trigger default encoding
	for i := 0; i < numUniqueInt; i++ {
		if i%2 == 0 {
			data[i] = -i
		} else {
			data[i] = i
		}
	}

	ensureEncodeAndDecode(t, data)
}
