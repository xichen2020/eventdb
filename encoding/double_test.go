package encoding

import (
	"bytes"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

const (
	numUniqueDouble = 10
)

func produceMockDoubleData(data []float64, iter *MockForwardDoubleIterator) {
	for _, s := range data {
		iter.EXPECT().Next().Return(true).Times(1)
		iter.EXPECT().Current().Return(s).Times(1)
	}
	iter.EXPECT().Next().Return(false).Times(1)
	iter.EXPECT().Err().Return(nil).Times(1)
}

func ensureEncodeAndDecodeDouble(t *testing.T, data []float64) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockIter := NewMockForwardDoubleIterator(ctrl)
	// Call produce data twice since the first pass of encode captures metadata.
	produceMockDoubleData(data, mockIter)

	var buf bytes.Buffer

	enc := NewDoubleEncoder()
	err := enc.Encode(&buf, mockIter)
	require.Nil(t, err)

	dec := NewDoubleDecoder()
	iter, err := dec.Decode(bytes.NewBuffer(buf.Bytes()))
	require.Nil(t, err)

	for idx := 0; iter.Next(); idx++ {
		require.Equal(t, data[idx], iter.Current())
	}
}

func TestPositiveDoubleEncodeAndDecode(t *testing.T) {
	base := float64(2.301239120291)
	data := make([]float64, numUniqueDouble)
	// we need > than max uniques to trigger default encoding
	for i := 0; i < numUniqueDouble; i++ {
		data[i] = float64(i) * base
	}

	ensureEncodeAndDecodeDouble(t, data)
}

func TestNegativeDoubleEncodeAndDecode(t *testing.T) {
	base := float64(-8.231332132)
	data := make([]float64, numUniqueDouble)
	// we need > than max uniques to trigger default encoding
	for i := 0; i < numUniqueDouble; i++ {
		data[i] = float64(i) * base
	}

	ensureEncodeAndDecodeDouble(t, data)
}
