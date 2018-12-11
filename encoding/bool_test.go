package encoding

import (
	"bytes"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func produceMockBoolData(data []bool, iter *MockForwardBoolIterator) {
	for _, s := range data {
		iter.EXPECT().Next().Return(true).Times(1)
		iter.EXPECT().Current().Return(s).Times(1)
	}
	iter.EXPECT().Next().Return(false).Times(1)
	iter.EXPECT().Err().Return(nil).Times(1)
}

func TestRunLengthEncodeAndDecodeBool(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	data := []bool{
		true,
		true,
		true,
		true,
		false,
		false,
		false,
		true,
		false,
		true,
		true,
	}

	mockIter := NewMockForwardBoolIterator(ctrl)
	// Call produce data twice since the first pass of encode captures metadata.
	produceMockBoolData(data, mockIter)

	var buf bytes.Buffer
	enc := NewBoolEncoder()
	err := enc.Encode(&buf, mockIter)
	require.Nil(t, err)

	dec := NewBoolDecoder()
	iter, err := dec.Decode(bytes.NewBuffer(buf.Bytes()))
	require.Nil(t, err)

	for idx := 0; iter.Next(); idx++ {
		require.Equal(t, data[idx], iter.Current())
	}
}
