package encoding

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

const (
	maxBytesPerBlock = 100
	numUnique        = 1000
)

func produceMockData(data []string, iter *MockRewindableStringIterator) {
	for _, s := range data {
		iter.EXPECT().Next().Return(true).Times(1)
		iter.EXPECT().Current().Return(s).Times(1)
	}
	iter.EXPECT().Next().Return(false).Times(1)
	iter.EXPECT().Err().Return(nil).Times(1)
}

func ensureEncodeAndDecodeString(t *testing.T, opts StringEncoderOptions) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	data := make([]string, numUnique)
	// we need > than max uniques to trigger default encoding
	for i := 0; i < numUnique; i++ {
		data[i] = fmt.Sprintf("unique string #%d.", i)
	}

	mockIter := NewMockRewindableStringIterator(ctrl)
	// Call produce data twice since the first pass of encode captures metadata.
	produceMockData(data, mockIter)
	mockIter.EXPECT().Rewind().Return().Times(1)
	produceMockData(data, mockIter)

	var buf bytes.Buffer
	enc := NewStringEncoder()
	err := enc.Encode(&buf, mockIter, &opts)
	require.Nil(t, err)

	dec := NewStringDecoder()
	iter, err := dec.Decode(bytes.NewReader(buf.Bytes()))
	require.Nil(t, err)

	for idx := 0; iter.Next(); idx++ {
		require.Equal(t, data[idx], iter.Current())
	}
}

func TestDictionaryBlockEncodeAndDecode(t *testing.T) {
	ensureEncodeAndDecodeString(t, StringEncoderOptions{
		UseBlocks:        true,
		MaxBytesPerBlock: maxBytesPerBlock,
	})
}

func TestDictionaryNoBlockEncodeAndDecode(t *testing.T) {
	ensureEncodeAndDecodeString(t, StringEncoderOptions{
		UseBlocks: false,
	})
}

func TestLengthBlockEncodeAndDecode(t *testing.T) {
	// When max unique strings is below number of uniques, length encoding
	// is triggered.
	dictEncodingMaxCardinalityString = numUnique - 1

	ensureEncodeAndDecodeString(t, StringEncoderOptions{
		UseBlocks:        true,
		MaxBytesPerBlock: maxBytesPerBlock,
	})
}

func TestLengthNoBlockEncodeAndDecode(t *testing.T) {
	// When max unique strings is below number of uniques, length encoding
	// is triggered.
	dictEncodingMaxCardinalityString = numUnique - 1

	ensureEncodeAndDecodeString(t, StringEncoderOptions{
		UseBlocks: false,
	})
}
