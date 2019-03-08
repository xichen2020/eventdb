package roundtriptest

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/xichen2020/eventdb/values"
	"github.com/xichen2020/eventdb/values/decoding"
	"github.com/xichen2020/eventdb/values/encoding"
	"github.com/xichen2020/eventdb/values/iterator"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestBytesDictionaryEncodeAndDecode(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	meta := values.BytesValuesMetadata{
		Size: 128,
		Min:  []byte("same bytes"),
		Max:  []byte("same bytes"),
	}
	data := make([][]byte, meta.Size)
	for i := 0; i < meta.Size; i++ {
		data[i] = []byte("same bytes")
	}
	iter1 := iterator.NewMockForwardBytesIterator(ctrl)
	produceMockBytesData(data, iter1)
	iter2 := iterator.NewMockForwardBytesIterator(ctrl)
	produceMockBytesData(data, iter2)

	vals := values.NewMockBytesValues(ctrl)
	gomock.InOrder(
		vals.EXPECT().Metadata().Return(meta),
		vals.EXPECT().Iter().Return(iter1, nil),
		vals.EXPECT().Iter().Return(iter2, nil),
	)

	encodedBufSize := testEncodeAndDecodeBytes(t, data, meta, vals)
	require.True(t, encodedBufSize < 70) // Encoded size should be much smaller than 128 * len("same bytes")
}

func TestBytesEmptyDictionaryEncodeAndDecode(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		meta = values.BytesValuesMetadata{}
		data [][]byte
	)
	iter1 := iterator.NewMockForwardBytesIterator(ctrl)
	produceMockBytesData(data, iter1)
	iter2 := iterator.NewMockForwardBytesIterator(ctrl)
	produceMockBytesData(data, iter2)

	vals := values.NewMockBytesValues(ctrl)
	gomock.InOrder(
		vals.EXPECT().Metadata().Return(meta),
		vals.EXPECT().Iter().Return(iter1, nil),
		vals.EXPECT().Iter().Return(iter2, nil),
	)

	testEncodeAndDecodeBytes(t, data, meta, vals)
}

func TestRawSizeEncodeAndDecode(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	meta := values.BytesValuesMetadata{
		Size: 128,
		Min:  []byte("unique bytes 0"),
		Max:  []byte("unique bytes 7"),
	}
	data := make([][]byte, meta.Size)
	for i := 0; i < meta.Size; i++ {
		data[i] = []byte(fmt.Sprintf("unique bytes %d", i/2))
	}
	iter1 := iterator.NewMockForwardBytesIterator(ctrl)
	gomock.InOrder(
		iter1.EXPECT().Next().Return(true),
		iter1.EXPECT().Current().Return([]byte("unique bytes 0")),
		iter1.EXPECT().Next().Return(true),
		iter1.EXPECT().Current().Return([]byte("unique bytes 0")),
		iter1.EXPECT().Next().Return(true),
		iter1.EXPECT().Current().Return([]byte("unique bytes 1")),
		iter1.EXPECT().Next().Return(true),
		iter1.EXPECT().Current().Return([]byte("unique bytes 1")),
		iter1.EXPECT().Next().Return(true),
		iter1.EXPECT().Current().Return([]byte("unique bytes 2")),
		iter1.EXPECT().Err().Return(nil),
		iter1.EXPECT().Close(),
	)

	iter2 := iterator.NewMockForwardBytesIterator(ctrl)
	produceMockBytesData(data, iter2)

	vals := values.NewMockBytesValues(ctrl)
	gomock.InOrder(
		vals.EXPECT().Metadata().Return(meta),
		vals.EXPECT().Iter().Return(iter1, nil),
		vals.EXPECT().Iter().Return(iter2, nil),
	)

	encodedBufSize := testEncodeAndDecodeBytes(t, data, meta, vals)
	require.True(t, encodedBufSize > 70) // Encoded size should be larger but not much larger due to compression
}

func produceMockBytesData(data [][]byte, iter *iterator.MockForwardBytesIterator) {
	for _, s := range data {
		iter.EXPECT().Next().Return(true).Times(1)
		iter.EXPECT().Current().Return(s).Times(1)
	}
	iter.EXPECT().Next().Return(false).Times(1)
	iter.EXPECT().Err().Return(nil).Times(1)
	iter.EXPECT().Close().Times(1)
}

// Ensure that encoding/decoding test data gives the same result.
func testEncodeAndDecodeBytes(
	t *testing.T,
	data [][]byte,
	meta values.BytesValuesMetadata,
	vals *values.MockBytesValues,
) int {
	var buf bytes.Buffer
	enc := encoding.NewBytesEncoder()
	require.NoError(t, enc.Encode(vals, &buf))
	encodedBufSize := buf.Len()

	dec := decoding.NewBytesDecoder()
	bytesVals, err := dec.DecodeRaw(buf.Bytes())
	require.NoError(t, err)

	require.Equal(t, meta, bytesVals.Metadata())
	valsIt, err := bytesVals.Iter()
	require.NoError(t, err)
	count := 0
	for valsIt.Next() {
		require.Equal(t, data[count], valsIt.Current())
		count++
	}
	require.NoError(t, valsIt.Err())
	require.Equal(t, len(data), count)
	return encodedBufSize
}
