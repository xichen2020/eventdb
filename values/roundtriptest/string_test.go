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

func TestStringDictionaryEncodeAndDecode(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	meta := values.StringValuesMetadata{
		Size: 128,
		Min:  "same string",
		Max:  "same string",
	}
	data := make([]string, meta.Size)
	for i := 0; i < meta.Size; i++ {
		data[i] = "same string"
	}
	iter1 := iterator.NewMockForwardStringIterator(ctrl)
	produceMockStringData(data, iter1)
	iter2 := iterator.NewMockForwardStringIterator(ctrl)
	produceMockStringData(data, iter2)

	vals := values.NewMockStringValues(ctrl)
	gomock.InOrder(
		vals.EXPECT().Metadata().Return(meta),
		vals.EXPECT().Iter().Return(iter1, nil),
		vals.EXPECT().Iter().Return(iter2, nil),
	)

	var buf bytes.Buffer
	enc := encoding.NewStringEncoder()
	require.NoError(t, enc.Encode(vals, &buf))
	require.True(t, buf.Len() < 70) // Encoded size should be much smaller than 128 * len("same string")

	dec := decoding.NewStringDecoder()
	strVals, err := dec.DecodeRaw(buf.Bytes())
	require.NoError(t, err)

	require.Equal(t, meta, strVals.Metadata())
	valsIt, err := strVals.Iter()
	require.NoError(t, err)
	count := 0
	for valsIt.Next() {
		require.Equal(t, data[count], valsIt.Current())
		count++
	}
	require.NoError(t, valsIt.Err())
	require.Equal(t, len(data), count)
}

func TestRawSizeEncodeAndDecode(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	meta := values.StringValuesMetadata{
		Size: 128,
		Min:  "unique string 0",
		Max:  "unique string 7",
	}
	data := make([]string, meta.Size)
	for i := 0; i < meta.Size; i++ {
		data[i] = fmt.Sprintf("unique string %d", i/2)
	}
	iter1 := iterator.NewMockForwardStringIterator(ctrl)
	gomock.InOrder(
		iter1.EXPECT().Next().Return(true),
		iter1.EXPECT().Current().Return("unique string 0"),
		iter1.EXPECT().Next().Return(true),
		iter1.EXPECT().Current().Return("unique string 0"),
		iter1.EXPECT().Next().Return(true),
		iter1.EXPECT().Current().Return("unique string 1"),
		iter1.EXPECT().Next().Return(true),
		iter1.EXPECT().Current().Return("unique string 1"),
		iter1.EXPECT().Next().Return(true),
		iter1.EXPECT().Current().Return("unique string 2"),
		iter1.EXPECT().Err().Return(nil),
		iter1.EXPECT().Close(),
	)

	iter2 := iterator.NewMockForwardStringIterator(ctrl)
	produceMockStringData(data, iter2)

	vals := values.NewMockStringValues(ctrl)
	gomock.InOrder(
		vals.EXPECT().Metadata().Return(meta),
		vals.EXPECT().Iter().Return(iter1, nil),
		vals.EXPECT().Iter().Return(iter2, nil),
	)

	var buf bytes.Buffer
	enc := encoding.NewStringEncoder()
	require.NoError(t, enc.Encode(vals, &buf))
	require.True(t, buf.Len() > 70) // Encoded size should be larger but not much larger due to compression

	dec := decoding.NewStringDecoder()
	strVals, err := dec.DecodeRaw(buf.Bytes())
	require.NoError(t, err)

	require.Equal(t, meta, strVals.Metadata())
	valsIt, err := strVals.Iter()
	require.NoError(t, err)
	count := 0
	for valsIt.Next() {
		require.Equal(t, data[count], valsIt.Current())
		count++
	}
	require.NoError(t, valsIt.Err())
	require.Equal(t, len(data), count)
}

func produceMockStringData(data []string, iter *iterator.MockForwardStringIterator) {
	for _, s := range data {
		iter.EXPECT().Next().Return(true).Times(1)
		iter.EXPECT().Current().Return(s).Times(1)
	}
	iter.EXPECT().Next().Return(false).Times(1)
	iter.EXPECT().Err().Return(nil).Times(1)
	iter.EXPECT().Close().Times(1)
}
