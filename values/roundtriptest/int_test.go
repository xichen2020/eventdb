package roundtriptest

import (
	"bytes"
	"testing"

	"github.com/xichen2020/eventdb/values"
	"github.com/xichen2020/eventdb/values/decoding"
	"github.com/xichen2020/eventdb/values/encoding"
	"github.com/xichen2020/eventdb/values/iterator"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestPositiveIntDictionaryEncodeAndDecode(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	meta := values.IntValuesMetadata{
		Size: 512,
		Min:  1,
		Max:  1,
	}
	data := make([]int, meta.Size)
	for i := 0; i < meta.Size; i++ {
		data[i] = 1
	}
	iter1 := iterator.NewMockForwardIntIterator(ctrl)
	produceMockIntData(data, iter1)
	iter2 := iterator.NewMockForwardIntIterator(ctrl)
	produceMockIntData(data, iter2)

	vals := values.NewMockIntValues(ctrl)
	gomock.InOrder(
		vals.EXPECT().Metadata().Return(meta),
		vals.EXPECT().Iter().Return(iter1, nil),
		vals.EXPECT().Iter().Return(iter2, nil),
	)

	testEncodeAndDecodeInt(t, data, meta, vals)
}

func TestNegativeIntDictionaryEncodeAndDecode(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	meta := values.IntValuesMetadata{
		Size: 512,
		Min:  -1,
		Max:  -1,
	}
	data := make([]int, meta.Size)
	for i := 0; i < meta.Size; i++ {
		data[i] = -1
	}
	iter1 := iterator.NewMockForwardIntIterator(ctrl)
	produceMockIntData(data, iter1)
	iter2 := iterator.NewMockForwardIntIterator(ctrl)
	produceMockIntData(data, iter2)

	vals := values.NewMockIntValues(ctrl)
	gomock.InOrder(
		vals.EXPECT().Metadata().Return(meta),
		vals.EXPECT().Iter().Return(iter1, nil),
		vals.EXPECT().Iter().Return(iter2, nil),
	)

	testEncodeAndDecodeInt(t, data, meta, vals)
}

func TestPositiveIntDeltaEncodeAndDecode(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	meta := values.IntValuesMetadata{
		Size: 512,
		Min:  1,
		Max:  512,
	}
	data := make([]int, meta.Size)
	for i := 0; i < meta.Size; i++ {
		data[i] = i + 1
	}
	iter1 := iterator.NewMockForwardIntIterator(ctrl)
	gomock.InOrder(
		iter1.EXPECT().Next().Return(true),
		iter1.EXPECT().Current().Return(1),
		iter1.EXPECT().Next().Return(true),
		iter1.EXPECT().Current().Return(2),
		iter1.EXPECT().Next().Return(true),
		iter1.EXPECT().Current().Return(3),
		iter1.EXPECT().Err().Return(nil),
		iter1.EXPECT().Close(),
	)

	iter2 := iterator.NewMockForwardIntIterator(ctrl)
	produceMockIntData(data, iter2)

	vals := values.NewMockIntValues(ctrl)
	gomock.InOrder(
		vals.EXPECT().Metadata().Return(meta),
		vals.EXPECT().Iter().Return(iter1, nil),
		vals.EXPECT().Iter().Return(iter2, nil),
	)

	testEncodeAndDecodeInt(t, data, meta, vals)
}

func TestNegativeIntDeltaEncodeAndDecode(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	meta := values.IntValuesMetadata{
		Size: 512,
		Min:  -512,
		Max:  -1,
	}
	data := make([]int, meta.Size)
	for i := 0; i < meta.Size; i++ {
		data[i] = -(i + 1)
	}
	iter1 := iterator.NewMockForwardIntIterator(ctrl)
	gomock.InOrder(
		iter1.EXPECT().Next().Return(true),
		iter1.EXPECT().Current().Return(-1),
		iter1.EXPECT().Next().Return(true),
		iter1.EXPECT().Current().Return(-2),
		iter1.EXPECT().Next().Return(true),
		iter1.EXPECT().Current().Return(-3),
		iter1.EXPECT().Err().Return(nil),
		iter1.EXPECT().Close(),
	)

	iter2 := iterator.NewMockForwardIntIterator(ctrl)
	produceMockIntData(data, iter2)

	vals := values.NewMockIntValues(ctrl)
	gomock.InOrder(
		vals.EXPECT().Metadata().Return(meta),
		vals.EXPECT().Iter().Return(iter1, nil),
		vals.EXPECT().Iter().Return(iter2, nil),
	)

	testEncodeAndDecodeInt(t, data, meta, vals)
}

func TestMixedIntDictionaryEncodeAndDecode(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	meta := values.IntValuesMetadata{
		Size: 512,
		Min:  -1,
		Max:  1,
	}
	data := make([]int, meta.Size)
	for i := 0; i < meta.Size; i++ {
		if i%2 == 0 {
			data[i] = -1
		} else {
			data[i] = 1
		}
	}
	iter1 := iterator.NewMockForwardIntIterator(ctrl)
	produceMockIntData(data, iter1)
	iter2 := iterator.NewMockForwardIntIterator(ctrl)
	produceMockIntData(data, iter2)

	vals := values.NewMockIntValues(ctrl)
	gomock.InOrder(
		vals.EXPECT().Metadata().Return(meta),
		vals.EXPECT().Iter().Return(iter1, nil),
		vals.EXPECT().Iter().Return(iter2, nil),
	)

	testEncodeAndDecodeInt(t, data, meta, vals)
}

func TestMixedIntDeltaEncodeAndDecode(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	meta := values.IntValuesMetadata{
		Size: 512,
		Min:  -256,
		Max:  255,
	}
	data := make([]int, meta.Size)
	for i := 0; i < meta.Size; i++ {
		data[i] = i - 256
	}
	iter1 := iterator.NewMockForwardIntIterator(ctrl)
	gomock.InOrder(
		iter1.EXPECT().Next().Return(true),
		iter1.EXPECT().Current().Return(-1),
		iter1.EXPECT().Next().Return(true),
		iter1.EXPECT().Current().Return(-2),
		iter1.EXPECT().Next().Return(true),
		iter1.EXPECT().Current().Return(-3),
		iter1.EXPECT().Err().Return(nil),
		iter1.EXPECT().Close(),
	)

	iter2 := iterator.NewMockForwardIntIterator(ctrl)
	produceMockIntData(data, iter2)

	vals := values.NewMockIntValues(ctrl)
	gomock.InOrder(
		vals.EXPECT().Metadata().Return(meta),
		vals.EXPECT().Iter().Return(iter1, nil),
		vals.EXPECT().Iter().Return(iter2, nil),
	)

	testEncodeAndDecodeInt(t, data, meta, vals)
}

func produceMockIntData(data []int, iter *iterator.MockForwardIntIterator) {
	for _, s := range data {
		iter.EXPECT().Next().Return(true).Times(1)
		iter.EXPECT().Current().Return(s).Times(1)
	}
	iter.EXPECT().Next().Return(false).Times(1)
	iter.EXPECT().Err().Return(nil).Times(1)
	iter.EXPECT().Close()
}

// Ensure that encoding/decoding test data gives the same result.
func testEncodeAndDecodeInt(
	t *testing.T,
	data []int,
	meta values.IntValuesMetadata,
	vals *values.MockIntValues,
) {
	var buf bytes.Buffer
	enc := encoding.NewIntEncoder()
	require.NoError(t, enc.Encode(vals, &buf))

	dec := decoding.NewIntDecoder()
	intVals, err := dec.DecodeRaw(buf.Bytes())
	require.NoError(t, err)

	require.Equal(t, meta, intVals.Metadata())
	valsIt, err := intVals.Iter()
	require.NoError(t, err)
	count := 0
	for valsIt.Next() {
		require.Equal(t, data[count], valsIt.Current())
		count++
	}
	require.NoError(t, valsIt.Err())
	require.Equal(t, len(data), count)
}
