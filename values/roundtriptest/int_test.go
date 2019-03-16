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

// Outliers are used to trigger/test Varint encoding.
const (
	mockDataOutlierMax = 1 << 32
	mockDataOutlierMin = -(1 << 32)
)

func TestPositiveIntVarintEncodeAndDecode(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	meta := values.IntValuesMetadata{
		Size: 11,
		Min:  0,
		Max:  mockDataOutlierMax,
	}
	data := make([]int, meta.Size)
	for i := 0; i < meta.Size; i++ {
		data[i] = i
	}
	// Last element is an outlier.
	data[len(data)-1] = mockDataOutlierMax

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

func TestNegativeIntVarintEncodeAndDecode(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	meta := values.IntValuesMetadata{
		Size: 11,
		Max:  0,
		Min:  mockDataOutlierMin,
	}
	data := make([]int, meta.Size)
	for i := 0; i < meta.Size; i++ {
		data[i] = -i
	}
	// Last element is an outlier.
	data[len(data)-1] = mockDataOutlierMin

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

func TestMixedIntVarintEncodeAndDecode(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	meta := values.IntValuesMetadata{
		Size: 11,
		Min:  mockDataOutlierMin,
		Max:  mockDataOutlierMax,
	}
	data := make([]int, meta.Size)
	for i := 0; i < meta.Size; i++ {
		if i%2 == 0 {
			data[i] = -i
		} else {
			data[i] = i
		}
	}
	// Last two elements are outliers.
	data[len(data)-1] = mockDataOutlierMin
	data[len(data)-2] = mockDataOutlierMax

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

func TestPositiveIntDictionaryEncodeAndDecode(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	meta := values.IntValuesMetadata{
		Size: 5,
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
		Size: 11,
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

func TestMixedIntDictionaryEncodeAndDecode(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	meta := values.IntValuesMetadata{
		Size: 510,
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

func TestEmptyDictionaryEncodeAndDecode(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		meta = values.IntValuesMetadata{}
		data []int
	)
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
		Size: 510,
		Min:  1,
		Max:  11,
	}
	data := make([]int, meta.Size)
	for i := 0; i < meta.Size; i++ {
		data[i] = i + 1
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

func TestNegativeIntDeltaEncodeAndDecode(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	meta := values.IntValuesMetadata{
		Size: 510,
		Min:  -11,
		Max:  -1,
	}
	data := make([]int, meta.Size)
	for i := 0; i < meta.Size; i++ {
		data[i] = -(i + 1)
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
		Size: 510,
		Min:  -256,
		Max:  255,
	}
	data := make([]int, meta.Size)
	for i := 0; i < meta.Size; i++ {
		data[i] = i - 256
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
