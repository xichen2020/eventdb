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

const (
	numUniqueDouble = 10
)

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

func produceMockDoubleData(data []float64, iter *iterator.MockForwardDoubleIterator) {
	for _, s := range data {
		iter.EXPECT().Next().Return(true).Times(1)
		iter.EXPECT().Current().Return(s).Times(1)
	}
	iter.EXPECT().Next().Return(false).Times(1)
	iter.EXPECT().Err().Return(nil).Times(1)
	iter.EXPECT().Close().Return(nil)
}

func ensureEncodeAndDecodeDouble(t *testing.T, data []float64) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	meta := values.DoubleValuesMetadata{
		Size: len(data),
		Min:  123.45, // Not real but ok for testing
		Max:  567.89, // Not real but ok for testing
	}
	iter := iterator.NewMockForwardDoubleIterator(ctrl)
	produceMockDoubleData(data, iter)

	vals := values.NewMockDoubleValues(ctrl)
	gomock.InOrder(
		vals.EXPECT().Metadata().Return(meta),
		vals.EXPECT().Iter().Return(iter, nil),
	)

	var buf bytes.Buffer
	enc := encoding.NewDoubleEncoder()
	require.NoError(t, enc.Encode(vals, &buf))

	dec := decoding.NewDoubleDecoder()
	doubleVals, err := dec.DecodeRaw(buf.Bytes())
	require.NoError(t, err)

	require.Equal(t, meta, doubleVals.Metadata())
	valsIt, err := doubleVals.Iter()
	require.NoError(t, err)
	count := 0
	for valsIt.Next() {
		require.Equal(t, data[count], valsIt.Current())
		count++
	}
	require.NoError(t, valsIt.Err())
	require.Equal(t, len(data), count)
}
