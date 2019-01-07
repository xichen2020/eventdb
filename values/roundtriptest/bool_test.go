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

func TestRunLengthEncodeAndDecodeBool(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	meta := values.BoolValuesMetadata{
		NumTrues:  7,
		NumFalses: 4,
	}
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

	iter := iterator.NewMockForwardBoolIterator(ctrl)
	produceMockBoolData(data, iter)

	vals := values.NewMockBoolValues(ctrl)
	gomock.InOrder(
		vals.EXPECT().Metadata().Return(meta),
		vals.EXPECT().Iter().Return(iter, nil),
	)

	var buf bytes.Buffer
	enc := encoding.NewBoolEncoder()
	require.NoError(t, enc.Encode(vals, &buf))

	dec := decoding.NewBoolDecoder()
	boolVals, err := dec.DecodeRaw(buf.Bytes())
	require.NoError(t, err)

	require.Equal(t, meta, boolVals.Metadata())
	valsIt, err := boolVals.Iter()
	require.NoError(t, err)
	count := 0
	for valsIt.Next() {
		require.Equal(t, data[count], valsIt.Current())
		count++
	}
	require.NoError(t, valsIt.Err())
	require.Equal(t, len(data), count)
}

func produceMockBoolData(
	data []bool,
	iter *iterator.MockForwardBoolIterator,
) {
	for _, s := range data {
		iter.EXPECT().Next().Return(true).Times(1)
		iter.EXPECT().Current().Return(s).Times(1)
	}
	iter.EXPECT().Next().Return(false).Times(1)
	iter.EXPECT().Err().Return(nil).Times(1)
	iter.EXPECT().Close().Return(nil)
}
