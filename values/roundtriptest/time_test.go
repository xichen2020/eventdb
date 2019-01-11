package roundtriptest

import (
	"bytes"
	"testing"
	"time"

	"github.com/xichen2020/eventdb/values"
	"github.com/xichen2020/eventdb/values/decoding"
	"github.com/xichen2020/eventdb/values/encoding"
	"github.com/xichen2020/eventdb/values/iterator"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestTimeDeltaEncodeAndDecode(t *testing.T) {
	tests := []struct {
		name string
		opts encoding.EncodeTimeOptions
	}{
		{"Nanos", encoding.EncodeTimeOptions{Resolution: time.Nanosecond}},
		{"Micros", encoding.EncodeTimeOptions{Resolution: time.Microsecond}},
		{"Millis", encoding.EncodeTimeOptions{Resolution: time.Millisecond}},
		{"Secs", encoding.EncodeTimeOptions{Resolution: time.Second}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			size := 512
			data := make([]int64, size)
			for i := 0; i < len(data); i++ {
				data[i] = time.Now().Add(time.Duration(-i) * time.Hour).UnixNano()
			}
			meta := values.TimeValuesMetadata{
				Size: size,
				Min:  data[size-1],
				Max:  data[0],
			}
			iter := iterator.NewMockForwardTimeIterator(ctrl)
			produceMockTimeData(data, iter)

			vals := values.NewMockTimeValues(ctrl)
			gomock.InOrder(
				vals.EXPECT().Metadata().Return(meta),
				vals.EXPECT().Iter().Return(iter, nil),
			)
			testEncodeAndDecodeTime(t, vals, meta, data, tt.opts)
		})
	}
}

func produceMockTimeData(data []int64, iter *iterator.MockForwardTimeIterator) {
	for _, s := range data {
		iter.EXPECT().Next().Return(true).Times(1)
		iter.EXPECT().Current().Return(s).Times(1)
	}
	iter.EXPECT().Next().Return(false).Times(1)
	iter.EXPECT().Err().Return(nil).Times(1)
	iter.EXPECT().Close().Times(1)
}

func testEncodeAndDecodeTime(
	t *testing.T,
	vals *values.MockTimeValues,
	meta values.TimeValuesMetadata,
	data []int64,
	opts encoding.EncodeTimeOptions,
) {
	var buf bytes.Buffer
	enc := encoding.NewTimeEncoder()
	require.NoError(t, enc.Encode(vals, &buf, opts))

	dec := decoding.NewTimeDecoder()
	timeVals, err := dec.DecodeRaw(buf.Bytes())
	require.NoError(t, err)

	require.Equal(t, meta, timeVals.Metadata())
	valsIt, err := timeVals.Iter()
	require.NoError(t, err)
	count := 0
	for valsIt.Next() {
		require.Equal(t, (data[count]/int64(opts.Resolution))*int64(opts.Resolution), valsIt.Current())
		count++
	}
	require.NoError(t, valsIt.Err())
	require.Equal(t, len(data), count)
}
