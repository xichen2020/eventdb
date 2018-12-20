package encoding

import (
	"bytes"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

const (
	numUniqueTime = 10
)

func produceMockTimeData(data []int64, iter *MockRewindableTimeIterator) {
	for _, s := range data {
		iter.EXPECT().Next().Return(true).Times(1)
		iter.EXPECT().Current().Return(s).Times(1)
	}
	iter.EXPECT().Next().Return(false).Times(1)
	iter.EXPECT().Err().Return(nil).Times(1)
}

// Ensure that encoding/decoding test data gives the same result.
func ensureEncodeAndDecodeTime(t *testing.T, res time.Duration, data []int64) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockIter := NewMockRewindableTimeIterator(ctrl)
	// Call produce data twice since the first pass of encode captures metadata.
	produceMockTimeData(data, mockIter)
	mockIter.EXPECT().Rewind().Return().Times(1)
	produceMockTimeData(data, mockIter)

	var buf bytes.Buffer

	enc := NewTimeEncoder()
	err := enc.Encode(&buf, mockIter, res)
	require.Nil(t, err)

	dec := NewTimeDecoder()
	iter, err := dec.Decode(bytes.NewBuffer(buf.Bytes()))
	require.Nil(t, err)

	for idx := 0; iter.Next(); idx++ {
		scaledData := (data[idx] / int64(res)) * int64(res)
		require.Equal(t, scaledData, iter.Current())
	}
}

func TestTimeDeltaEncodeAndDecode(t *testing.T) {
	tests := []struct {
		name       string
		resolution time.Duration
	}{
		{"Nanos", time.Nanosecond},
		{"Micros", time.Microsecond},
		{"Millis", time.Millisecond},
		{"Secs", time.Second},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := make([]int64, numUniqueTime)
			// we need > than max uniques to trigger default encoding
			for i := 0; i < numUniqueTime; i++ {
				data[i] = time.Now().Add(time.Duration(-i) * time.Hour).UnixNano()
			}
			ensureEncodeAndDecodeTime(t, tt.resolution, data)
		})
	}
}
