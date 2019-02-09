package query

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTimeBucketResultsMarshalJSON(t *testing.T) {
	res := &TimeBucketResults{
		StartBucketNanos: 10000,
		BucketSizeNanos:  2000,
		NumBuckets:       5,
		buckets:          []int{12, 0, 78, 0, 15},
	}
	b, err := json.Marshal(res)
	require.NoError(t, err)
	expected := `{"granularity":2000,"buckets":[{"startAtNanos":10000,"value":12},{"startAtNanos":12000,"value":0},{"startAtNanos":14000,"value":78},{"startAtNanos":16000,"value":0},{"startAtNanos":18000,"value":15}]}`
	require.Equal(t, expected, string(b))
}
