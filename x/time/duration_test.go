package time

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDurationMarshalJSON(t *testing.T) {
	inputs := []struct {
		dur      time.Duration
		expected string
	}{
		{
			dur:      time.Second,
			expected: `"1s"`,
		},
		{
			dur:      5 * time.Minute,
			expected: `"5m0s"`,
		},
		{
			dur:      2 * time.Hour,
			expected: `"2h0m0s"`,
		},
	}

	for _, input := range inputs {
		b, err := json.Marshal(Duration(input.dur))
		require.NoError(t, err)
		require.Equal(t, input.expected, string(b))
	}
}

func TestDurationUnmarshalJSON(t *testing.T) {
	inputs := []struct {
		expected time.Duration
		str      string
	}{
		{
			str:      `"1s"`,
			expected: time.Second,
		},
		{
			str:      `"5m"`,
			expected: 5 * time.Minute,
		},
		{
			str:      `"2h"`,
			expected: 2 * time.Hour,
		},
	}

	for _, input := range inputs {
		var dur Duration
		err := json.Unmarshal([]byte(input.str), &dur)
		require.NoError(t, err)
		require.Equal(t, input.expected, time.Duration(dur))
	}
}
