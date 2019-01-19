package io

import (
	"encoding/binary"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVarintBytes(t *testing.T) {
	buf := make([]byte, binary.MaxVarintLen64)
	lowerBounds := []int64{
		-one,
		-two,
		-three,
		-four,
		-five,
		-six,
		-seven,
		-eight,
		-nine,
	}
	upperBounds := []int64{
		one - 1,
		two - 1,
		three - 1,
		four - 1,
		five - 1,
		six - 1,
		seven - 1,
		eight - 1,
		nine - 1,
	}
	for idx := 0; idx < len(upperBounds); idx++ {
		require.Equal(t, binary.PutVarint(buf, upperBounds[idx]), VarintBytes(upperBounds[idx]))
		require.Equal(t, binary.PutVarint(buf, lowerBounds[idx]), VarintBytes(lowerBounds[idx]))
		// Also step over boundaries as a sanity check.
		require.Equal(t, binary.PutVarint(buf, upperBounds[idx]+1), VarintBytes(upperBounds[idx]+1))
		require.Equal(t, binary.PutVarint(buf, lowerBounds[idx]-1), VarintBytes(lowerBounds[idx]-1))
	}
	require.Equal(t, binary.PutVarint(buf, math.MaxInt64), VarintBytes(math.MaxInt64))
	require.Equal(t, binary.PutVarint(buf, math.MinInt64), VarintBytes(math.MinInt64))
}
