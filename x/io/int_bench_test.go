package io

import (
	"math"
	"testing"
)

type bounds struct {
	upper int64
	lower int64
}

// nolint: unparam
func calculateVarintBytesBinarySearch(value int64) int {
	var (
		currIdx     int
		lowerIdx    int
		upperIdx    = 9
		lookupArray = []bounds{
			bounds{
				upper: one - 1,
				lower: -one,
			},
			bounds{
				upper: two - 1,
				lower: -two,
			},
			bounds{
				upper: three - 1,
				lower: -three,
			},
			bounds{
				upper: four - 1,
				lower: -four,
			},
			bounds{
				upper: five - 1,
				lower: -five,
			},
			bounds{
				upper: six - 1,
				lower: -six,
			},
			bounds{
				upper: seven - 1,
				lower: -seven,
			},
			bounds{
				upper: eight - 1,
				lower: -eight,
			},
			bounds{
				upper: nine - 1,
				lower: -nine,
			},
			bounds{
				upper: math.MaxInt64,
				lower: math.MinInt64,
			},
		}
	)
	for {
		if upperIdx == lowerIdx {
			currIdx = upperIdx
			break
		}
		// Rounds down.
		currIdx = (upperIdx + lowerIdx) / 2

		// Inside bounds.
		if value <= lookupArray[currIdx].upper && value >= lookupArray[currIdx].lower {
			upperIdx = currIdx
		} else {
			// Outside bounds.
			lowerIdx = currIdx + 1
		}
	}
	return currIdx
}

func BenchmarkComparison(b *testing.B) {
	for n := 0; n < b.N; n++ {
		VarintBytes(int64(n * 10))
	}
}

func BenchmarkBinarySearch(b *testing.B) {
	for n := 0; n < b.N; n++ {
		calculateVarintBytesBinarySearch(int64(n * 10))
	}
}
