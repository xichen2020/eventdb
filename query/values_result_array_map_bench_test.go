package query

import (
	"fmt"
	"testing"

	"github.com/xichen2020/eventdb/document/field"
)

const (
	testBenchNumKeys = 100000
)

func BenchmarkValueResultArrayMapGetWithTwoValueKeys(b *testing.B) {
	testKey := []field.ValueUnion{
		field.NewIntUnion(0),
		field.NewStringUnion("foobarbaz1"),
	}
	m := genValueResultArrayMap(testBenchNumKeys, 2)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		m.Get(testKey)
	}
}

func BenchmarkValueResultArrayMapGetWithThreeValueKeys(b *testing.B) {
	testKey := []field.ValueUnion{
		field.NewIntUnion(0),
		field.NewStringUnion("foobarbaz1"),
		field.NewDoubleUnion(56.78),
	}
	m := genValueResultArrayMap(testBenchNumKeys, 3)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		m.Get(testKey)
	}
}

func genValueResultArrayMap(
	numKeys int,
	numValuesPerKey int,
) *ValuesResultArrayHash {
	m := NewValuesResultArrayMap(numKeys)
	keys := make([]field.ValueUnion, 0, numValuesPerKey)
	for i := 0; i < numValuesPerKey; i++ {
		var key field.ValueUnion
		switch i % 3 {
		case 0:
			key = field.NewIntUnion(1234)
		case 1:
			key = field.NewStringUnion("foobarbazblah")
		case 2:
			key = field.NewDoubleUnion(56.78)
		}
		keys = append(keys, key)
	}

	keyIdx := 0
	for j := 0; j < numKeys; j++ {
		// Perturb the keys a little.
		switch keyIdx % 3 {
		case 0:
			keys[keyIdx] = field.NewIntUnion(j)
		case 1:
			keys[keyIdx] = field.NewStringUnion(fmt.Sprintf("foobarbaz%d", j))
		case 2:
			keys[keyIdx] = field.NewDoubleUnion(float64(-j))
		}
		m.Set(keys, nil)
		keyIdx++
		if keyIdx >= numValuesPerKey {
			keyIdx = 0
		}
	}
	return m
}
