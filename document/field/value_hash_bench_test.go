package field

import (
	"bytes"
	"fmt"
	"testing"

	xhash "github.com/xichen2020/eventdb/x/hash"
)

// Summary: The builtin string map is 4x faster than the custom one. Both have 0 allocs.

const (
	testBenchKeyPrefixSize = 200
	testBenchMapSize       = 100000
)

func BenchmarkBuiltinStringMap(b *testing.B) {
	prefix := genBenchStringKeyPrefix(testBenchKeyPrefixSize)
	m := genBenchBuiltinStringMap(testBenchKeyPrefixSize, testBenchMapSize)
	testBenchKey := prefix + "0"
	b.ResetTimer()

	var numExists int32
	for i := 0; i < b.N; i++ {
		_, exists := m[testBenchKey]
		if exists {
			numExists++
		}
	}
}

func BenchmarkCustomStringMap(b *testing.B) {
	prefix := genBenchStringKeyPrefix(testBenchKeyPrefixSize)
	m := genBenchCustomStringMap(testBenchKeyPrefixSize, testBenchMapSize)
	testBenchKey := prefix + "0"
	b.ResetTimer()

	var numExists int32
	for i := 0; i < b.N; i++ {
		hash := xhash.StringHash(testBenchKey)
		_, exists := m[hash]
		if exists {
			numExists++
		}
	}
}

func genBenchStringKeyPrefix(
	keyPrefixSize int,
) string {
	var b bytes.Buffer
	for i := 0; i < keyPrefixSize; i++ {
		b.WriteByte(byte('b'))
	}
	return b.String()
}

// nolint: unparam
func genBenchBuiltinStringMap(
	keyPrefixSize int,
	mapSize int,
) map[string]struct{} {
	prefix := genBenchStringKeyPrefix(keyPrefixSize)
	m := make(map[string]struct{}, mapSize)
	for i := 0; i < mapSize; i++ {
		key := fmt.Sprintf("%s%d", prefix, i)
		m[key] = struct{}{}
	}
	return m
}

// nolint: unparam
func genBenchCustomStringMap(
	keyPrefixSize int,
	mapSize int,
) map[xhash.Hash]struct{} {
	prefix := genBenchStringKeyPrefix(keyPrefixSize)
	m := make(map[xhash.Hash]struct{}, mapSize)
	for i := 0; i < mapSize; i++ {
		key := fmt.Sprintf("%s%d", prefix, i)
		hash := xhash.StringHash(key)
		m[hash] = struct{}{}
	}
	return m
}
