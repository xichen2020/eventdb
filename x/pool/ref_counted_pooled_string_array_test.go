package pool

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRefCountedPooledStringArray(t *testing.T) {
	buckets := []StringArrayBucket{
		{Capacity: 4, Count: 1},
		{Capacity: 8, Count: 1},
	}
	pool := NewBucketizedStringArrayPool(buckets, nil)
	pool.Init(func(capacity int) []string { return make([]string, 0, capacity) })

	vals := pool.Get(4)
	arr := NewRefCountedPooledStringArray(vals, pool, nil)
	require.Equal(t, int32(1), arr.cnt.RefCount())
	require.Equal(t, 0, len(arr.Get()))
	require.Equal(t, 4, cap(arr.Get()))

	arr.Append("foo")
	arr.Append("bar")
	arr.Append("baz")
	arr.Append("cat")
	require.Equal(t, int32(1), arr.cnt.RefCount())
	require.Equal(t, 4, len(arr.Get()))

	arr2 := arr.Snapshot()
	require.Equal(t, int32(2), arr.cnt.RefCount())
	require.Equal(t, 4, len(arr.Get()))
	require.Equal(t, int32(2), arr2.cnt.RefCount())
	require.Equal(t, 4, len(arr2.Get()))
	expected1 := []string{"foo", "bar", "baz", "cat"}
	assertReturnedToStringArrayPool(t, pool, expected1, false)

	arr.Append("rand")
	require.Equal(t, int32(1), arr.cnt.RefCount())
	require.Equal(t, 5, len(arr.Get()))
	require.Equal(t, int32(1), arr2.cnt.RefCount())
	require.Equal(t, 4, len(arr2.Get()))
	assertReturnedToStringArrayPool(t, pool, expected1, false)
	expected2 := []string{"foo", "bar", "baz", "cat", "rand"}
	assertReturnedToStringArrayPool(t, pool, expected2, false)

	arr.Close()
	assertReturnedToStringArrayPool(t, pool, expected1, false)
	assertReturnedToStringArrayPool(t, pool, expected2, true)

	arr2.Close()
	assertReturnedToStringArrayPool(t, pool, expected1, true)
}

func assertReturnedToStringArrayPool(
	t *testing.T,
	p *BucketizedStringArrayPool,
	expected []string,
	shouldReturn bool,
) {
	toCheck := p.Get(len(expected))[:len(expected)]
	if shouldReturn {
		require.Equal(t, expected, toCheck)
	} else {
		require.NotEqual(t, expected, toCheck)
	}
}
