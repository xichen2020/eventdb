package pool

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRefCountedPooledBytesArray(t *testing.T) {
	buckets := []BytesArrayBucket{
		{Capacity: 4, Count: 1},
		{Capacity: 8, Count: 1},
	}
	pool := NewBucketizedBytesArrayPool(buckets, nil)
	pool.Init(func(capacity int) [][]byte { return make([][]byte, 0, capacity) })

	vals := pool.Get(4)
	arr := NewRefCountedPooledBytesArray(vals, pool, nil)
	require.Equal(t, int32(1), arr.cnt.RefCount())
	require.Equal(t, 0, len(arr.Get()))
	require.Equal(t, 4, cap(arr.Get()))

	arr.Append([]byte("foo"))
	arr.Append([]byte("bar"))
	arr.Append([]byte("baz"))
	arr.Append([]byte("cat"))
	require.Equal(t, int32(1), arr.cnt.RefCount())
	require.Equal(t, 4, len(arr.Get()))

	arr2 := arr.Snapshot()
	require.Equal(t, int32(2), arr.cnt.RefCount())
	require.Equal(t, 4, len(arr.Get()))
	require.Equal(t, int32(2), arr2.cnt.RefCount())
	require.Equal(t, 4, len(arr2.Get()))
	expected1 := [][]byte{[]byte("foo"), []byte("bar"), []byte("baz"), []byte("cat")}
	assertReturnedToBytesArrayPool(t, pool, expected1, false)

	arr.Append([]byte("rand"))
	require.Equal(t, int32(1), arr.cnt.RefCount())
	require.Equal(t, 5, len(arr.Get()))
	require.Equal(t, int32(1), arr2.cnt.RefCount())
	require.Equal(t, 4, len(arr2.Get()))
	assertReturnedToBytesArrayPool(t, pool, expected1, false)
	expected2 := [][]byte{[]byte("foo"), []byte("bar"), []byte("baz"), []byte("cat"), []byte("rand")}
	assertReturnedToBytesArrayPool(t, pool, expected2, false)

	arr.Close()
	assertReturnedToBytesArrayPool(t, pool, expected1, false)
	assertReturnedToBytesArrayPool(t, pool, expected2, true)

	arr2.Close()
	assertReturnedToBytesArrayPool(t, pool, expected1, true)
}

func assertReturnedToBytesArrayPool(
	t *testing.T,
	p *BucketizedBytesArrayPool,
	expected [][]byte,
	shouldReturn bool,
) {
	toCheck := p.Get(len(expected))[:len(expected)]
	if shouldReturn {
		require.Equal(t, expected, toCheck)
	} else {
		require.NotEqual(t, expected, toCheck)
	}
}
