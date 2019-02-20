package pool

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xichen2020/eventdb/x/strings"
)

func TestRefCountedPooledStringArray(t *testing.T) {
	buckets := []StringArrayBucket{
		{Capacity: 4, Count: 1},
		{Capacity: 8, Count: 1},
	}
	pool := NewBucketizedStringArrayPool(buckets, nil)
	pool.Init(func(capacity int) []string { return make([]string, 0, capacity) })

	vals := pool.Get(4)
	var valuesResetFn = strings.ValuesResetFn
	arr := NewRefCountedPooledStringArray(vals, pool, &valuesResetFn)
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
	assertReturnedToStringArrayPool(t, pool, arr.Get(), false)
	assertReturnedToStringArrayPool(t, pool, arr2.Get(), false)

	arr.Append("rand")
	require.Equal(t, int32(1), arr.cnt.RefCount())
	require.Equal(t, 5, len(arr.Get()))
	require.Equal(t, int32(1), arr2.cnt.RefCount())
	require.Equal(t, 4, len(arr2.Get()))
	assertReturnedToStringArrayPool(t, pool, arr.Get(), false)
	assertReturnedToStringArrayPool(t, pool, arr2.Get(), false)

	expected := arr.Get()
	arr.Close()
	assertReturnedToStringArrayPool(t, pool, expected, true)
	assertReturnedToStringArrayPool(t, pool, arr2.Get(), false)

	expected = arr2.Get()
	arr2.Close()
	assertReturnedToStringArrayPool(t, pool, expected, true)
}

func assertReturnedToStringArrayPool(
	t *testing.T,
	p *BucketizedStringArrayPool,
	expected []string,
	shouldReturn bool,
) {
	toCheck := p.Get(len(expected))[:len(expected)]
	// Since we reset all the values upon returning a slice to the pool,
	// we won't check if the elements match. Rather, we'll check to see if the slice
	// is pointing to the same memory adresss.
	if shouldReturn {
		require.Equal(t, fmt.Sprintf("%p", expected), fmt.Sprintf("%p", toCheck))
	} else {
		require.NotEqual(t, fmt.Sprintf("%p", expected), fmt.Sprintf("%p", toCheck))
	}
}
