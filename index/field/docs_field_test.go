package field

import (
	"fmt"
	"testing"

	"github.com/xichen2020/eventdb/document/field"
	"github.com/xichen2020/eventdb/x/pool"

	"github.com/stretchr/testify/require"
)

func TestDocsFieldBuilderBytesSealAndSnapshot(t *testing.T) {
	stringArrayBuckets := []pool.BytesArrayBucket{
		{Capacity: 128, Count: 1},
		{Capacity: 256, Count: 1},
	}
	stringArrayPool := pool.NewBucketizedBytesArrayPool(stringArrayBuckets, nil)
	stringArrayPool.Init(func(capacity int) [][]byte { return make([][]byte, 0, capacity) })
	fieldTypes := field.ValueTypeSet{
		field.BytesType: struct{}{},
	}
	opts := NewDocsFieldBuilderOptions().
		SetBytesArrayPool(stringArrayPool)
	builder := NewDocsFieldBuilder([]string{"testPath"}, opts)

	// Add some string values.
	builder.Add(1, field.ValueUnion{
		Type:     field.BytesType,
		BytesVal: []byte("foo"),
	})
	builder.Add(3, field.ValueUnion{
		Type:     field.BytesType,
		BytesVal: []byte("bar"),
	})
	builder.Add(6, field.ValueUnion{
		Type:     field.BytesType,
		BytesVal: []byte("baz"),
	})

	// Take a snapshot.
	snapshot1, remainderTypes, err := builder.SnapshotFor(fieldTypes)
	require.Equal(t, 0, len(remainderTypes))
	require.NoError(t, err)

	snapshotField1, _ := snapshot1.BytesField()
	metadata1 := snapshotField1.Values().Metadata()
	require.Equal(t, 3, metadata1.Size)

	// Add some more values.
	builder.Add(10, field.ValueUnion{
		Type:     field.BytesType,
		BytesVal: []byte("cat"),
	})

	// Assert that the new value is invisible to the snapshot.
	require.Equal(t, 3, metadata1.Size)

	// Take another snapshot.
	snapshot2, remainderTypes, err := builder.SnapshotFor(fieldTypes)
	require.Equal(t, 0, len(remainderTypes))
	require.NoError(t, err)

	snapshotField2, _ := snapshot2.BytesField()
	metadata2 := snapshotField2.Values().Metadata()
	require.Equal(t, 4, metadata2.Size)

	var expectedLarge [][]byte
	expectedSmall := [][]byte{[]byte("foo"), []byte("bar"), []byte("baz"), []byte("cat")}
	expectedLarge = append(expectedLarge, expectedSmall...)
	for i := 128; i < 256; i++ {
		val := fmt.Sprintf("cat%d", i)
		builder.Add(int32(i), field.ValueUnion{
			Type:     field.BytesType,
			BytesVal: []byte(val),
		})
		expectedLarge = append(expectedLarge, []byte(val))
	}

	assertReturnedToBytesArrayPool(t, stringArrayPool, expectedSmall, false)
	assertReturnedToBytesArrayPool(t, stringArrayPool, expectedLarge, false)

	// Seal the builder.
	sealed := builder.Seal(15)

	// Close the builder.
	builder.Close()
	assertReturnedToBytesArrayPool(t, stringArrayPool, expectedSmall, false)
	assertReturnedToBytesArrayPool(t, stringArrayPool, expectedLarge, false)

	// Make a shallow copy of the sealed field.
	shallowCopy := sealed.ShallowCopy()
	assertReturnedToBytesArrayPool(t, stringArrayPool, expectedSmall, false)
	assertReturnedToBytesArrayPool(t, stringArrayPool, expectedLarge, false)

	snapshot1.Close()
	assertReturnedToBytesArrayPool(t, stringArrayPool, expectedSmall, false)
	assertReturnedToBytesArrayPool(t, stringArrayPool, expectedLarge, false)

	snapshot2.Close()
	assertReturnedToBytesArrayPool(t, stringArrayPool, expectedSmall, true)
	assertReturnedToBytesArrayPool(t, stringArrayPool, expectedLarge, false)

	shallowCopy.Close()
	assertReturnedToBytesArrayPool(t, stringArrayPool, expectedLarge, false)

	sealed.Close()
	assertReturnedToBytesArrayPool(t, stringArrayPool, expectedLarge, true)
}

func TestDocsFieldNewDocsField(t *testing.T) {
	stringArrayBuckets := []pool.BytesArrayBucket{
		{Capacity: 128, Count: 1},
		{Capacity: 256, Count: 1},
	}
	stringArrayPool := pool.NewBucketizedBytesArrayPool(stringArrayBuckets, nil)
	stringArrayPool.Init(func(capacity int) [][]byte { return make([][]byte, 0, capacity) })

	opts := NewDocsFieldBuilderOptions().
		SetBytesArrayPool(stringArrayPool)
	builder := NewDocsFieldBuilder([]string{"testPath"}, opts)

	// Add some string values.
	builder.Add(1, field.ValueUnion{
		Type:     field.BytesType,
		BytesVal: []byte("foo"),
	})
	builder.Add(3, field.ValueUnion{
		Type:     field.BytesType,
		BytesVal: []byte("bar"),
	})
	builder.Add(6, field.ValueUnion{
		Type:     field.BytesType,
		BytesVal: []byte("baz"),
	})
	builder.Add(10, field.ValueUnion{
		Type:     field.BytesType,
		BytesVal: []byte("cat"),
	})

	expected := [][]byte{[]byte("foo"), []byte("bar"), []byte("baz"), []byte("cat")}

	// Seal the builder.
	sealed := builder.Seal(15)

	// Close the builder.
	builder.Close()
	assertReturnedToBytesArrayPool(t, stringArrayPool, expected, false)

	// Create a new docs field from the newly sealed docs field.
	newField, remainder, err := sealed.NewDocsFieldFor(field.ValueTypeSet{
		field.BytesType: struct{}{},
	})
	require.NoError(t, err)
	require.Nil(t, remainder)

	expectedMeta := DocsFieldMetadata{
		FieldPath:  []string{"testPath"},
		FieldTypes: []field.ValueType{field.BytesType},
	}
	require.Equal(t, expectedMeta, newField.Metadata())

	// Closing the sealed field should not cause the string field to be returned to pool.
	sealed.Close()
	assertReturnedToBytesArrayPool(t, stringArrayPool, expected, false)

	// Closing the new field should cause the string field to be returned to pool.
	newField.Close()
	assertReturnedToBytesArrayPool(t, stringArrayPool, expected, true)
}

func TestDocsFieldNewMergedDocsField(t *testing.T) {
	stringArrayBuckets1 := []pool.BytesArrayBucket{
		{Capacity: 128, Count: 1},
	}
	bytesArrayPool1 := pool.NewBucketizedBytesArrayPool(stringArrayBuckets1, nil)
	bytesArrayPool1.Init(func(capacity int) [][]byte { return make([][]byte, 0, capacity) })

	opts1 := NewDocsFieldBuilderOptions().
		SetBytesArrayPool(bytesArrayPool1)
	builder1 := NewDocsFieldBuilder([]string{"testPath"}, opts1)

	// Add some string values.
	builder1.Add(1, field.ValueUnion{
		Type:     field.BytesType,
		BytesVal: []byte("foo"),
	})
	builder1.Add(3, field.ValueUnion{
		Type:     field.BytesType,
		BytesVal: []byte("bar"),
	})
	builder1.Add(6, field.ValueUnion{
		Type:     field.BytesType,
		BytesVal: []byte("baz"),
	})
	builder1.Add(10, field.ValueUnion{
		Type:     field.BytesType,
		BytesVal: []byte("cat"),
	})

	expected1 := [][]byte{[]byte("foo"), []byte("bar"), []byte("baz"), []byte("cat")}

	// Seal the builder.
	sealed1 := builder1.Seal(15)

	// Close the builder.
	builder1.Close()
	assertReturnedToBytesArrayPool(t, bytesArrayPool1, expected1, false)

	stringArrayBuckets2 := []pool.BytesArrayBucket{
		{Capacity: 128, Count: 1},
	}
	bytesArrayPool2 := pool.NewBucketizedBytesArrayPool(stringArrayBuckets2, nil)
	bytesArrayPool2.Init(func(capacity int) [][]byte { return make([][]byte, 0, capacity) })

	opts2 := NewDocsFieldBuilderOptions().
		SetBytesArrayPool(bytesArrayPool2)
	builder2 := NewDocsFieldBuilder([]string{"testPath"}, opts2)

	// Add some string values.
	builder2.Add(2, field.ValueUnion{
		Type:     field.BytesType,
		BytesVal: []byte("quest"),
	})
	builder2.Add(4, field.ValueUnion{
		Type:     field.BytesType,
		BytesVal: []byte("blah"),
	})
	builder2.Add(5, field.ValueUnion{
		Type:     field.BytesType,
		BytesVal: []byte("raw"),
	})
	builder2.Add(7, field.ValueUnion{
		Type:     field.BytesType,
		BytesVal: []byte("cat"),
	})

	expected2 := [][]byte{[]byte("quest"), []byte("blah"), []byte("raw"), []byte("cat")}

	// Seal the builder.
	sealed2 := builder2.Seal(10)

	// Close the builder.
	builder2.Close()
	assertReturnedToBytesArrayPool(t, bytesArrayPool2, expected2, false)

	// Create a new merged field.
	merged := sealed1.NewMergedDocsField(sealed2)

	// Merging should not cause the string array 1 to be returned to pool.
	assertReturnedToBytesArrayPool(t, bytesArrayPool1, expected1, false)

	// Merging should not cause the string array 2 to be returned to pool.
	assertReturnedToBytesArrayPool(t, bytesArrayPool2, expected2, false)

	// Closing the 2nd field should cause string array 2 to be returned to pool.
	sealed2.Close()
	assertReturnedToBytesArrayPool(t, bytesArrayPool2, expected2, true)

	// Closing the 1st field will not return string array 1 to pool.
	sealed1.Close()
	assertReturnedToBytesArrayPool(t, bytesArrayPool1, expected1, false)

	// Closing the merged field will return string array 1 to pool.
	merged.Close()
	assertReturnedToBytesArrayPool(t, bytesArrayPool1, expected1, true)
}

func TestDocsFieldFieldBuilderInitializedOnce(t *testing.T) {
	var (
		boolArrayBuckets = []pool.BoolArrayBucket{
			{Capacity: 128, Count: 1},
		}
		doubleArrayBuckets = []pool.Float64ArrayBucket{
			{Capacity: 128, Count: 1},
		}
		intArrayBuckets = []pool.IntArrayBucket{
			{Capacity: 128, Count: 1},
		}
		int64ArrayBuckets = []pool.Int64ArrayBucket{
			{Capacity: 128, Count: 1},
		}
		stringArrayBuckets = []pool.BytesArrayBucket{
			{Capacity: 128, Count: 1},
		}
		boolArrayPool   = pool.NewBucketizedBoolArrayPool(boolArrayBuckets, nil)
		doubleArrayPool = pool.NewBucketizedFloat64ArrayPool(doubleArrayBuckets, nil)
		intArrayPool    = pool.NewBucketizedIntArrayPool(intArrayBuckets, nil)
		int64ArrayPool  = pool.NewBucketizedInt64ArrayPool(int64ArrayBuckets, nil)
		stringArrayPool = pool.NewBucketizedBytesArrayPool(stringArrayBuckets, nil)
		boolAllocs      int
		doubleAllocs    int
		intAllocs       int
		int64Allocs     int
		stringAllocs    int
	)
	boolArrayPool.Init(func(capacity int) []bool {
		boolAllocs++
		return make([]bool, 0, capacity)
	})
	doubleArrayPool.Init(func(capacity int) []float64 {
		doubleAllocs++
		return make([]float64, 0, capacity)
	})
	intArrayPool.Init(func(capacity int) []int {
		intAllocs++
		return make([]int, 0, capacity)
	})
	int64ArrayPool.Init(func(capacity int) []int64 {
		int64Allocs++
		return make([]int64, 0, capacity)
	})
	stringArrayPool.Init(func(capacity int) [][]byte {
		stringAllocs++
		return make([][]byte, 0, capacity)
	})
	opts := NewDocsFieldBuilderOptions().
		SetBoolArrayPool(boolArrayPool).
		SetDoubleArrayPool(doubleArrayPool).
		SetIntArrayPool(intArrayPool).
		SetInt64ArrayPool(int64ArrayPool).
		SetBytesArrayPool(stringArrayPool)
	builder := NewDocsFieldBuilder([]string{"testPath"}, opts)

	builder.Add(1, field.ValueUnion{Type: field.BoolType, BoolVal: true})
	builder.Add(2, field.ValueUnion{Type: field.BoolType, BoolVal: false})
	builder.Add(1, field.ValueUnion{Type: field.DoubleType, DoubleVal: 1})
	builder.Add(2, field.ValueUnion{Type: field.DoubleType, DoubleVal: 2})
	builder.Add(1, field.ValueUnion{Type: field.IntType, IntVal: 1})
	builder.Add(2, field.ValueUnion{Type: field.IntType, IntVal: 2})
	builder.Add(1, field.ValueUnion{Type: field.TimeType, TimeNanosVal: 1})
	builder.Add(2, field.ValueUnion{Type: field.TimeType, TimeNanosVal: 2})
	builder.Add(1, field.ValueUnion{Type: field.BytesType, BytesVal: []byte("foo")})
	builder.Add(2, field.ValueUnion{Type: field.BytesType, BytesVal: []byte("bar")})
	builder.Close()

	require.Equal(t, 1, boolAllocs)
	require.Equal(t, 1, doubleAllocs)
	require.Equal(t, 1, intAllocs)
	require.Equal(t, 1, int64Allocs)
	require.Equal(t, 1, stringAllocs)

	require.Equal(t, []bool{true, false}, boolArrayPool.Get(2)[:2])
	require.Equal(t, []float64{1, 2}, doubleArrayPool.Get(2)[:2])
	require.Equal(t, []int{1, 2}, intArrayPool.Get(2)[:2])
	require.Equal(t, []int64{1, 2}, int64ArrayPool.Get(2)[:2])
	require.Equal(t, [][]byte{[]byte("foo"), []byte("bar")}, stringArrayPool.Get(2)[:2])
}

func assertReturnedToBytesArrayPool(
	t *testing.T,
	p *pool.BucketizedBytesArrayPool,
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
