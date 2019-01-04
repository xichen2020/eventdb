package index

import (
	"fmt"
	"testing"

	"github.com/xichen2020/eventdb/document/field"
	"github.com/xichen2020/eventdb/x/pool"

	"github.com/stretchr/testify/require"
)

func TestDocsFieldBuilderString(t *testing.T) {
	stringArrayBuckets := []pool.StringArrayBucket{
		{Capacity: 128, Count: 1},
		{Capacity: 256, Count: 1},
	}
	stringArrayPool := pool.NewBucketizedStringArrayPool(stringArrayBuckets, nil)
	stringArrayPool.Init(func(capacity int) []string { return make([]string, 0, capacity) })

	opts := NewDocsFieldBuilderOptions().
		SetStringArrayPool(stringArrayPool)
	builder := NewDocsFieldBuilder([]string{"testPath"}, opts)

	// Add some string values.
	builder.Add(1, field.ValueUnion{
		Type:      field.StringType,
		StringVal: "foo",
	})
	builder.Add(3, field.ValueUnion{
		Type:      field.StringType,
		StringVal: "bar",
	})
	builder.Add(6, field.ValueUnion{
		Type:      field.StringType,
		StringVal: "baz",
	})

	// Take a snapshot.
	snapshot1 := builder.Snapshot()
	snapshotField1, _ := snapshot1.StringField()
	metadata1 := snapshotField1.Values().Metadata()
	require.Equal(t, 3, metadata1.Size)

	// Add some more values.
	builder.Add(10, field.ValueUnion{
		Type:      field.StringType,
		StringVal: "cat",
	})

	// Assert that the new value is invisible to the snapshot.
	require.Equal(t, 3, metadata1.Size)

	// Take another snapshot.
	snapshot2 := builder.Snapshot()
	snapshotField2, _ := snapshot2.StringField()
	metadata2 := snapshotField2.Values().Metadata()
	require.Equal(t, 4, metadata2.Size)

	var expectedLarge []string
	expectedSmall := []string{"foo", "bar", "baz", "cat"}
	expectedLarge = append(expectedLarge, expectedSmall...)
	for i := 128; i < 256; i++ {
		val := fmt.Sprintf("cat%d", i)
		builder.Add(int32(i), field.ValueUnion{
			Type:      field.StringType,
			StringVal: val,
		})
		expectedLarge = append(expectedLarge, val)
	}

	assertReturnedToStringArrayPool(t, stringArrayPool, expectedSmall, false)
	assertReturnedToStringArrayPool(t, stringArrayPool, expectedLarge, false)

	// Seal the builder.
	sealed := builder.Seal(15)

	// Close the builder.
	builder.Close()
	assertReturnedToStringArrayPool(t, stringArrayPool, expectedSmall, false)
	assertReturnedToStringArrayPool(t, stringArrayPool, expectedLarge, false)

	// Make a shallow copy of the sealed field.
	shallowCopy := sealed.ShallowCopy()
	assertReturnedToStringArrayPool(t, stringArrayPool, expectedSmall, false)
	assertReturnedToStringArrayPool(t, stringArrayPool, expectedLarge, false)

	snapshot1.Close()
	assertReturnedToStringArrayPool(t, stringArrayPool, expectedSmall, false)
	assertReturnedToStringArrayPool(t, stringArrayPool, expectedLarge, false)

	snapshot2.Close()
	assertReturnedToStringArrayPool(t, stringArrayPool, expectedSmall, true)
	assertReturnedToStringArrayPool(t, stringArrayPool, expectedLarge, false)

	shallowCopy.Close()
	assertReturnedToStringArrayPool(t, stringArrayPool, expectedLarge, false)

	sealed.Close()
	assertReturnedToStringArrayPool(t, stringArrayPool, expectedLarge, true)
}

func assertReturnedToStringArrayPool(
	t *testing.T,
	p *pool.BucketizedStringArrayPool,
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
