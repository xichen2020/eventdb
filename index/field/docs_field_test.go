package field

import (
	"fmt"
	"testing"

	"github.com/xichen2020/eventdb/document/field"
	"github.com/xichen2020/eventdb/x/pool"

	"github.com/stretchr/testify/require"
)

func TestDocsFieldBuilderStringSealAndSnapshot(t *testing.T) {
	stringArrayBuckets := []pool.StringArrayBucket{
		{Capacity: 128, Count: 1},
		{Capacity: 256, Count: 1},
	}
	stringArrayPool := pool.NewBucketizedStringArrayPool(stringArrayBuckets, nil)
	stringArrayPool.Init(func(capacity int) []string { return make([]string, 0, capacity) })
	fieldTypes := field.ValueTypeSet{
		field.StringType: struct{}{},
	}
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
	snapshot1, remainderTypes, err := builder.SnapshotFor(fieldTypes)
	require.Equal(t, 0, len(remainderTypes))
	require.NoError(t, err)

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
	snapshot2, remainderTypes, err := builder.SnapshotFor(fieldTypes)
	require.Equal(t, 0, len(remainderTypes))
	require.NoError(t, err)

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

func TestDocsFieldNewDocsField(t *testing.T) {
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
	builder.Add(10, field.ValueUnion{
		Type:      field.StringType,
		StringVal: "cat",
	})

	expected := []string{"foo", "bar", "baz", "cat"}

	// Seal the builder.
	sealed := builder.Seal(15)

	// Close the builder.
	builder.Close()
	assertReturnedToStringArrayPool(t, stringArrayPool, expected, false)

	// Create a new docs field from the newly sealed docs field.
	newField, remainder, err := sealed.NewDocsFieldFor(field.ValueTypeSet{
		field.StringType: struct{}{},
	})
	require.NoError(t, err)
	require.Nil(t, remainder)

	expectedMeta := DocsFieldMetadata{
		FieldPath:  []string{"testPath"},
		FieldTypes: []field.ValueType{field.StringType},
	}
	require.Equal(t, expectedMeta, newField.Metadata())

	// Closing the sealed field should not cause the string field to be retruend to pool.
	sealed.Close()
	assertReturnedToStringArrayPool(t, stringArrayPool, expected, false)

	// Closing the new field should cause the string field to be retruend to pool.
	newField.Close()
	assertReturnedToStringArrayPool(t, stringArrayPool, expected, true)
}

func TestDocsFieldMergeInPlace(t *testing.T) {
	stringArrayBuckets1 := []pool.StringArrayBucket{
		{Capacity: 128, Count: 1},
	}
	stringArrayPool1 := pool.NewBucketizedStringArrayPool(stringArrayBuckets1, nil)
	stringArrayPool1.Init(func(capacity int) []string { return make([]string, 0, capacity) })

	opts1 := NewDocsFieldBuilderOptions().
		SetStringArrayPool(stringArrayPool1)
	builder1 := NewDocsFieldBuilder([]string{"testPath"}, opts1)

	// Add some string values.
	builder1.Add(1, field.ValueUnion{
		Type:      field.StringType,
		StringVal: "foo",
	})
	builder1.Add(3, field.ValueUnion{
		Type:      field.StringType,
		StringVal: "bar",
	})
	builder1.Add(6, field.ValueUnion{
		Type:      field.StringType,
		StringVal: "baz",
	})
	builder1.Add(10, field.ValueUnion{
		Type:      field.StringType,
		StringVal: "cat",
	})

	expected1 := []string{"foo", "bar", "baz", "cat"}

	// Seal the builder.
	sealed1 := builder1.Seal(15)

	// Close the builder.
	builder1.Close()
	assertReturnedToStringArrayPool(t, stringArrayPool1, expected1, false)

	stringArrayBuckets2 := []pool.StringArrayBucket{
		{Capacity: 128, Count: 1},
	}
	stringArrayPool2 := pool.NewBucketizedStringArrayPool(stringArrayBuckets2, nil)
	stringArrayPool2.Init(func(capacity int) []string { return make([]string, 0, capacity) })

	opts2 := NewDocsFieldBuilderOptions().
		SetStringArrayPool(stringArrayPool2)
	builder2 := NewDocsFieldBuilder([]string{"testPath"}, opts2)

	// Add some string values.
	builder2.Add(2, field.ValueUnion{
		Type:      field.StringType,
		StringVal: "quest",
	})
	builder2.Add(4, field.ValueUnion{
		Type:      field.StringType,
		StringVal: "blah",
	})
	builder2.Add(5, field.ValueUnion{
		Type:      field.StringType,
		StringVal: "raw",
	})
	builder2.Add(7, field.ValueUnion{
		Type:      field.StringType,
		StringVal: "cat",
	})

	expected2 := []string{"quest", "blah", "raw", "cat"}

	// Seal the builder.
	sealed2 := builder2.Seal(10)

	// Close the builder.
	builder2.Close()
	assertReturnedToStringArrayPool(t, stringArrayPool2, expected2, false)

	// Merge 2nd field into the 1st field.
	sealed1.MergeInPlace(sealed2)

	// Merging should cause the string array 1 to be returned to pool.
	assertReturnedToStringArrayPool(t, stringArrayPool1, expected1, true)

	// Closing the 2nd field should not cause string array 2 to be retruend to pool.
	sealed2.Close()
	assertReturnedToStringArrayPool(t, stringArrayPool2, expected2, false)

	// Closing the 1st field will ƒinally return string array 2 to pool.
	sealed1.Close()
	assertReturnedToStringArrayPool(t, stringArrayPool2, expected2, true)
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
