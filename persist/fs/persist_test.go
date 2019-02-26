package fs

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/xichen2020/eventdb/document/field"
	indexfield "github.com/xichen2020/eventdb/index/field"
	"github.com/xichen2020/eventdb/persist"
)

const (
	testShardID            = uint32(0)
	testSegmentID          = "test-segment-id"
	testFilePathPrefix     = "./testdata"
	testTimestampPrecision = time.Nanosecond
)

var (
	testNamespace = []byte("test-namespace")

	f0FieldPath = []string{"timestamp"}
	f0ValueType = field.TimeType
	f1FieldPath = []string{"service"}
	f1ValueType = field.StringType
	f2FieldPath = []string{"fields", "int_field"}
	f2ValueType = field.IntType
	f3FieldPath = []string{"fields", "bool_field"}
	f3ValueType = field.BoolType

	f0 = createDocsField(f0FieldPath, []field.ValueUnion{
		{Type: f0ValueType, TimeNanosVal: time.Now().UnixNano()},
		{Type: f0ValueType, TimeNanosVal: time.Now().UnixNano()},
		{Type: f0ValueType, TimeNanosVal: time.Now().UnixNano()},
		{Type: f0ValueType, TimeNanosVal: time.Now().UnixNano()},
	})
	f1 = createDocsField(f1FieldPath, []field.ValueUnion{
		{Type: f1ValueType, StringVal: "foo1"},
		{Type: f1ValueType, StringVal: "foo2"},
		{Type: f1ValueType, StringVal: "foo3"},
		{Type: f1ValueType, StringVal: "foo4"},
	})
	f2 = createDocsField(f2FieldPath, []field.ValueUnion{
		{Type: f2ValueType, IntVal: 1},
		{Type: f2ValueType, IntVal: 2},
		{Type: f2ValueType, IntVal: 3},
		{Type: f2ValueType, IntVal: 4},
	})
	f3 = createDocsField(f3FieldPath, []field.ValueUnion{
		{Type: f3ValueType, BoolVal: true},
		{Type: f3ValueType, BoolVal: true},
		{Type: f3ValueType, BoolVal: false},
		{Type: f3ValueType, BoolVal: false},
	})
)

func createDocsField(
	fieldPath []string,
	values []field.ValueUnion,
) indexfield.DocsField {
	b := indexfield.NewDocsFieldBuilder(fieldPath, nil)
	for docID, v := range values {
		if err := b.Add(int32(docID), v); err != nil {
			panic(err)
		}
	}
	return b.Seal(int32(len(values)))
}

func writeFields(fields []indexfield.DocsField) error {
	opts := NewOptions()
	opts = opts.SetFilePathPrefix(testFilePathPrefix)
	opts = opts.SetTimestampPrecision(testTimestampPrecision)
	pm := NewPersistManager(opts)

	ps, err := pm.StartPersist()
	if err != nil {
		return err
	}
	defer ps.Finish()

	prepareOpts := persist.PrepareOptions{
		Namespace: testNamespace,
		Shard:     testShardID,
		SegmentMeta: persist.SegmentMetadata{
			ID: testSegmentID,
		},
	}
	prepared, err := ps.Prepare(prepareOpts)
	if err != nil {
		return err
	}
	defer prepared.Close()

	return prepared.Persist.WriteFields(fields)
}

func retrieveFields(fields []persist.RetrieveFieldOptions) ([]indexfield.DocsField, error) {
	opts := NewOptions()
	opts = opts.SetFilePathPrefix(testFilePathPrefix)
	opts = opts.SetTimestampPrecision(testTimestampPrecision)
	fr := NewFieldRetriever(opts)
	segmentMeta := persist.SegmentMetadata{
		ID: testSegmentID,
	}
	return fr.RetrieveFields(testNamespace, testShardID, segmentMeta, fields)
}

func TestWriteAndRetrieveFields(t *testing.T) {
	err := writeFields([]indexfield.DocsField{f0, f1, f2, f3})
	require.NoError(t, err)

	fields, err := retrieveFields([]persist.RetrieveFieldOptions{
		{FieldPath: f0FieldPath, FieldTypes: field.ValueTypeSet{f0ValueType: struct{}{}}},
		{FieldPath: f1FieldPath, FieldTypes: field.ValueTypeSet{f1ValueType: struct{}{}}},
		{FieldPath: f2FieldPath, FieldTypes: field.ValueTypeSet{f2ValueType: struct{}{}}},
		{FieldPath: f3FieldPath, FieldTypes: field.ValueTypeSet{f3ValueType: struct{}{}}},
	})
	require.NoError(t, err)

	f0Actual, ok := fields[0].TimeField()
	require.True(t, ok)
	f0Expected, ok := f0.TimeField()
	require.True(t, ok)
	f0Equals, err := timeFieldEquals(f0Expected, f0Actual)
	require.NoError(t, err)
	require.True(t, f0Equals)

	f1Actual, ok := fields[1].StringField()
	require.True(t, ok)
	f1Expected, ok := f1.StringField()
	require.True(t, ok)
	f1Equals, err := stringFieldEquals(f1Expected, f1Actual)
	require.NoError(t, err)
	require.True(t, f1Equals)

	f2Actual, ok := fields[2].IntField()
	require.True(t, ok)
	f2Expected, ok := f2.IntField()
	require.True(t, ok)
	f2Equals, err := intFieldEquals(f2Expected, f2Actual)
	require.NoError(t, err)
	require.True(t, f2Equals)

	f3Actual, ok := fields[3].BoolField()
	require.True(t, ok)
	f3Expected, ok := f3.BoolField()
	require.True(t, ok)
	f3Equals, err := boolFieldEquals(f3Expected, f3Actual)
	require.NoError(t, err)
	require.True(t, f3Equals)

	// Remove data directory entirely.
	err = os.RemoveAll(testFilePathPrefix)
	require.NoError(t, err)
}
