package executor

import (
	"testing"

	"github.com/xichen2020/eventdb/document/field"
	"github.com/xichen2020/eventdb/filter"
	"github.com/xichen2020/eventdb/index"
	indexfield "github.com/xichen2020/eventdb/index/field"
	"github.com/xichen2020/eventdb/query"
	"github.com/xichen2020/eventdb/values"
	"github.com/xichen2020/eventdb/values/iterator"
	"github.com/xichen2020/eventdb/x/bytes"

	gomock "github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestApplyFilters(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		startNanosInclusive = int64(0)
		endNanosExclusive   = int64(3456)
		filters             = []query.FilterList{
			{
				Filters: []query.Filter{
					{
						FieldPath: []string{"field1"},
						Op:        filter.Equals,
						Value: &field.ValueUnion{
							Type:     field.BytesType,
							BytesVal: bytes.NewImmutableBytes([]byte("foo")),
						},
					},
				},
				FilterCombinator: filter.And,
			},
		}
		allowedFieldTypes = []field.ValueTypeSet{
			{
				field.TimeType: struct{}{},
			},
			{
				field.BytesType: struct{}{},
			},
		}
		timestampFieldIdx = 0
		filterStartIdx    = 1
		fieldIndexMap     = []int{0, 1}
		numTotalDocs      = int32(5000)
	)

	timestampDocIDSetIter := index.NewMockDocIDSetIterator(ctrl)
	timestampDocIDSet := index.NewMockDocIDSet(ctrl)
	timestampDocIDSet.EXPECT().Iter().Return(timestampDocIDSetIter).MinTimes(1)
	timestampValuesMetadata := values.TimeValuesMetadata{
		Min: 1234,
		Max: 5678,
	}
	timestampValuesIter := iterator.NewMockForwardTimeIterator(ctrl)
	timestampValues := values.NewMockTimeValues(ctrl)
	timestampValues.EXPECT().Metadata().Return(timestampValuesMetadata).MinTimes(1)
	timestampValues.EXPECT().Iter().Return(timestampValuesIter, nil).MinTimes(1)
	timestampField := indexfield.NewMockCloseableTimeField(ctrl)
	timestampField.EXPECT().DocIDSet().Return(timestampDocIDSet)
	timestampField.EXPECT().Values().Return(timestampValues).MinTimes(1)

	queryField1 := indexfield.NewMockDocsField(ctrl)
	queryField1.EXPECT().TimeField().Return(timestampField, true).MinTimes(1)

	toFilterDocIDIter := index.NewMockDocIDSetIterator(ctrl)
	toFilterField := indexfield.NewMockDocsField(ctrl)
	toFilterField.EXPECT().Filter(filter.Equals, gomock.Any(), numTotalDocs).Return(toFilterDocIDIter, nil)
	toFilterField.EXPECT().Close()
	queryField2 := indexfield.NewMockDocsField(ctrl)
	queryField2.EXPECT().NewDocsFieldFor(allowedFieldTypes[1]).Return(toFilterField, nil, nil)
	queryFields := []indexfield.DocsField{queryField1, queryField2}

	_, err := applyFilters(
		startNanosInclusive, endNanosExclusive, filters, allowedFieldTypes,
		timestampFieldIdx, filterStartIdx, fieldIndexMap, queryFields, numTotalDocs,
	)
	require.NoError(t, err)
}
