// +build integration

package integration

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/xichen2020/eventdb/calculation"
	"github.com/xichen2020/eventdb/document"
	"github.com/xichen2020/eventdb/document/field"
	"github.com/xichen2020/eventdb/query"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGroupByQueryMultiKeyNoFilterOrderByGRPC(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	cfg := loadConfig(t, testConfig1)
	ts := newTestServerSetup(t, cfg, nil)
	defer ts.close(t)

	log := ts.dbOpts.InstrumentOptions().Logger()
	log.Info("testing groupby query w/ multi-keys, no filter with orderby via GRPC endpoints")
	require.NoError(t, ts.startServer())
	log.Info("server is now up")

	rawDocStrs := []string{
		`{"service":"testNamespace","@timestamp":"2019-01-22T13:25:42-08:00","st":true,"sid":{"foo":1,"bar":2},"tt":"active","tz":-6,"v":1.5}`,
		`{"service":"testNamespace","@timestamp":"2019-01-22T13:26:42-08:00","st":true,"sid":{"foo":1,"bar":2},"tt":"active","tz":-6,"v":1.5}`,
		`{"service":"testNamespace","@timestamp":"2019-01-22T13:27:42-08:00","st":true,"sid":{"foo":1,"bar":2},"tt":"active","tz":-6,"v":1.5}`,
		`{"service":"testNamespace","@timestamp":"2019-01-22T13:28:42-08:00","st":true,"sid":{"foo":1,"bar":2},"tt":"active","tz":-6,"v":1.5}`,
		`{"service":"testNamespace","@timestamp":"2019-01-22T13:29:42-08:00","st":true,"sid":{"foo":1,"bar":2},"tt":"active","tz":-6,"v":1.5}`,
		`{"service":"testNamespace","@timestamp":"2019-01-22T13:30:42-08:00","st":true,"sid":{"foo":2,"bar":4},"tt":"active","tz":-6,"v":1.5}`,
		`{"service":"testNamespace","@timestamp":"2019-01-22T13:31:42-08:00","st":true,"sid":{"foo":2,"bar":4},"tt":"active","tz":-6,"v":1.5}`,
		`{"service":"testNamespace","@timestamp":"2019-01-22T13:32:42-08:00","st":true,"sid":{"foo":2,"bar":4},"tt":"active","tz":-6,"v":1.5}`,
		`{"service":"testNamespace","@timestamp":"2019-01-22T13:33:42-08:00","st":true,"sid":{"foo":2,"bar":4},"tt":"active","tz":-6,"v":1.5}`,
		`{"service":"testNamespace","@timestamp":"2019-01-22T13:34:42-08:00","st":true,"sid":{"foo":2,"bar":4},"tt":"active","tz":-6,"v":1.5}`,
		`{"service":"testNamespace","@timestamp":"2019-01-22T13:35:42-08:00","st":false,"sid":{"foo":3,"bar":6},"tt":"inactive","tz":-6,"v":15}`,
		`{"service":"testNamespace","@timestamp":"2019-01-22T13:36:42-08:00","st":false,"sid":{"foo":3,"bar":6},"tt":"inactive","tz":-6,"v":15}`,
		`{"service":"testNamespace","@timestamp":"2019-01-22T13:37:42-08:00","st":false,"sid":{"foo":3,"bar":6},"tt":"inactive","tz":-6,"v":15}`,
		`{"service":"testNamespace","@timestamp":"2019-01-22T13:38:42-08:00","st":false,"sid":{"foo":3,"bar":6},"tt":"inactive","tz":-6,"v":15}`,
		`{"service":"testNamespace","@timestamp":"2019-01-22T13:39:42-08:00","st":false,"sid":{"foo":3,"bar":6},"tt":"inactive","tz":-6,"v":15}`,
		`{"service":"testNamespace","@timestamp":"2019-01-22T13:40:42-08:00","st":false,"sid":{"foo":4,"bar":8},"tt":"inactive","tz":-6,"v":15}`,
		`{"service":"testNamespace","@timestamp":"2019-01-22T13:41:42-08:00","st":false,"sid":{"foo":4,"bar":8},"tt":"inactive","tz":-6,"v":15}`,
		`{"service":"testNamespace","@timestamp":"2019-01-22T13:42:42-08:00","st":false,"sid":{"foo":4,"bar":8},"tt":"inactive","tz":-6,"v":15}`,
		`{"service":"testNamespace","@timestamp":"2019-01-22T13:43:42-08:00","st":false,"sid":{"foo":4,"bar":8},"tt":"inactive","tz":-6,"v":15}`,
		`{"service":"testNamespace","@timestamp":"2019-01-22T13:44:42-08:00","st":false,"sid":{"foo":4,"bar":8},"tt":"inactive","tz":-6,"v":15}`,
	}
	var (
		timestampFieldPath = []string{"@timestamp"}
		timestampFormat    = time.RFC3339
		inputDocs          = make([]document.Document, 0, len(rawDocStrs))
	)
	for i, rawDocStr := range rawDocStrs {
		doc, err := newDocumentFromRaw("doc"+strconv.Itoa(i), rawDocStr, timestampFieldPath, timestampFormat)
		require.NoError(t, err)
		inputDocs = append(inputDocs, doc)
	}

	tests := []struct {
		groupedQuery    query.UnparsedGroupedQuery
		expectedResults query.MultiKeyGroupQueryResults
	}{
		{
			groupedQuery: query.UnparsedGroupedQuery{
				Namespace: "testNamespace",
				StartTime: pInt64(1548115200),
				EndTime:   pInt64(1548201600),
				Calculations: query.RawCalculations{
					{
						Op: calculation.Count,
					},
				},
				GroupBy: []string{
					"st",
					"tt",
				},
				OrderBy: query.RawOrderBys{
					{
						Field: pString("st"),
						Order: pOrderBy(query.Ascending),
					},
				},
			},
			expectedResults: query.MultiKeyGroupQueryResults{
				Groups: []query.MultiKeyGroupQueryResult{
					{
						Key: field.Values{
							{Type: field.BoolType, BoolVal: false},
							{Type: field.StringType, StringVal: "inactive"},
						},
						Values: calculation.Values{
							{Type: calculation.NumberType, NumberVal: float64(10)},
						},
					},
					{
						Key: field.Values{
							{Type: field.BoolType, BoolVal: true},
							{Type: field.StringType, StringVal: "active"},
						},
						Values: calculation.Values{
							{Type: calculation.NumberType, NumberVal: float64(10)},
						},
					},
				},
			},
		},
		{
			groupedQuery: query.UnparsedGroupedQuery{
				Namespace: "testNamespace",
				StartTime: pInt64(1548115200),
				EndTime:   pInt64(1548201600),
				Calculations: query.RawCalculations{
					{
						Op: calculation.Count,
					},
				},
				GroupBy: []string{
					"sid.foo",
					"tt",
				},
				OrderBy: query.RawOrderBys{
					{
						Field: pString("sid.foo"),
						Order: pOrderBy(query.Ascending),
					},
				},
			},
			expectedResults: query.MultiKeyGroupQueryResults{
				Groups: []query.MultiKeyGroupQueryResult{
					{
						Key: field.Values{
							{Type: field.IntType, IntVal: 1},
							{Type: field.StringType, StringVal: "active"},
						},
						Values: calculation.Values{
							{Type: calculation.NumberType, NumberVal: float64(5)},
						},
					},
					{
						Key: field.Values{
							{Type: field.IntType, IntVal: 2},
							{Type: field.StringType, StringVal: "active"},
						},
						Values: calculation.Values{
							{Type: calculation.NumberType, NumberVal: float64(5)},
						},
					},
					{
						Key: field.Values{
							{Type: field.IntType, IntVal: 3},
							{Type: field.StringType, StringVal: "inactive"},
						},
						Values: calculation.Values{
							{Type: calculation.NumberType, NumberVal: float64(5)},
						},
					},
					{
						Key: field.Values{
							{Type: field.IntType, IntVal: 4},
							{Type: field.StringType, StringVal: "inactive"},
						},
						Values: calculation.Values{
							{Type: calculation.NumberType, NumberVal: float64(5)},
						},
					},
				},
			},
		},
	}

	client, err := ts.newGRPCClient()
	require.NoError(t, err)
	require.NoError(t, client.Write(context.Background(), b("testNamespace"), inputDocs))

	for _, test := range tests {
		results, err := client.QueryGrouped(context.Background(), test.groupedQuery)
		assert.NoError(t, err)
		assert.Equal(t, test.expectedResults, *results.MultiKey)
	}

	require.NoError(t, ts.stopServer())
	log.Info("server is now down")
}
