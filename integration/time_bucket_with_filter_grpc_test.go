// +build integration

package integration

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/xichen2020/eventdb/document"
	"github.com/xichen2020/eventdb/filter"
	"github.com/xichen2020/eventdb/query"
	xtime "github.com/xichen2020/eventdb/x/time"

	m3xtime "github.com/m3db/m3/src/x/time"
	"github.com/stretchr/testify/require"
)

func TestTimeBucketQueryWithFilterGRPC(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	// Create server.
	cfg := loadConfig(t, testConfig1)
	ts := newTestServerSetup(t, cfg, nil)
	defer ts.close(t)

	// Start the server.
	log := ts.dbOpts.InstrumentOptions().Logger()
	log.Info("testing time bucket query with filter clauses via GRPC endpoints")
	require.NoError(t, ts.startServer())
	log.Info("server is now up")

	defer func() {
		// Stop the server.
		require.NoError(t, ts.stopServer())
		log.Info("server is now down")
	}()

	rawDocStrs := []string{
		`{"service":"testNamespace","@timestamp":"2019-01-22T13:25:42-08:00","st":true,"sid":1,"tt":"active","tz":-6,"v":1.5}`,
		`{"service":"testNamespace","@timestamp":"2019-01-22T13:26:42-08:00","st":true,"sid":2,"tt":"active","tz":-6,"v":1.5}`,
		`{"service":"testNamespace","@timestamp":"2019-01-22T13:27:42-08:00","st":true,"sid":3,"tt":"active","tz":-6,"v":1.5}`,
		`{"service":"testNamespace","@timestamp":"2019-01-22T13:28:42-08:00","st":true,"sid":4,"tt":"active","tz":-6,"v":1.5}`,
		`{"service":"testNamespace","@timestamp":"2019-01-22T13:29:42-08:00","st":true,"sid":5,"tt":"active","tz":-6,"v":1.5}`,
		`{"service":"testNamespace","@timestamp":"2019-01-22T13:30:42-08:00","st":true,"sid":1,"tt":"active","tz":-6,"v":1.5}`,
		`{"service":"testNamespace","@timestamp":"2019-01-22T13:31:42-08:00","st":true,"sid":2,"tt":"active","tz":-6,"v":1.5}`,
		`{"service":"testNamespace","@timestamp":"2019-01-22T13:32:42-08:00","st":true,"sid":3,"tt":"active","tz":-6,"v":1.5}`,
		`{"service":"testNamespace","@timestamp":"2019-01-22T13:33:42-08:00","st":true,"sid":4,"tt":"active","tz":-6,"v":1.5}`,
		`{"service":"testNamespace","@timestamp":"2019-01-22T13:34:42-08:00","st":true,"sid":5,"tt":"active","tz":-6,"v":1.5}`,
		`{"service":"testNamespace","@timestamp":"2019-01-22T13:35:42-08:00","st":false,"sid":1,"tt":"inactive","tz":-6,"v":15}`,
		`{"service":"testNamespace","@timestamp":"2019-01-22T13:36:42-08:00","st":false,"sid":2,"tt":"inactive","tz":-6,"v":15}`,
		`{"service":"testNamespace","@timestamp":"2019-01-22T13:37:42-08:00","st":false,"sid":3,"tt":"inactive","tz":-6,"v":15}`,
		`{"service":"testNamespace","@timestamp":"2019-01-22T13:38:42-08:00","st":false,"sid":4,"tt":"inactive","tz":-6,"v":15}`,
		`{"service":"testNamespace","@timestamp":"2019-01-22T13:39:42-08:00","st":false,"sid":5,"tt":"inactive","tz":-6,"v":15}`,
		`{"service":"testNamespace","@timestamp":"2019-01-22T13:40:42-08:00","st":false,"sid":1,"tt":"inactive","tz":-6,"v":15}`,
		`{"service":"testNamespace","@timestamp":"2019-01-22T13:41:42-08:00","st":false,"sid":2,"tt":"inactive","tz":-6,"v":15}`,
		`{"service":"testNamespace","@timestamp":"2019-01-22T13:42:42-08:00","st":false,"sid":3,"tt":"inactive","tz":-6,"v":15}`,
		`{"service":"testNamespace","@timestamp":"2019-01-22T13:43:42-08:00","st":false,"sid":4,"tt":"inactive","tz":-6,"v":15}`,
		`{"service":"testNamespace","@timestamp":"2019-01-22T13:44:42-08:00","st":false,"sid":5,"tt":"inactive","tz":-6,"v":15}`,
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
		timeBucketQuery query.UnparsedTimeBucketQuery
		expectedResults query.TimeBucketQueryResults
	}{
		{
			timeBucketQuery: query.UnparsedTimeBucketQuery{
				Namespace:       "testNamespace",
				StartTime:       pInt64(1548190000),
				EndTime:         pInt64(1548196000),
				TimeUnit:        pTimeUnit(xtime.Unit(m3xtime.Second)),
				TimeGranularity: pDuration(xtime.Duration(10 * time.Minute)),
				Filters: query.RawFilterLists{
					{
						Filters: query.RawFilters{
							{
								Field: "tz",
								Op:    filter.Equals,
								Value: -6,
							},
							{
								Field: "sid",
								Op:    filter.Equals,
								Value: 2,
							},
						},
						FilterCombinator: pFilterCombinator(filter.And),
					},
				},
			},
			expectedResults: query.TimeBucketQueryResults{
				GranularityNanos: 600000000000,
				Buckets: []query.TimeBucketQueryResult{
					{StartAtNanos: 1548189600000000000, Value: 0},
					{StartAtNanos: 1548190200000000000, Value: 0},
					{StartAtNanos: 1548190800000000000, Value: 0},
					{StartAtNanos: 1548191400000000000, Value: 0},
					{StartAtNanos: 1548192000000000000, Value: 1},
					{StartAtNanos: 1548192600000000000, Value: 2},
					{StartAtNanos: 1548193200000000000, Value: 1},
					{StartAtNanos: 1548193800000000000, Value: 0},
					{StartAtNanos: 1548194400000000000, Value: 0},
					{StartAtNanos: 1548195000000000000, Value: 0},
					{StartAtNanos: 1548195600000000000, Value: 0},
				},
			},
		},
		{
			timeBucketQuery: query.UnparsedTimeBucketQuery{
				Namespace:       "testNamespace",
				StartTime:       pInt64(1548190000),
				EndTime:         pInt64(1548196000),
				TimeUnit:        pTimeUnit(xtime.Unit(m3xtime.Second)),
				TimeGranularity: pDuration(xtime.Duration(10 * time.Minute)),
				Filters: query.RawFilterLists{
					{
						Filters: query.RawFilters{
							{
								Field: "tt",
								Op:    filter.Equals,
								Value: "active",
							},
						},
					},
				},
			},
			expectedResults: query.TimeBucketQueryResults{
				GranularityNanos: 600000000000,
				Buckets: []query.TimeBucketQueryResult{
					{StartAtNanos: 1548189600000000000, Value: 0},
					{StartAtNanos: 1548190200000000000, Value: 0},
					{StartAtNanos: 1548190800000000000, Value: 0},
					{StartAtNanos: 1548191400000000000, Value: 0},
					{StartAtNanos: 1548192000000000000, Value: 5},
					{StartAtNanos: 1548192600000000000, Value: 5},
					{StartAtNanos: 1548193200000000000, Value: 0},
					{StartAtNanos: 1548193800000000000, Value: 0},
					{StartAtNanos: 1548194400000000000, Value: 0},
					{StartAtNanos: 1548195000000000000, Value: 0},
					{StartAtNanos: 1548195600000000000, Value: 0},
				},
			},
		},
	}

	// Write data.
	client, err := ts.newGRPCClient()
	require.NoError(t, err)
	require.NoError(t, client.Write(context.Background(), b("testNamespace"), inputDocs))

	// Test queries.
	for _, test := range tests {
		res, err := client.QueryTimeBucket(context.Background(), test.timeBucketQuery)
		require.NoError(t, err)
		require.Equal(t, test.expectedResults, *res)
	}
}
