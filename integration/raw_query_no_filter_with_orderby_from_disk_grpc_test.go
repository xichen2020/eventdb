// +build integration

package integration

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/xichen2020/eventdb/document"
	"github.com/xichen2020/eventdb/query"

	"github.com/stretchr/testify/require"
)

func TestRawQueryNoFilterWithOrderByFromDiskGRPC(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	// Create server.
	cfg := loadConfig(t, testConfig1)
	ts := newTestServerSetup(t, cfg, nil)
	ts.dbOpts = ts.dbOpts.
		SetFilePathPrefix(os.TempDir()).
		SetSegmentUnloadAfterUnreadFor(time.Second).
		SetTickMinInterval(time.Second)
	defer ts.close(t)

	// Start the server.
	log := ts.dbOpts.InstrumentOptions().Logger()
	log.Info("testing raw query without filter with order by clauses via GRPC endpoints, triggering loading data from disk")
	require.NoError(t, ts.startServer())
	log.Info("server is now up")

	defer func() {
		// Stop the server.
		require.NoError(t, ts.stopServer())
		log.Info("server is now down")
	}()

	rawDocStrs := []string{
		`{"service":"testNamespace","@timestamp":"2019-01-22T13:25:42-08:00","st":true,"sid":1,"tt":"active","tz":-6,"v":1.5}`,
		`{"service":"testNamespace","@timestamp":"2019-01-22T13:26:42-08:00","st":true,"sid":1,"tt":"active","tz":-6,"v":1.5}`,
		`{"service":"testNamespace","@timestamp":"2019-01-22T13:27:42-08:00","st":true,"sid":1,"tt":"active","tz":-6,"v":1.5}`,
		`{"service":"testNamespace","@timestamp":"2019-01-22T13:28:42-08:00","st":true,"sid":1,"tt":"active","tz":-6,"v":1.5}`,
		`{"service":"testNamespace","@timestamp":"2019-01-22T13:29:42-08:00","st":true,"sid":1,"tt":"active","tz":-6,"v":1.5}`,
		`{"service":"testNamespace","@timestamp":"2019-01-22T13:30:42-08:00","st":true,"sid":2,"tt":"active","tz":-6,"v":1.5}`,
		`{"service":"testNamespace","@timestamp":"2019-01-22T13:31:42-08:00","st":true,"sid":2,"tt":"active","tz":-6,"v":1.5}`,
		`{"service":"testNamespace","@timestamp":"2019-01-22T13:32:42-08:00","st":true,"sid":2,"tt":"active","tz":-6,"v":1.5}`,
		`{"service":"testNamespace","@timestamp":"2019-01-22T13:33:42-08:00","st":true,"sid":2,"tt":"active","tz":-6,"v":1.5}`,
		`{"service":"testNamespace","@timestamp":"2019-01-22T13:34:42-08:00","st":true,"sid":2,"tt":"active","tz":-6,"v":1.5}`,
		`{"service":"testNamespace","@timestamp":"2019-01-22T13:35:42-08:00","st":false,"sid":3,"tt":"inactive","tz":-6,"v":15}`,
		`{"service":"testNamespace","@timestamp":"2019-01-22T13:36:42-08:00","st":false,"sid":3,"tt":"inactive","tz":-6,"v":15}`,
		`{"service":"testNamespace","@timestamp":"2019-01-22T13:37:42-08:00","st":false,"sid":3,"tt":"inactive","tz":-6,"v":15}`,
		`{"service":"testNamespace","@timestamp":"2019-01-22T13:38:42-08:00","st":false,"sid":3,"tt":"inactive","tz":-6,"v":15}`,
		`{"service":"testNamespace","@timestamp":"2019-01-22T13:39:42-08:00","st":false,"sid":3,"tt":"inactive","tz":-6,"v":15}`,
		`{"service":"testNamespace","@timestamp":"2019-01-22T13:40:42-08:00","st":false,"sid":4,"tt":"inactive","tz":-6,"v":15}`,
		`{"service":"testNamespace","@timestamp":"2019-01-22T13:41:42-08:00","st":false,"sid":4,"tt":"inactive","tz":-6,"v":15}`,
		`{"service":"testNamespace","@timestamp":"2019-01-22T13:42:42-08:00","st":false,"sid":4,"tt":"inactive","tz":-6,"v":15}`,
		`{"service":"testNamespace","@timestamp":"2019-01-22T13:43:42-08:00","st":false,"sid":4,"tt":"inactive","tz":-6,"v":15}`,
		`{"service":"testNamespace","@timestamp":"2019-01-22T13:44:42-08:00","st":false,"sid":4,"tt":"inactive","tz":-6,"v":15}`,
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
		rawQuery              query.UnparsedRawQuery
		expectedSortedResults []string
	}{
		{
			rawQuery: query.UnparsedRawQuery{
				Namespace: "testNamespace",
				StartTime: pInt64(1548115200),
				EndTime:   pInt64(1548201600),
				OrderBy: query.RawOrderBys{
					{
						Field: pBytes("@timestamp"),
						Order: pOrderBy(query.Ascending),
					},
				},
			},
			expectedSortedResults: []string{
				`{"service":"testNamespace","@timestamp":"2019-01-22T13:25:42-08:00","st":true,"sid":1,"tt":"active","tz":-6,"v":1.5}`,
				`{"service":"testNamespace","@timestamp":"2019-01-22T13:26:42-08:00","st":true,"sid":1,"tt":"active","tz":-6,"v":1.5}`,
				`{"service":"testNamespace","@timestamp":"2019-01-22T13:27:42-08:00","st":true,"sid":1,"tt":"active","tz":-6,"v":1.5}`,
				`{"service":"testNamespace","@timestamp":"2019-01-22T13:28:42-08:00","st":true,"sid":1,"tt":"active","tz":-6,"v":1.5}`,
				`{"service":"testNamespace","@timestamp":"2019-01-22T13:29:42-08:00","st":true,"sid":1,"tt":"active","tz":-6,"v":1.5}`,
				`{"service":"testNamespace","@timestamp":"2019-01-22T13:30:42-08:00","st":true,"sid":2,"tt":"active","tz":-6,"v":1.5}`,
				`{"service":"testNamespace","@timestamp":"2019-01-22T13:31:42-08:00","st":true,"sid":2,"tt":"active","tz":-6,"v":1.5}`,
				`{"service":"testNamespace","@timestamp":"2019-01-22T13:32:42-08:00","st":true,"sid":2,"tt":"active","tz":-6,"v":1.5}`,
				`{"service":"testNamespace","@timestamp":"2019-01-22T13:33:42-08:00","st":true,"sid":2,"tt":"active","tz":-6,"v":1.5}`,
				`{"service":"testNamespace","@timestamp":"2019-01-22T13:34:42-08:00","st":true,"sid":2,"tt":"active","tz":-6,"v":1.5}`,
				`{"service":"testNamespace","@timestamp":"2019-01-22T13:35:42-08:00","st":false,"sid":3,"tt":"inactive","tz":-6,"v":15}`,
				`{"service":"testNamespace","@timestamp":"2019-01-22T13:36:42-08:00","st":false,"sid":3,"tt":"inactive","tz":-6,"v":15}`,
				`{"service":"testNamespace","@timestamp":"2019-01-22T13:37:42-08:00","st":false,"sid":3,"tt":"inactive","tz":-6,"v":15}`,
				`{"service":"testNamespace","@timestamp":"2019-01-22T13:38:42-08:00","st":false,"sid":3,"tt":"inactive","tz":-6,"v":15}`,
				`{"service":"testNamespace","@timestamp":"2019-01-22T13:39:42-08:00","st":false,"sid":3,"tt":"inactive","tz":-6,"v":15}`,
				`{"service":"testNamespace","@timestamp":"2019-01-22T13:40:42-08:00","st":false,"sid":4,"tt":"inactive","tz":-6,"v":15}`,
				`{"service":"testNamespace","@timestamp":"2019-01-22T13:41:42-08:00","st":false,"sid":4,"tt":"inactive","tz":-6,"v":15}`,
				`{"service":"testNamespace","@timestamp":"2019-01-22T13:42:42-08:00","st":false,"sid":4,"tt":"inactive","tz":-6,"v":15}`,
				`{"service":"testNamespace","@timestamp":"2019-01-22T13:43:42-08:00","st":false,"sid":4,"tt":"inactive","tz":-6,"v":15}`,
				`{"service":"testNamespace","@timestamp":"2019-01-22T13:44:42-08:00","st":false,"sid":4,"tt":"inactive","tz":-6,"v":15}`,
			},
		},
		{
			rawQuery: query.UnparsedRawQuery{
				Namespace: "testNamespace",
				StartTime: pInt64(1548192900),
				EndTime:   pInt64(1548201600),
				OrderBy: query.RawOrderBys{
					{
						Field: pBytes("@timestamp"),
						Order: pOrderBy(query.Ascending),
					},
				},
			},
			expectedSortedResults: []string{
				`{"service":"testNamespace","@timestamp":"2019-01-22T13:35:42-08:00","st":false,"sid":3,"tt":"inactive","tz":-6,"v":15}`,
				`{"service":"testNamespace","@timestamp":"2019-01-22T13:36:42-08:00","st":false,"sid":3,"tt":"inactive","tz":-6,"v":15}`,
				`{"service":"testNamespace","@timestamp":"2019-01-22T13:37:42-08:00","st":false,"sid":3,"tt":"inactive","tz":-6,"v":15}`,
				`{"service":"testNamespace","@timestamp":"2019-01-22T13:38:42-08:00","st":false,"sid":3,"tt":"inactive","tz":-6,"v":15}`,
				`{"service":"testNamespace","@timestamp":"2019-01-22T13:39:42-08:00","st":false,"sid":3,"tt":"inactive","tz":-6,"v":15}`,
				`{"service":"testNamespace","@timestamp":"2019-01-22T13:40:42-08:00","st":false,"sid":4,"tt":"inactive","tz":-6,"v":15}`,
				`{"service":"testNamespace","@timestamp":"2019-01-22T13:41:42-08:00","st":false,"sid":4,"tt":"inactive","tz":-6,"v":15}`,
				`{"service":"testNamespace","@timestamp":"2019-01-22T13:42:42-08:00","st":false,"sid":4,"tt":"inactive","tz":-6,"v":15}`,
				`{"service":"testNamespace","@timestamp":"2019-01-22T13:43:42-08:00","st":false,"sid":4,"tt":"inactive","tz":-6,"v":15}`,
				`{"service":"testNamespace","@timestamp":"2019-01-22T13:44:42-08:00","st":false,"sid":4,"tt":"inactive","tz":-6,"v":15}`,
			},
		},
	}

	// Write data.
	client, err := ts.newGRPCClient()
	require.NoError(t, err)
	require.NoError(t, client.Write(context.Background(), b("testNamespace"), inputDocs))

	// Wait for db flush.
	time.Sleep(5 * time.Second)

	// Test queries.
	for _, test := range tests {
		res, err := client.QueryRaw(context.Background(), test.rawQuery)
		require.NoError(t, err)
		actual := convertRawResultToStrings(res.Raw)
		require.Equal(t, test.expectedSortedResults, actual)
	}
}
