// +build integration

package integration

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTimeBucketQueryWithFilter(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	// Create server.
	ts := newTestServerSetup(t, testConfig1)
	defer ts.close(t)

	// Start the server.
	log := ts.dbOpts.InstrumentOptions().Logger()
	log.Info("testing time bucket query with filter clauses")
	require.NoError(t, ts.startServer())
	log.Info("server is now up")

	testData := `
{"service":"testNamespace","@timestamp":"2019-01-22T13:25:42-08:00","st":true,"sid":1,"tt":"active","tz":-6,"v":1.5}
{"service":"testNamespace","@timestamp":"2019-01-22T13:26:42-08:00","st":true,"sid":2,"tt":"active","tz":-6,"v":1.5}
{"service":"testNamespace","@timestamp":"2019-01-22T13:27:42-08:00","st":true,"sid":3,"tt":"active","tz":-6,"v":1.5}
{"service":"testNamespace","@timestamp":"2019-01-22T13:28:42-08:00","st":true,"sid":4,"tt":"active","tz":-6,"v":1.5}
{"service":"testNamespace","@timestamp":"2019-01-22T13:29:42-08:00","st":true,"sid":5,"tt":"active","tz":-6,"v":1.5}
{"service":"testNamespace","@timestamp":"2019-01-22T13:30:42-08:00","st":true,"sid":1,"tt":"active","tz":-6,"v":1.5}
{"service":"testNamespace","@timestamp":"2019-01-22T13:31:42-08:00","st":true,"sid":2,"tt":"active","tz":-6,"v":1.5}
{"service":"testNamespace","@timestamp":"2019-01-22T13:32:42-08:00","st":true,"sid":3,"tt":"active","tz":-6,"v":1.5}
{"service":"testNamespace","@timestamp":"2019-01-22T13:33:42-08:00","st":true,"sid":4,"tt":"active","tz":-6,"v":1.5}
{"service":"testNamespace","@timestamp":"2019-01-22T13:34:42-08:00","st":true,"sid":5,"tt":"active","tz":-6,"v":1.5}
{"service":"testNamespace","@timestamp":"2019-01-22T13:35:42-08:00","st":false,"sid":1,"tt":"inactive","tz":-6,"v":15}
{"service":"testNamespace","@timestamp":"2019-01-22T13:36:42-08:00","st":false,"sid":2,"tt":"inactive","tz":-6,"v":15}
{"service":"testNamespace","@timestamp":"2019-01-22T13:37:42-08:00","st":false,"sid":3,"tt":"inactive","tz":-6,"v":15}
{"service":"testNamespace","@timestamp":"2019-01-22T13:38:42-08:00","st":false,"sid":4,"tt":"inactive","tz":-6,"v":15}
{"service":"testNamespace","@timestamp":"2019-01-22T13:39:42-08:00","st":false,"sid":5,"tt":"inactive","tz":-6,"v":15}
{"service":"testNamespace","@timestamp":"2019-01-22T13:40:42-08:00","st":false,"sid":1,"tt":"inactive","tz":-6,"v":15}
{"service":"testNamespace","@timestamp":"2019-01-22T13:41:42-08:00","st":false,"sid":2,"tt":"inactive","tz":-6,"v":15}
{"service":"testNamespace","@timestamp":"2019-01-22T13:42:42-08:00","st":false,"sid":3,"tt":"inactive","tz":-6,"v":15}
{"service":"testNamespace","@timestamp":"2019-01-22T13:43:42-08:00","st":false,"sid":4,"tt":"inactive","tz":-6,"v":15}
{"service":"testNamespace","@timestamp":"2019-01-22T13:44:42-08:00","st":false,"sid":5,"tt":"inactive","tz":-6,"v":15}`

	tests := []struct {
		queryJSON       string
		expectedResults timeBucketQueryResults
	}{
		{
			queryJSON: `
        {
          "namespace":  "testNamespace",
          "start_time": 1548190000,
          "end_time":   1548196000,
          "time_unit":  "1s",
          "time_granularity": "10m",
          "filters": [
            {
              "filters": [
                {
                  "field": "tz",
                  "op": "=",
                  "value": -6
                },
                {
                  "field": "sid",
                  "op": "=",
                  "value": 2
                }
              ],
              "filter_combinator": "AND"
            }
          ]
        }
      `,
			expectedResults: timeBucketQueryResults{
				Granularity: 600000000000,
				Buckets: []bucket{
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
			queryJSON: `
        {
          "namespace":  "testNamespace",
          "start_time": 1548190000,
          "end_time":   1548196000,
          "time_unit":  "1s",
          "time_granularity": "10m",
          "filters": [
            {
              "filters": [
                {
                  "field": "tt",
                  "op": "=",
                  "value": "active"
                }
              ]
            }
          ]
        }
      `,
			expectedResults: timeBucketQueryResults{
				Granularity: 600000000000,
				Buckets: []bucket{
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
	client := ts.newHTTPClient()
	require.NoError(t, client.write([]byte(strings.TrimSpace(testData))))

	// Test queries.
	for _, test := range tests {
		actual, err := client.queryTimeBucket([]byte(test.queryJSON))
		require.NoError(t, err)
		require.Equal(t, test.expectedResults, actual)
	}

	// Stop the server.
	require.NoError(t, ts.stopServer())
	log.Info("server is now down")
}
