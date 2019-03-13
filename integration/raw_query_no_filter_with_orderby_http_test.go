// +build integration

package integration

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRawQueryNoFilterWithOrderByHTTP(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	// Create server.
	cfg := loadConfig(t, testConfig1)
	ts := newTestServerSetup(t, cfg, nil)
	defer ts.close(t)

	// Start the server.
	log := ts.dbOpts.InstrumentOptions().Logger()
	log.Info("testing raw query without filter with order by clauses via HTTP endpoints")
	require.NoError(t, ts.startServer())
	log.Info("server is now up")

	defer func() {
		// Stop the server.
		require.NoError(t, ts.stopServer())
		log.Info("server is now down")
	}()

	testData := `
{"service":"testNamespace","@timestamp":"2019-01-22T13:25:42-08:00","st":true,"sid":1,"tt":"active","tz":-6,"v":1.5}
{"service":"testNamespace","@timestamp":"2019-01-22T13:26:42-08:00","st":true,"sid":1,"tt":"active","tz":-6,"v":1.5}
{"service":"testNamespace","@timestamp":"2019-01-22T13:27:42-08:00","st":true,"sid":1,"tt":"active","tz":-6,"v":1.5}
{"service":"testNamespace","@timestamp":"2019-01-22T13:28:42-08:00","st":true,"sid":1,"tt":"active","tz":-6,"v":1.5}
{"service":"testNamespace","@timestamp":"2019-01-22T13:29:42-08:00","st":true,"sid":1,"tt":"active","tz":-6,"v":1.5}
{"service":"testNamespace","@timestamp":"2019-01-22T13:30:42-08:00","st":true,"sid":2,"tt":"active","tz":-6,"v":1.5}
{"service":"testNamespace","@timestamp":"2019-01-22T13:31:42-08:00","st":true,"sid":2,"tt":"active","tz":-6,"v":1.5}
{"service":"testNamespace","@timestamp":"2019-01-22T13:32:42-08:00","st":true,"sid":2,"tt":"active","tz":-6,"v":1.5}
{"service":"testNamespace","@timestamp":"2019-01-22T13:33:42-08:00","st":true,"sid":2,"tt":"active","tz":-6,"v":1.5}
{"service":"testNamespace","@timestamp":"2019-01-22T13:34:42-08:00","st":true,"sid":2,"tt":"active","tz":-6,"v":1.5}
{"service":"testNamespace","@timestamp":"2019-01-22T13:35:42-08:00","st":false,"sid":3,"tt":"inactive","tz":-6,"v":15}
{"service":"testNamespace","@timestamp":"2019-01-22T13:36:42-08:00","st":false,"sid":3,"tt":"inactive","tz":-6,"v":15}
{"service":"testNamespace","@timestamp":"2019-01-22T13:37:42-08:00","st":false,"sid":3,"tt":"inactive","tz":-6,"v":15}
{"service":"testNamespace","@timestamp":"2019-01-22T13:38:42-08:00","st":false,"sid":3,"tt":"inactive","tz":-6,"v":15}
{"service":"testNamespace","@timestamp":"2019-01-22T13:39:42-08:00","st":false,"sid":3,"tt":"inactive","tz":-6,"v":15}
{"service":"testNamespace","@timestamp":"2019-01-22T13:40:42-08:00","st":false,"sid":4,"tt":"inactive","tz":-6,"v":15}
{"service":"testNamespace","@timestamp":"2019-01-22T13:41:42-08:00","st":false,"sid":4,"tt":"inactive","tz":-6,"v":15}
{"service":"testNamespace","@timestamp":"2019-01-22T13:42:42-08:00","st":false,"sid":4,"tt":"inactive","tz":-6,"v":15}
{"service":"testNamespace","@timestamp":"2019-01-22T13:43:42-08:00","st":false,"sid":4,"tt":"inactive","tz":-6,"v":15}
{"service":"testNamespace","@timestamp":"2019-01-22T13:44:42-08:00","st":false,"sid":4,"tt":"inactive","tz":-6,"v":15}`

	tests := []struct {
		queryJSON             string
		expectedSortedResults [][]byte
	}{
		{
			queryJSON: `
				{
					"namespace":  "testNamespace",
					"start_time": 1548115200,
					"end_time":   1548201600,
					"order_by": [
						{
							"field": "@timestamp",
							"order": "ascending"
						}
					]
				}
			`,
			expectedSortedResults: [][]byte{
				b(`{"service":"testNamespace","@timestamp":"2019-01-22T13:25:42-08:00","st":true,"sid":1,"tt":"active","tz":-6,"v":1.5}`),
				b(`{"service":"testNamespace","@timestamp":"2019-01-22T13:26:42-08:00","st":true,"sid":1,"tt":"active","tz":-6,"v":1.5}`),
				b(`{"service":"testNamespace","@timestamp":"2019-01-22T13:27:42-08:00","st":true,"sid":1,"tt":"active","tz":-6,"v":1.5}`),
				b(`{"service":"testNamespace","@timestamp":"2019-01-22T13:28:42-08:00","st":true,"sid":1,"tt":"active","tz":-6,"v":1.5}`),
				b(`{"service":"testNamespace","@timestamp":"2019-01-22T13:29:42-08:00","st":true,"sid":1,"tt":"active","tz":-6,"v":1.5}`),
				b(`{"service":"testNamespace","@timestamp":"2019-01-22T13:30:42-08:00","st":true,"sid":2,"tt":"active","tz":-6,"v":1.5}`),
				b(`{"service":"testNamespace","@timestamp":"2019-01-22T13:31:42-08:00","st":true,"sid":2,"tt":"active","tz":-6,"v":1.5}`),
				b(`{"service":"testNamespace","@timestamp":"2019-01-22T13:32:42-08:00","st":true,"sid":2,"tt":"active","tz":-6,"v":1.5}`),
				b(`{"service":"testNamespace","@timestamp":"2019-01-22T13:33:42-08:00","st":true,"sid":2,"tt":"active","tz":-6,"v":1.5}`),
				b(`{"service":"testNamespace","@timestamp":"2019-01-22T13:34:42-08:00","st":true,"sid":2,"tt":"active","tz":-6,"v":1.5}`),
				b(`{"service":"testNamespace","@timestamp":"2019-01-22T13:35:42-08:00","st":false,"sid":3,"tt":"inactive","tz":-6,"v":15}`),
				b(`{"service":"testNamespace","@timestamp":"2019-01-22T13:36:42-08:00","st":false,"sid":3,"tt":"inactive","tz":-6,"v":15}`),
				b(`{"service":"testNamespace","@timestamp":"2019-01-22T13:37:42-08:00","st":false,"sid":3,"tt":"inactive","tz":-6,"v":15}`),
				b(`{"service":"testNamespace","@timestamp":"2019-01-22T13:38:42-08:00","st":false,"sid":3,"tt":"inactive","tz":-6,"v":15}`),
				b(`{"service":"testNamespace","@timestamp":"2019-01-22T13:39:42-08:00","st":false,"sid":3,"tt":"inactive","tz":-6,"v":15}`),
				b(`{"service":"testNamespace","@timestamp":"2019-01-22T13:40:42-08:00","st":false,"sid":4,"tt":"inactive","tz":-6,"v":15}`),
				b(`{"service":"testNamespace","@timestamp":"2019-01-22T13:41:42-08:00","st":false,"sid":4,"tt":"inactive","tz":-6,"v":15}`),
				b(`{"service":"testNamespace","@timestamp":"2019-01-22T13:42:42-08:00","st":false,"sid":4,"tt":"inactive","tz":-6,"v":15}`),
				b(`{"service":"testNamespace","@timestamp":"2019-01-22T13:43:42-08:00","st":false,"sid":4,"tt":"inactive","tz":-6,"v":15}`),
				b(`{"service":"testNamespace","@timestamp":"2019-01-22T13:44:42-08:00","st":false,"sid":4,"tt":"inactive","tz":-6,"v":15}`),
			},
		},
		{
			queryJSON: `
				{
					"namespace":  "testNamespace",
					"start_time": 1548192900,
					"end_time":   1548201600,
					"order_by": [
						{
							"field": "@timestamp",
							"order": "ascending"
						}
					]
				}
			`,
			expectedSortedResults: [][]byte{
				b(`{"service":"testNamespace","@timestamp":"2019-01-22T13:35:42-08:00","st":false,"sid":3,"tt":"inactive","tz":-6,"v":15}`),
				b(`{"service":"testNamespace","@timestamp":"2019-01-22T13:36:42-08:00","st":false,"sid":3,"tt":"inactive","tz":-6,"v":15}`),
				b(`{"service":"testNamespace","@timestamp":"2019-01-22T13:37:42-08:00","st":false,"sid":3,"tt":"inactive","tz":-6,"v":15}`),
				b(`{"service":"testNamespace","@timestamp":"2019-01-22T13:38:42-08:00","st":false,"sid":3,"tt":"inactive","tz":-6,"v":15}`),
				b(`{"service":"testNamespace","@timestamp":"2019-01-22T13:39:42-08:00","st":false,"sid":3,"tt":"inactive","tz":-6,"v":15}`),
				b(`{"service":"testNamespace","@timestamp":"2019-01-22T13:40:42-08:00","st":false,"sid":4,"tt":"inactive","tz":-6,"v":15}`),
				b(`{"service":"testNamespace","@timestamp":"2019-01-22T13:41:42-08:00","st":false,"sid":4,"tt":"inactive","tz":-6,"v":15}`),
				b(`{"service":"testNamespace","@timestamp":"2019-01-22T13:42:42-08:00","st":false,"sid":4,"tt":"inactive","tz":-6,"v":15}`),
				b(`{"service":"testNamespace","@timestamp":"2019-01-22T13:43:42-08:00","st":false,"sid":4,"tt":"inactive","tz":-6,"v":15}`),
				b(`{"service":"testNamespace","@timestamp":"2019-01-22T13:44:42-08:00","st":false,"sid":4,"tt":"inactive","tz":-6,"v":15}`),
			},
		},
	}

	// Write data.
	client := ts.newHTTPClient()
	require.NoError(t, client.write([]byte(strings.TrimSpace(testData))))

	// Test queries.
	for _, test := range tests {
		resp, err := client.queryRaw([]byte(test.queryJSON))
		require.NoError(t, err)
		require.Equal(t, test.expectedSortedResults, resp.Raw)
	}
}
