// +build integration

package integration

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRawQueryWithFilterOrderBy(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	// Create server.
	ts := newTestServerSetup(t, testConfig1)
	defer ts.close(t)

	// Start the server.
	log := ts.dbOpts.InstrumentOptions().Logger()
	log.Info("testing raw query with filter and order by clauses")
	require.NoError(t, ts.startServer())
	log.Info("server is now up")

	testData := `
{"service":"testNamespace","@timestamp":"2019-01-22T13:25:42-08:00","st":true,"sid":{"foo":1,"bar":2},"tt":"active","tz":-6,"v":1.5}
{"service":"testNamespace","@timestamp":"2019-01-22T13:26:42-08:00","st":true,"sid":{"foo":1,"bar":2},"tt":"active","tz":-6,"v":1.5}
{"service":"testNamespace","@timestamp":"2019-01-22T13:27:42-08:00","st":true,"sid":{"foo":1,"bar":2},"tt":"active","tz":-6,"v":1.5}
{"service":"testNamespace","@timestamp":"2019-01-22T13:28:42-08:00","st":true,"sid":{"foo":1,"bar":2},"tt":"active","tz":-6,"v":1.5}
{"service":"testNamespace","@timestamp":"2019-01-22T13:29:42-08:00","st":true,"sid":{"foo":1,"bar":2},"tt":"active","tz":-6,"v":1.5}
{"service":"testNamespace","@timestamp":"2019-01-22T13:30:42-08:00","st":true,"sid":{"foo":2,"bar":4},"tt":"active","tz":-6,"v":1.5}
{"service":"testNamespace","@timestamp":"2019-01-22T13:31:42-08:00","st":true,"sid":{"foo":2,"bar":4},"tt":"active","tz":-6,"v":1.5}
{"service":"testNamespace","@timestamp":"2019-01-22T13:32:42-08:00","st":true,"sid":{"foo":2,"bar":4},"tt":"active","tz":-6,"v":1.5}
{"service":"testNamespace","@timestamp":"2019-01-22T13:33:42-08:00","st":true,"sid":{"foo":2,"bar":4},"tt":"active","tz":-6,"v":1.5}
{"service":"testNamespace","@timestamp":"2019-01-22T13:34:42-08:00","st":true,"sid":{"foo":2,"bar":4},"tt":"active","tz":-6,"v":1.5}
{"service":"testNamespace","@timestamp":"2019-01-22T13:35:42-08:00","st":false,"sid":{"foo":3,"bar":6},"tt":"inactive","tz":-6,"v":15}
{"service":"testNamespace","@timestamp":"2019-01-22T13:36:42-08:00","st":false,"sid":{"foo":3,"bar":6},"tt":"inactive","tz":-6,"v":15}
{"service":"testNamespace","@timestamp":"2019-01-22T13:37:42-08:00","st":false,"sid":{"foo":3,"bar":6},"tt":"inactive","tz":-6,"v":15}
{"service":"testNamespace","@timestamp":"2019-01-22T13:38:42-08:00","st":false,"sid":{"foo":3,"bar":6},"tt":"inactive","tz":-6,"v":15}
{"service":"testNamespace","@timestamp":"2019-01-22T13:39:42-08:00","st":false,"sid":{"foo":3,"bar":6},"tt":"inactive","tz":-6,"v":15}
{"service":"testNamespace","@timestamp":"2019-01-22T13:40:42-08:00","st":false,"sid":{"foo":4,"bar":8},"tt":"inactive","tz":-6,"v":15}
{"service":"testNamespace","@timestamp":"2019-01-22T13:41:42-08:00","st":false,"sid":{"foo":4,"bar":8},"tt":"inactive","tz":-6,"v":15}
{"service":"testNamespace","@timestamp":"2019-01-22T13:42:42-08:00","st":false,"sid":{"foo":4,"bar":8},"tt":"inactive","tz":-6,"v":15}
{"service":"testNamespace","@timestamp":"2019-01-22T13:43:42-08:00","st":false,"sid":{"foo":4,"bar":8},"tt":"inactive","tz":-6,"v":15}
{"service":"testNamespace","@timestamp":"2019-01-22T13:44:42-08:00","st":false,"sid":{"foo":4,"bar":8},"tt":"inactive","tz":-6,"v":15}`

	tests := []struct {
		queryJSON             string
		expectedSortedResults []string
	}{
		{
			queryJSON: `{
        "namespace":  "testNamespace",
        "start_time": 1548115200,
        "end_time":   1548201600,
        "filters": [
          {
            "filters": [
              {
                "field": "tz",
                "op": "=",
                "value": -6
              },
              {
                "field": "sid.foo",
                "op": "=",
                "value": 3
              }
            ],
            "filter_combinator": "AND"
          }
        ],
        "order_by": [
          {
            "field": "@timestamp",
            "order": "ascending"
          }
        ]
        }`,
			expectedSortedResults: []string{
				`{"service":"testNamespace","@timestamp":"2019-01-22T13:35:42-08:00","st":false,"sid":{"foo":3,"bar":6},"tt":"inactive","tz":-6,"v":15}`,
				`{"service":"testNamespace","@timestamp":"2019-01-22T13:36:42-08:00","st":false,"sid":{"foo":3,"bar":6},"tt":"inactive","tz":-6,"v":15}`,
				`{"service":"testNamespace","@timestamp":"2019-01-22T13:37:42-08:00","st":false,"sid":{"foo":3,"bar":6},"tt":"inactive","tz":-6,"v":15}`,
				`{"service":"testNamespace","@timestamp":"2019-01-22T13:38:42-08:00","st":false,"sid":{"foo":3,"bar":6},"tt":"inactive","tz":-6,"v":15}`,
				`{"service":"testNamespace","@timestamp":"2019-01-22T13:39:42-08:00","st":false,"sid":{"foo":3,"bar":6},"tt":"inactive","tz":-6,"v":15}`,
			},
		},
		{
			queryJSON: `
		    {
		      "namespace":  "testNamespace",
		      "start_time": 1548192900,
		      "end_time":   1548201600,
		      "filters": [
          {
            "filters": [
              {
                "field": "tz",
                "op": "=",
                "value": -6
              },
              {
                "field": "sid.bar",
                "op": "=",
                "value": 8
              }
            ],
            "filter_combinator": "AND"
          }
        ],
        "order_by": [
          {
            "field": "@timestamp",
            "order": "ascending"
          }
        ]
        }`,
			expectedSortedResults: []string{
				`{"service":"testNamespace","@timestamp":"2019-01-22T13:40:42-08:00","st":false,"sid":{"foo":4,"bar":8},"tt":"inactive","tz":-6,"v":15}`,
				`{"service":"testNamespace","@timestamp":"2019-01-22T13:41:42-08:00","st":false,"sid":{"foo":4,"bar":8},"tt":"inactive","tz":-6,"v":15}`,
				`{"service":"testNamespace","@timestamp":"2019-01-22T13:42:42-08:00","st":false,"sid":{"foo":4,"bar":8},"tt":"inactive","tz":-6,"v":15}`,
				`{"service":"testNamespace","@timestamp":"2019-01-22T13:43:42-08:00","st":false,"sid":{"foo":4,"bar":8},"tt":"inactive","tz":-6,"v":15}`,
				`{"service":"testNamespace","@timestamp":"2019-01-22T13:44:42-08:00","st":false,"sid":{"foo":4,"bar":8},"tt":"inactive","tz":-6,"v":15}`,
			},
		},
		{
			queryJSON: `
          {
            "namespace":  "testNamespace",
            "start_time": 1548192900,
            "end_time":   1548201600,
            "filters": [
            {
              "filters": [
                {
                  "field": "tz",
                  "op": "=",
                  "value": -6
                },
                {
                  "field": "v",
                  "op": "=",
                  "value": 18
                }
              ],
              "filter_combinator": "AND"
            }
          ],
          "order_by": [
            {
              "field": "@timestamp",
              "order": "ascending"
            }
          ]
          }`,
			expectedSortedResults: nil,
		},
	}

	// Write data.
	client := ts.newHTTPClient()
	require.NoError(t, client.write([]byte(strings.TrimSpace(testData))))

	// Test queries.
	for _, test := range tests {
		resp, err := client.queryRaw([]byte(test.queryJSON))
		require.NoError(t, err)
		actual := resp.Raw
		require.Equal(t, test.expectedSortedResults, actual)
	}

	// Stop the server.
	require.NoError(t, ts.stopServer())
	log.Info("server is now down")
}
