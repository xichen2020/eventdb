// +build integration

package integration

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRawQueryOrderBy(t *testing.T) {
	tests := []struct {
		queryJSON       string
		expectedResults int
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
			expectedResults: 20,
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
			expectedResults: 10,
		},
	}
	ts := newTestServerSetup(t, testConfig1)
	ts.startServer()
	defer ts.close(t)
	client := ts.newClient()
	require.NoError(t, ts.waitUntil(10*time.Second, client.serverIsHealthy))
	require.NoError(t, client.write([]byte(strings.TrimSpace(testData1))))

	for _, test := range tests {
		resp, err := client.queryRaw([]byte(test.queryJSON))
		assert.NoError(t, err)
		// TODO(wjang): Allow actually comparing results.
		assert.Len(t, resp.Raw, test.expectedResults)
	}
}
