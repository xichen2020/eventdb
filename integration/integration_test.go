package integration

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOrderBy(t *testing.T) {
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
	client, closer := setup(t, "config/config_01.yaml")
	_ = closer
	// defer closer() // TODO(wjang): closer() is panic-ing in storage/shard.go

	for _, test := range tests {
		resp, err := client.query([]byte(test.queryJSON))
		assert.NoError(t, err)
		// TODO(wjang): allow actually comparing results
		assert.Len(t, resp, test.expectedResults)
	}
}
