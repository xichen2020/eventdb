package integration

import (
	"testing"
)

func TestS(t *testing.T) {
	tests := []struct {
		name      string
		queryJSON string
	}{
		{
			name: "",
			queryJSON: `
				{
					"namespace": "testNamespace",
					"start_time": 1548115200,
					"end_time": 1548201600,
					"time_unit": 1000000,
					"order_by": [
						{
							"field": "@timestamp",
							"order": "ascending"
						}
					]
				}
			`,
		},
	}
	client, closer := setup(t, "config/config_01.yaml")
	_ = closer
	// defer closer() // TODO(wjang): closer() is panic-ing

	for _, test := range tests {
		err := client.query([]byte(test.queryJSON))
		if err != nil {
			print(err.Error())
		}
	}
}
