package integration

import (
	"testing"

	"github.com/xichen2020/eventdb/query"

	xtime "github.com/m3db/m3x/time"
)

func TestS(t *testing.T) {
	var (
		startTime = int64(1548115200)
		endTime   = int64(1548201600)
		unit      = query.TimeUnit(xtime.Second)
	)

	tests := []struct {
		query query.RawQuery
	}{
		{
			query: query.RawQuery{
				Namespace: "testNamespace",
				StartTime: &startTime,
				EndTime:   &endTime,
				TimeUnit:  &unit,
				GroupBy:   []string{"service"},
			},
		},
	}
	_, client, _ := setup(t, "config/config_01.yaml")
	// defer closer()

	for _, test := range tests {
		err := client.query(test.query)
		if err != nil {
			print(err.Error())
		}
	}
}
