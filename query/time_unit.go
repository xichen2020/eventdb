package query

import (
	"encoding/json"
	"time"

	xtime "github.com/m3db/m3x/time"
)

// TimeUnit is a time unit.
type TimeUnit xtime.Unit

// UnmarshalJSON unmarshals a JSON object as a time unit.
func (tu *TimeUnit) UnmarshalJSON(data []byte) error {
	var d time.Duration
	if err := json.Unmarshal(data, &d); err != nil {
		return err
	}
	u, err := xtime.UnitFromDuration(d)
	if err != nil {
		return err
	}
	*tu = TimeUnit(u)
	return nil
}
