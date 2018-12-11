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

// MustValue returns the duration value of the time unit,
// or panics if an error is encountered.
func (tu TimeUnit) MustValue() time.Duration {
	xu := xtime.Unit(tu)
	v, err := xu.Value()
	if err != nil {
		panic(err)
	}
	return v
}
