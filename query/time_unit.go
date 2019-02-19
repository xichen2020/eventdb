package query

import (
	"encoding/json"
	"time"

	xtime "github.com/xichen2020/eventdb/x/time"

	m3xtime "github.com/m3db/m3x/time"
)

// TimeUnit is a time unit.
type TimeUnit m3xtime.Unit

// UnmarshalJSON unmarshals a JSON object as a time unit.
func (tu *TimeUnit) UnmarshalJSON(data []byte) error {
	var d xtime.Duration
	if err := json.Unmarshal(data, &d); err != nil {
		return err
	}
	u, err := m3xtime.UnitFromDuration(time.Duration(d))
	if err != nil {
		return err
	}
	*tu = TimeUnit(u)
	return nil
}

// MustValue returns the duration value of the time unit,
// or panics if an error is encountered.
func (tu TimeUnit) MustValue() time.Duration {
	xu := m3xtime.Unit(tu)
	v, err := xu.Value()
	if err != nil {
		panic(err)
	}
	return v
}
