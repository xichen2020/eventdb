package time

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/xichen2020/eventdb/generated/proto/servicepb"

	m3xtime "github.com/m3db/m3/src/x/time"
)

// Unit is a time unit.
type Unit m3xtime.Unit

// UnmarshalJSON unmarshals a JSON object as a time unit.
func (tu *Unit) UnmarshalJSON(data []byte) error {
	var d Duration
	if err := json.Unmarshal(data, &d); err != nil {
		return err
	}
	u, err := m3xtime.UnitFromDuration(time.Duration(d))
	if err != nil {
		return err
	}
	*tu = Unit(u)
	return nil
}

// ToProto converts a time unit to an optional time unit protobuf message.
func (tu *Unit) ToProto() (servicepb.OptionalTimeUnit, error) {
	if tu == nil {
		pbNoValue := &servicepb.OptionalTimeUnit_NoValue{NoValue: true}
		return servicepb.OptionalTimeUnit{Value: pbNoValue}, nil
	}

	value := &servicepb.OptionalTimeUnit_Data{}
	switch m3xtime.Unit(*tu) {
	case m3xtime.Second:
		value.Data = servicepb.TimeUnit_SECOND
	case m3xtime.Millisecond:
		value.Data = servicepb.TimeUnit_MILLISECOND
	case m3xtime.Microsecond:
		value.Data = servicepb.TimeUnit_MICROSECOND
	case m3xtime.Nanosecond:
		value.Data = servicepb.TimeUnit_NANOSECOND
	case m3xtime.Minute:
		value.Data = servicepb.TimeUnit_MINUTE
	case m3xtime.Hour:
		value.Data = servicepb.TimeUnit_HOUR
	case m3xtime.Day:
		value.Data = servicepb.TimeUnit_DAY
	case m3xtime.Year:
		value.Data = servicepb.TimeUnit_YEAR
	default:
		return servicepb.OptionalTimeUnit{}, fmt.Errorf("invalid time unit %v", *tu)
	}
	return servicepb.OptionalTimeUnit{Value: value}, nil
}

// MustValue returns the duration value of the time unit,
// or panics if an error is encountered.
func (tu Unit) MustValue() time.Duration {
	xu := m3xtime.Unit(tu)
	v, err := xu.Value()
	if err != nil {
		panic(err)
	}
	return v
}
