package time

import (
	"encoding/json"
	"time"
)

// Duration is a time duration that can be unmarshalled from JSON.
type Duration time.Duration

// String returns the duration string.
func (d Duration) String() string { return time.Duration(d).String() }

// MarshalJSON marshals the duration as a string.
func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

// UnmarshalJSON unmarshals the raw bytes into a duration.
func (d *Duration) UnmarshalJSON(b []byte) error {
	var v string
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	dur, err := time.ParseDuration(v)
	if err != nil {
		return err
	}
	*d = Duration(dur)
	return nil
}
