package event

import (
	"github.com/xichen2020/eventdb/event/field"
)

// Event is a discrete, timestamped event.
type Event struct {
	ID        []byte
	TimeNanos int64
	FieldIter field.Iterator
	RawData   []byte
}
