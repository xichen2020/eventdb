package storage

import (
	"fmt"
	"sync"

	"github.com/xichen2020/eventdb/event"
)

// mutableDatabaseSegment is an immutable database segment.
type immutableDatabaseSegment interface {
}

// mutableDatabaseSegment is a mutable database segment.
type mutableDatabaseSegment interface {
	immutableDatabaseSegment

	// Write writes an event to the mutable segment.
	Write(ev event.Event) error

	// Seal seals the mutable segment and make it immutable.
	Seal() immutableDatabaseSegment

	// Close closes the mutable segment.
	Close() error
}

type dbSegment struct {
	sync.RWMutex

	opts *Options
}

func newMutableDatabaseSegment(
	opts *Options,
) *dbSegment {
	return &dbSegment{
		opts: opts,
	}
}

func (s *dbSegment) Write(ev event.Event) error {
	return fmt.Errorf("not implemented")
}

func (s *dbSegment) Seal() immutableDatabaseSegment {
	panic(fmt.Errorf("not implemented"))
}

func (s *dbSegment) Close() error {
	return fmt.Errorf("not implemented")
}
