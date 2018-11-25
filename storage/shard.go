package storage

import (
	"errors"
	"sync"

	"github.com/xichen2020/eventdb/event"
)

type databaseShard interface {
	// Write writes an event within the shard.
	Write(ev event.Event) error

	// Close closes the shard.
	Close() error
}

var (
	errShardAlreadyClosed = errors.New("shard already closed")
)

type dbShard struct {
	sync.RWMutex

	shard uint32
	opts  *Options

	closed bool
	active mutableDatabaseSegment
	// nolint: megacheck,structcheck
	sealed []immutableDatabaseSegment
}

func newDatabaseShard(
	shard uint32,
	opts *Options,
) *dbShard {
	return &dbShard{
		shard:  shard,
		opts:   opts,
		active: newMutableDatabaseSegment(opts),
	}
}

func (s *dbShard) Write(ev event.Event) error {
	s.RLock()
	segment := s.active
	s.RUnlock()

	return segment.Write(ev)
}

func (s *dbShard) Close() error {
	s.Lock()
	defer s.Unlock()

	if s.closed {
		return errShardAlreadyClosed
	}
	s.closed = true
	return nil
}
