package storage

import (
	"errors"
	"sync"

	xerrors "github.com/m3db/m3x/errors"
	"github.com/xichen2020/eventdb/event"
	"github.com/xichen2020/eventdb/persist"
)

type databaseShard interface {
	// ID returns the shard ID.
	ID() uint32

	// Write writes an event within the shard.
	Write(ev event.Event) error

	// Flush flushes the segments in the shard.
	Flush(ps persist.Persister) error

	// Close closes the shard.
	Close() error
}

var (
	errShardAlreadyClosed = errors.New("shard already closed")
)

type dbShard struct {
	sync.RWMutex

	namespace []byte
	shard     uint32
	opts      *Options

	closed bool
	active mutableDatabaseSegment
}

func newDatabaseShard(
	namespace []byte,
	shard uint32,
	opts *Options,
) *dbShard {
	return &dbShard{
		namespace: namespace,
		shard:     shard,
		opts:      opts,
		active:    newDatabaseSegment(opts),
	}
}

func (s *dbShard) ID() uint32 { return s.shard }

func (s *dbShard) Write(ev event.Event) error {
	s.RLock()
	segment := s.active
	s.RUnlock()

	return segment.Write(ev)
}

// TODO(xichen): retry logic on segment persistence failure.
func (s *dbShard) Flush(ps persist.Persister) error {
	var multiErr xerrors.MultiError
	s.Lock()
	activeSegment := s.active
	s.active = newDatabaseSegment(s.opts)
	s.Unlock()

	activeSegment.Seal()
	if activeSegment.NumDocuments() == 0 {
		return nil
	}

	prepareOpts := persist.PrepareOptions{
		Namespace:    s.namespace,
		Shard:        s.ID(),
		MinTimeNanos: activeSegment.MinTimeNanos(),
		MaxTimeNanos: activeSegment.MaxTimeNanos(),
		NumDocuments: activeSegment.NumDocuments(),
	}
	prepared, err := ps.Prepare(prepareOpts)
	if err != nil {
		return err
	}

	if err := activeSegment.Flush(prepared.Persist); err != nil {
		multiErr = multiErr.Add(err)
	}
	if err := prepared.Close(); err != nil {
		multiErr = multiErr.Add(err)
	}

	return multiErr.FinalError()
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
