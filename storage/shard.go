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
	sealed []immutableDatabaseSegment
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
	s.Lock()
	activeSegment := s.active
	s.active = newDatabaseSegment(s.opts)
	activeSegment.Seal()
	s.sealed = append(s.sealed, activeSegment)
	numToFlush := s.opts.MaxNumCachedSegmentsPerShard() - len(s.sealed)
	if numToFlush <= 0 {
		// Nothing to do.
		s.Unlock()
		return nil
	}
	segmentsToFlush := make([]immutableDatabaseSegment, numToFlush)
	copy(segmentsToFlush, s.sealed[:numToFlush])
	copy(s.sealed, s.sealed[numToFlush:])
	s.Unlock()

	var multiErr xerrors.MultiError
	for _, sm := range segmentsToFlush {
		if err := s.flushOne(ps, sm); err != nil {
			multiErr = multiErr.Add(err)
		}
	}
	return multiErr.FinalError()
}

func (s *dbShard) flushOne(
	ps persist.Persister,
	sm immutableDatabaseSegment,
) error {
	if sm.NumDocuments() == 0 {
		return nil
	}
	var multiErr xerrors.MultiError
	prepareOpts := persist.PrepareOptions{
		Namespace:    s.namespace,
		Shard:        s.ID(),
		SegmentID:    sm.ID(),
		MinTimeNanos: sm.MinTimeNanos(),
		MaxTimeNanos: sm.MaxTimeNanos(),
		NumDocuments: sm.NumDocuments(),
	}
	prepared, err := ps.Prepare(prepareOpts)
	if err != nil {
		return err
	}

	if err := sm.Flush(prepared.Persist); err != nil {
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
