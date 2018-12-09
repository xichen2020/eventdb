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

	namespace                    []byte
	shard                        uint32
	opts                         *Options
	maxNumCachedSegmentsPerShard int

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
		maxNumCachedSegmentsPerShard: opts.MaxNumCachedSegmentsPerShard(),
		active: newDatabaseSegment(opts),
	}
}

func (s *dbShard) ID() uint32 { return s.shard }

func (s *dbShard) Write(ev event.Event) error {
	s.RLock()
	segment := s.active
	s.RUnlock()

	err := segment.Write(ev)
	if err == errSegmentIsFull {
		// Active segment is full, need to seal and rotate the segment.
		s.sealAndRotate()

		// Retry writing the event.
		return s.Write(ev)
	}
	return err
}

// TODO(xichen): retry logic on segment persistence failure.
func (s *dbShard) Flush(ps persist.Persister) error {
	s.RLock()
	numToFlush := len(s.sealed) - s.maxNumCachedSegmentsPerShard
	if numToFlush <= 0 {
		// Nothing to do.
		s.RUnlock()
		return nil
	}
	segmentsToFlush := make([]immutableDatabaseSegment, numToFlush)
	copy(segmentsToFlush, s.sealed[:numToFlush])
	s.RUnlock()

	var (
		multiErr       xerrors.MultiError
		successIndices = make([]int, 0, numToFlush)
	)
	for i, sm := range segmentsToFlush {
		if err := s.flushOne(ps, sm); err != nil {
			multiErr = multiErr.Add(err)
		} else {
			successIndices = append(successIndices, i)
		}
	}

	// Only remove the segments from memory if they have been successfully flushed to disk.
	s.removeFlushedSegments(successIndices, numToFlush)

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

func (s *dbShard) sealAndRotate() {
	s.Lock()
	if !s.active.IsFull() {
		// Someone else has got ahead of us and rotated the active segment.
		s.Unlock()
		return
	}
	activeSegment := s.active
	s.active = newDatabaseSegment(s.opts)
	activeSegment.Seal()
	s.sealed = append(s.sealed, activeSegment)
	s.Unlock()
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

func (s *dbShard) removeFlushedSegments(successIndices []int, numToFlush int) {
	s.Lock()
	if len(successIndices) == numToFlush {
		// All success.
		n := copy(s.sealed, s.sealed[numToFlush:])
		s.sealed = s.sealed[:n]
	} else {
		// One or more segments failed to flush.
		sealedIdx := 0
		successIdx := 0
		for i := 0; i < len(s.sealed); i++ {
			if successIdx < len(successIndices) && i == successIndices[successIdx] {
				// The current segment has been successfully flushed, so remove from sealed array.
				successIdx++
				continue
			}
			// Otherwise, either all segments that have been successfully flushed have been removed,
			// or the current segment has not been successfully flushed. Either way we should keep it.
			s.sealed[sealedIdx] = s.sealed[i]
			sealedIdx++
		}
		s.sealed = s.sealed[:sealedIdx]
	}
	s.Unlock()
}
