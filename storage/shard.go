package storage

import (
	"errors"
	"math"
	"sync"

	"github.com/xichen2020/eventdb/document"
	"github.com/xichen2020/eventdb/persist"
	"github.com/xichen2020/eventdb/query"

	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/context"
	xerrors "github.com/m3db/m3x/errors"
	xlog "github.com/m3db/m3x/log"
	skiplist "github.com/notbdu/fast-skiplist"
	"github.com/pborman/uuid"
)

const (
	defaultBatchPercent = 0.1
)

type databaseShard interface {
	// ID returns the shard ID.
	ID() uint32

	// Write writes an document within the shard.
	Write(doc document.Document) error

	// QueryRaw performs a raw query against the documents in the shard.
	QueryRaw(
		ctx context.Context,
		q query.ParsedRawQuery,
	) (query.RawResults, error)

	// Tick ticks through the sealed segments in the shard.
	Tick(ctx context.Context) error

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
	nsOpts    *NamespaceOptions
	nowFn     clock.NowFn
	logger    xlog.Logger

	closed bool
	active mutableSegment

	// TODO(xichen): Templatize the skiplist.

	// Array of sealed segments in insertion order. Newly sealed segments are always
	// appended at the end so that the flushing thread can scan through the list in small
	// batches easily.
	unflushed []sealedFlushingSegment

	// List of sealed segments sorted by maximum segment timestamp in ascending order.
	// This list is used for locating eligible segments during query time. Ideally it
	// can also auto rebalance after a sequence of insertion and deletions due to new
	// sealed segments becoming available and old segments expiring.
	sealedByMaxTimeAsc *skiplist.SkipList
}

func newDatabaseShard(
	namespace []byte,
	shard uint32,
	opts *Options,
	nsOpts *NamespaceOptions,
) *dbShard {
	return &dbShard{
		namespace:          namespace,
		shard:              shard,
		opts:               opts,
		nsOpts:             nsOpts,
		nowFn:              opts.ClockOptions().NowFn(),
		logger:             opts.InstrumentOptions().Logger(),
		active:             newMutableSegment(namespace, shard, uuid.New(), opts),
		sealedByMaxTimeAsc: skiplist.New(),
	}
}

func (s *dbShard) ID() uint32 { return s.shard }

func (s *dbShard) Write(doc document.Document) error {
	s.RLock()
	if s.closed {
		s.RUnlock()
		return errShardAlreadyClosed
	}

	// Increment accessor count of the active segment so it cannot be
	// closed before the write finishes.
	segment := s.active
	segment.IncAccessor()
	s.RUnlock()

	err := segment.Write(doc)
	segment.DecAccessor()
	if err == nil {
		return nil
	}

	switch err {
	case errMutableSegmentAlreadySealed:
		// The active segment has become sealed before a write can be performed
		// against it. As a result we should retry the write.
		return s.Write(doc)
	case errMutableSegmentAlreadyFull:
		// The active segment has become full. As a result we seal the active
		// segment, add it to the list of immutable segments, and retry the write.
		if sealErr := s.sealAndRotate(); sealErr != nil {
			return sealErr
		}
		return s.Write(doc)
	default:
		return err
	}
}

// NB(xichen): Can optimize by accessing sealed segments first if the query requires
// sorting by @timestamp in ascending order, and possibly terminate early without
// accessing the active segment.
// TODO(xichen): Pool sealed segment.
func (s *dbShard) QueryRaw(
	ctx context.Context,
	q query.ParsedRawQuery,
) (query.RawResults, error) {
	s.RLock()
	if s.closed {
		s.RUnlock()
		return query.RawResults{}, errShardAlreadyClosed
	}

	// Increment accessor count of the active segment so it cannot be
	// closed before the read finishes.
	active := s.active
	active.IncAccessor()

	var sealed []sealedFlushingSegment
	geElem := s.sealedByMaxTimeAsc.GetGreaterThanOrEqualTo(float64(q.StartNanosInclusive))
	for elem := geElem; elem != nil; elem = elem.Next() {
		// Increment accessor count of the immutable segment so it cannot be
		// closed before the read finishes.
		ss := elem.Value().(sealedFlushingSegment)
		ss.IncAccessor()
		sealed = append(sealed, ss)
	}
	s.RUnlock()

	// NB(xichen): This allocates but makes the code more readable.
	cleanup := func() {
		active.DecAccessor()
		for _, seg := range sealed {
			seg.DecAccessor()
		}
	}
	defer cleanup()

	// Querying active segment and adds to result set.
	activeRes, err := active.QueryRaw(ctx, q)
	if err == errMutableSegmentAlreadySealed {
		// The active segment has become sealed before a read can be performed
		// against it. As a result we should retry the read.
		return s.QueryRaw(ctx, q)
	}
	if err != nil {
		return query.RawResults{}, err
	}
	res := q.NewRawResults()
	res.AddBatch(activeRes)
	if !res.IsOrdered() && res.LimitReached() {
		return res, nil
	}

	// Querying sealed segments and adds to result set.
	for _, ss := range sealed {
		if err := ss.QueryRaw(ctx, q, &res); err != nil {
			return query.RawResults{}, err
		}
		if !res.IsOrdered() && res.LimitReached() {
			return res, nil
		}
	}

	return res, nil
}

func (s *dbShard) Flush(ps persist.Persister) error {
	s.RLock()
	if s.closed {
		s.RUnlock()
		return errShardAlreadyClosed
	}
	numToFlush := len(s.unflushed)
	if numToFlush == 0 {
		// Nothing to do.
		s.RUnlock()
		return nil
	}
	segmentsToFlush := make([]sealedFlushingSegment, numToFlush)
	copy(segmentsToFlush, s.unflushed)
	for _, seg := range segmentsToFlush {
		seg.IncAccessor()
	}
	s.RUnlock()

	var (
		multiErr    xerrors.MultiError
		doneIndices = make([]int, 0, numToFlush)
	)
	for i, sm := range segmentsToFlush {
		// The flush status is only accessed in a single-thread context so no need to lock here.
		sm.SetFlushState(flushing)
		if err := s.flushSegment(ps, sm); err != nil {
			multiErr = multiErr.Add(err)
			sm.SetFlushState(flushFailed)
		} else {
			sm.SetFlushState(flushSuccess)
		}
		sm.IncNumFlushes()
		if sm.FlushIsDone() {
			doneIndices = append(doneIndices, i)
		}
		sm.DecAccessor()
	}

	// Remove segments that have either been flushed to disk successfully, or those that have
	// failed sufficient number of times.
	s.removeFlushDoneSegments(doneIndices, numToFlush)

	return multiErr.FinalError()
}

// TODO(xichen): Need to expire and discard segments that have expired, which requires
// the skiplist to implement a method to delete all elements before and including
// a given element. Ignore for now due to lack of proper API.
// TODO(xichen): Propagate ticking stats back up.
func (s *dbShard) Tick(ctx context.Context) error {
	return s.tryUnloadSegments(ctx)
}

// NB(xichen): Closing a shard may block until all readers against the existing active
// and sealed segments have finished reading. Typically the shard should be closed asynchronously.
func (s *dbShard) Close() error {
	s.Lock()
	if s.closed {
		s.Unlock()
		return errShardAlreadyClosed
	}
	s.closed = true
	s.unflushed = nil
	active := s.active
	s.active = nil
	s.sealedByMaxTimeAsc = nil
	byMaxTimeAsc := s.sealedByMaxTimeAsc
	s.Unlock()

	active.Close()
	if byMaxTimeAsc.Length == 0 {
		return nil
	}
	for elem := byMaxTimeAsc.Front(); elem != nil; elem = elem.Next() {
		segment := elem.Value().(sealedFlushingSegment)
		segment.Close()
	}
	return nil
}

func (s *dbShard) sealAndRotate() error {
	s.Lock()
	if !s.active.IsFull() {
		// Someone else has got ahead of us and rotated the active segment.
		s.Unlock()
		return nil
	}
	activeSegment := s.active
	s.active = newMutableSegment(s.namespace, s.shard, uuid.New(), s.opts)
	immutableSeg, err := activeSegment.Seal()
	if err != nil {
		s.Unlock()
		return err
	}
	sealedSegmentOpts := sealedFlushingSegmentOptions{
		nowFn:                s.opts.ClockOptions().NowFn(),
		unloadAfterUnreadFor: s.opts.SegmentUnloadAfterUnreadFor(),
	}
	sealedSegment := newSealedFlushingSegment(immutableSeg, sealedSegmentOpts)
	s.unflushed = append(s.unflushed, sealedSegment)
	s.sealedByMaxTimeAsc.Set(float64(activeSegment.MaxTimeNanos()), sealedSegment)
	s.Unlock()
	return nil
}

func (s *dbShard) flushSegment(
	ps persist.Persister,
	sm sealedFlushingSegment,
) error {
	numDocs := sm.NumDocuments()
	if numDocs == 0 {
		return nil
	}
	var multiErr xerrors.MultiError
	prepareOpts := persist.PrepareOptions{
		Namespace:    s.namespace,
		Shard:        s.ID(),
		NumDocuments: numDocs,
		SegmentMeta: persist.SegmentMetadata{
			ID:           sm.ID(),
			MinTimeNanos: sm.MinTimeNanos(),
			MaxTimeNanos: sm.MaxTimeNanos(),
		},
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

func (s *dbShard) removeFlushDoneSegments(doneIndices []int, numToFlush int) {
	s.Lock()
	defer s.Unlock()

	if len(doneIndices) == numToFlush {
		// All success.
		n := copy(s.unflushed, s.unflushed[numToFlush:])
		s.unflushed = s.unflushed[:n]
	} else {
		// One or more segments in the current flush iteration are not done.
		unflushedIdx := 0
		doneIdx := 0
		for i := 0; i < len(s.unflushed); i++ {
			if doneIdx < len(doneIndices) && i == doneIndices[doneIdx] {
				// The current segment is done, so remove from unflushed array.
				doneIdx++
				continue
			}
			// Otherwise, either all segments that have been done have been removed,
			// or the current segment is not yet done. Either way we should keep it.
			s.unflushed[unflushedIdx] = s.unflushed[i]
			unflushedIdx++
		}
		s.unflushed = s.unflushed[:unflushedIdx]
	}
}

// nolint: unparam
func (s *dbShard) tryUnloadSegments(ctx context.Context) error {
	var toUnload []sealedFlushingSegment
	err := s.forEachSegment(func(segment sealedFlushingSegment) error {
		if segment.ShouldUnload() {
			toUnload = append(toUnload, segment)
		}
		return nil
	})
	if err != nil {
		return err
	}

	var multiErr xerrors.MultiError
	s.Lock()
	if s.closed {
		s.Unlock()
		return errShardAlreadyClosed
	}
	for _, segment := range toUnload {
		if !segment.ShouldUnload() {
			continue
		}
		// If we get here, it means the segment should be unloaded, and as such
		// it means there are no current readers reading from the segment (otherwise
		// `ShouldUnload` will return false).
		if err := segment.Unload(); err != nil {
			multiErr = multiErr.Add(err)
		}
	}
	s.Unlock()

	return multiErr.FinalError()
}

type segmentFn func(segment sealedFlushingSegment) error

// NB(xichen): This assumes that a skiplist element may not become invalid (e.g., deleted)
// after new elements are inserted into the skiplist while iterating over a batch of elements.
// A brief look at the skiplist implementation seems to suggest this is the case but may need
// a closer look to validate fully.
func (s *dbShard) forEachSegment(segmentFn segmentFn) error {
	// Determine batch size.
	s.RLock()
	if s.closed {
		s.RUnlock()
		return errShardAlreadyClosed
	}
	elemsLen := s.sealedByMaxTimeAsc.Length
	if elemsLen == 0 {
		// If the list is empty, nothing to do.
		s.RUnlock()
		return nil
	}
	batchSize := int(math.Max(1.0, math.Ceil(defaultBatchPercent*float64(elemsLen))))
	currElem := s.sealedByMaxTimeAsc.Front()
	s.RUnlock()

	var multiErr xerrors.MultiError
	currSegments := make([]sealedFlushingSegment, 0, batchSize)
	for currElem != nil {
		s.RLock()
		if s.closed {
			s.RUnlock()
			return errShardAlreadyClosed
		}
		for numChecked := 0; numChecked < batchSize && currElem != nil; numChecked++ {
			nextElem := currElem.Next()
			ss := currElem.Value().(sealedFlushingSegment)
			currSegments = append(currSegments, ss)
			currElem = nextElem
		}
		s.RUnlock()

		for _, segment := range currSegments {
			if err := segmentFn(segment); err != nil {
				multiErr = multiErr.Add(err)
			}
		}
		for i := range currSegments {
			currSegments[i] = nil
		}
		currSegments = currSegments[:0]
	}
	return multiErr.FinalError()
}
