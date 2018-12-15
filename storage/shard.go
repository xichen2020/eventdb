package storage

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"

	"github.com/xichen2020/eventdb/event"
	"github.com/xichen2020/eventdb/persist"
	"github.com/xichen2020/eventdb/query"

	"github.com/MauriceGit/skiplist"
	"github.com/m3db/m3x/context"
	xerrors "github.com/m3db/m3x/errors"
	xlog "github.com/m3db/m3x/log"
)

const (
	defaultBatchPercent    = 0.1
	defaultMaxFlushRetries = 3
)

type databaseShard interface {
	// ID returns the shard ID.
	ID() uint32

	// Write writes an event within the shard.
	Write(ev event.Event) error

	// Query performs a query against the events in the shard.
	Query(ctx context.Context, q query.ParsedQuery) (query.ResultSet, error)

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
	logger    xlog.Logger

	closed bool
	active mutableDatabaseSegment

	// TODO(xichen): Templatize the skiplist.

	// Array of sealed segments in insertion order. Newly sealed segments are always
	// appended at the end so that the flushing thread can scan through the list in small
	// batches easily.
	unflushed []*sealedSegment

	// List of sealed segments sorted by maximum segment timestamp in ascending order.
	// This list is used for locating eligible segments during query time. Ideally it
	// can also auto rebalance after a sequence of insertion and deletions due to new
	// sealed segments becoming available and old segments expiring.
	sealedByMaxTimeAsc skiplist.SkipList
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
		logger:             opts.InstrumentOptions().Logger(),
		active:             newDatabaseSegment(opts),
		sealedByMaxTimeAsc: skiplist.New(),
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

// NB(xichen): Can optimize by accessing sealed segments first if the query requires
// sorting by @timestamp in ascending order, and possibly terminate early without
// accessing the active segment.
func (s *dbShard) Query(
	ctx context.Context,
	q query.ParsedQuery,
) (query.ResultSet, error) {
	return query.ResultSet{}, fmt.Errorf("not implemented")
}

// TODO(xichen): Need to expire and discard segments that have expired, which requires
// the skiplist to implement a method to delete all elements before and including
// a given element. Ignore for now due to lack of proper API.
// TODO(xichen): Propagate ticking stats back up.
func (s *dbShard) Tick(ctx context.Context) error {
	return s.tryUnloadSegments(ctx)
}

func (s *dbShard) Flush(ps persist.Persister) error {
	s.RLock()
	numToFlush := len(s.unflushed)
	if numToFlush == 0 {
		// Nothing to do.
		s.RUnlock()
		return nil
	}
	segmentsToFlush := make([]*sealedSegment, numToFlush)
	copy(segmentsToFlush, s.unflushed)
	s.RUnlock()

	var (
		multiErr    xerrors.MultiError
		doneIndices = make([]int, 0, numToFlush)
	)
	for i, sm := range segmentsToFlush {
		// The flush status is only accessed in a single-thread context so no need to lock here.
		sm.flushStatus.state = flushing

		// We only flush segments that have never been flushed successfully before,
		// so by definition the segment should always be in memory.
		if err := s.flushSegment(ps, sm.segment); err != nil {
			multiErr = multiErr.Add(err)
			sm.flushStatus.state = flushFailed
		} else {
			sm.flushStatus.state = flushSuccess
		}
		sm.flushStatus.attempts++

		if sm.flushStatus.isDone() {
			doneIndices = append(doneIndices, i)
		}
	}

	// Remove segments that have either been flushed to disk successfully, or those that have
	// failed sufficient number of times.
	s.removeFlushDoneSegments(doneIndices, numToFlush)

	return multiErr.FinalError()
}

func (s *dbShard) Close() error {
	s.Lock()
	defer s.Unlock()

	if s.closed {
		return errShardAlreadyClosed
	}
	s.closed = true

	var (
		active       = s.active
		byMaxTimeAsc = s.sealedByMaxTimeAsc
	)
	s.active = nil
	s.unflushed = nil
	s.sealedByMaxTimeAsc = skiplist.SkipList{}

	// Closing the sealed segments asynchronously in case there are existing readers
	// reading from the segments blocking the close.
	go func() {
		active.Close()

		if byMaxTimeAsc.GetNodeCount() == 0 {
			return
		}

		firstTime := true
		firstElem := byMaxTimeAsc.GetSmallestNode()
		for elem := firstElem; firstTime || elem != firstElem; elem = byMaxTimeAsc.Next(elem) {
			firstTime = false
			segment := elem.GetValue().(*sealedSegment)
			segment.Close()
		}
	}()

	return nil
}

func (s *dbShard) tryUnloadSegments(ctx context.Context) error {
	return s.forEachSegment(func(segment *sealedSegment) error {
		if segment.ShouldUnload() {
			segment.Unload(ctx)
		}
		return nil
	})
}

// NB(xichen): This assumes that a skiplist element may not become invalid (e.g., deleted)
// after new elements are inserted into the skiplist while iterating over a batch of elements.
// A brief look at the skiplist implementation seems to suggest this is the case but may need
// a closer look to validate fully.
func (s *dbShard) forEachSegment(segmentFn segmentFn) error {
	// Determine batch size.
	s.RLock()
	elemsLen := s.sealedByMaxTimeAsc.GetNodeCount()
	if elemsLen == 0 {
		// If the list is empty, nothing to do.
		s.RUnlock()
		return nil
	}
	batchSize := int(math.Max(1.0, math.Ceil(defaultBatchPercent*float64(elemsLen))))
	currElem := s.sealedByMaxTimeAsc.GetSmallestNode()
	firstTime := true
	firstElem := currElem
	s.RUnlock()

	var multiErr xerrors.MultiError
	currSegments := make([]*sealedSegment, 0, batchSize)
	for currElem != nil {
		s.RLock()
		// The skip list is a circular, doubly linked list.
		for numChecked := 0; numChecked < batchSize && (firstTime || currElem != firstElem); numChecked++ {
			nextElem := s.sealedByMaxTimeAsc.Next(currElem)
			ss := currElem.GetValue().(*sealedSegment)
			currSegments = append(currSegments, ss)
			currElem = nextElem
			firstTime = false
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
	sealedSegment := newSealedSegment(s.namespace, s.shard, activeSegment, s.opts)
	s.unflushed = append(s.unflushed, sealedSegment)
	s.sealedByMaxTimeAsc.Insert(sealedSegment)
	s.Unlock()
}

func (s *dbShard) flushSegment(
	ps persist.Persister,
	sm immutableDatabaseSegment,
) error {
	numDocs := sm.NumDocuments()
	if numDocs == 0 {
		return nil
	}
	var multiErr xerrors.MultiError
	prepareOpts := persist.PrepareOptions{
		Namespace:    s.namespace,
		Shard:        s.ID(),
		SegmentID:    sm.ID(),
		MinTimeNanos: sm.MinTimeNanos(),
		MaxTimeNanos: sm.MaxTimeNanos(),
		NumDocuments: numDocs,
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
	s.Unlock()
}

type segmentFlushState int

// nolint: deadcode,varcheck,megacheck
const (
	notFlushed segmentFlushState = iota
	flushing
	flushSuccess
	flushFailed
)

type segmentFlushStatus struct {
	state    segmentFlushState
	attempts int
}

func (st segmentFlushStatus) isDone() bool {
	return st.state == flushSuccess ||
		(st.state == flushFailed && st.attempts >= defaultMaxFlushRetries)
}

type segmentMetadata struct {
	id           string
	minTimeNanos int64
	maxTimeNanos int64
}

type segmentRetriever struct {
	filePath    string
	namespace   []byte
	shard       uint32
	segmentMeta segmentMetadata
}

func newSegmentRetriever(
	filePath string,
	namespace []byte,
	shard uint32,
	segmentMeta segmentMetadata,
) *segmentRetriever {
	return &segmentRetriever{
		filePath:    filePath,
		namespace:   namespace,
		shard:       shard,
		segmentMeta: segmentMeta,
	}
}

// TODO(xichen): Implement this.
func (r *segmentRetriever) Close() {
	panic("not implemented")
}

type segmentFn func(segment *sealedSegment) error

type sealedSegment struct {
	sync.RWMutex

	namespace []byte
	shard     uint32
	metadata  segmentMetadata
	opts      *Options

	// The flush status field is always accessed within a single-thread context
	// and as such does not require lock protection.
	flushStatus segmentFlushStatus

	closed          bool
	lastReadAtNanos int64
	unloaded        int32
	segment         immutableDatabaseSegment
	retriever       *segmentRetriever
}

func newSealedSegment(
	namespace []byte,
	shard uint32,
	segment immutableDatabaseSegment,
	opts *Options,
) *sealedSegment {
	var (
		id           = segment.ID()
		minTimeNanos = segment.MinTimeNanos()
		maxTimeNanos = segment.MaxTimeNanos()
	)
	return &sealedSegment{
		namespace: namespace,
		shard:     shard,
		metadata: segmentMetadata{
			id:           id,
			minTimeNanos: minTimeNanos,
			maxTimeNanos: maxTimeNanos,
		},
		opts:    opts,
		segment: segment,
	}
}

func (s *sealedSegment) ExtractKey() float64 { return float64(s.metadata.maxTimeNanos) }
func (s *sealedSegment) String() string      { return fmt.Sprintf("%v", s.metadata) }

func (s *sealedSegment) ShouldUnload() bool {
	// If the segment has already been unloaded, don't unload again.
	if atomic.LoadInt32(&s.unloaded) == 1 {
		return false
	}

	// Do not unload segments that is not yet done flushing.
	if !s.flushStatus.isDone() {
		return false
	}

	// If the segment was read recently, it's likely the segment is going to be read
	// again in the future, and as a result we keep it loaded in memory.
	nowNanos := s.opts.ClockOptions().NowFn()().UnixNano()
	unreadDuration := s.opts.SegmentUnloadAfterUnreadFor().Nanoseconds()
	unloadAfter := atomic.LoadInt64(&s.lastReadAtNanos) + unreadDuration
	return nowNanos >= unloadAfter
}

// TODO(xichen): Implement this.
func (s *sealedSegment) Load() error {
	return fmt.Errorf("not implemented")
}

func (s *sealedSegment) Unload(ctx context.Context) {
	// Acquiring the write lock first so the state can't change beneath us.
	s.Lock()
	defer s.Unlock()

	if !s.ShouldUnload() {
		return
	}
	atomic.StoreInt32(&s.unloaded, 1)
	segment := s.segment
	s.segment = nil
	ctx.RegisterCloser(segment)
	s.retriever = newSegmentRetriever(s.opts.FilePathPrefix(), s.namespace, s.shard, s.metadata)
}

func (s *sealedSegment) Close() {
	s.Lock()
	defer s.Unlock()

	if s.closed {
		return
	}
	s.closed = true
	if s.segment != nil {
		s.segment.Close()
	}
	if s.retriever != nil {
		s.retriever.Close()
	}
}
