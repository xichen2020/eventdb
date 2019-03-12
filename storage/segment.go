package storage

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	indexfield "github.com/xichen2020/eventdb/index/field"
	"github.com/xichen2020/eventdb/index/segment"
	"github.com/xichen2020/eventdb/persist"
	"github.com/xichen2020/eventdb/query"
	"github.com/xichen2020/eventdb/query/executor"

	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/instrument"
)

const (
	defaultMaxFlushRetries = 3
)

var (
	errDatabaseSegmentAlreadyClosed   = errors.New("database segment is already closed")
	errFlushingNotInMemoryOnlySegment = errors.New("flushing a segment that is not in memory only")
)

type segmentOptions struct {
	nowFn                clock.NowFn
	unloadAfterUnreadFor time.Duration
	instrumentOptions    instrument.Options
	fieldRetriever       persist.FieldRetriever
	executor             executor.Executor
}

type segmentLoadedStatus int32

const (
	// The full segment is loaded in memory.
	segmentFullyLoaded segmentLoadedStatus = iota

	// The segment is partially loaded in memory.
	segmentPartiallyLoaded

	// The full segment has been unloaded onto disk.
	segmentUnloaded
)

type segmentDataLocation int32

const (
	// The segment data is only available in memory.
	inMemoryOnly segmentDataLocation = iota

	// The segment data is available on disk and may or may not be in memory.
	availableOnDisk
)

type segmentFlushState int

// nolint: deadcode,varcheck,megacheck
const (
	notFlushed segmentFlushState = iota
	flushing
	flushSuccess
	flushFailed
)

// Concurrent access to the raw segment is protected by the segment lock.
type dbSegment struct {
	sync.RWMutex

	meta                 segment.Metadata
	raw                  segment.MutableSegment
	retriever            executor.FieldRetriever
	executor             executor.Executor
	nowFn                clock.NowFn
	unloadAfterUnreadFor time.Duration

	closed          bool
	lastReadAtNanos int64
	loadedStatus    int32
	dataLocation    int32
	// The flush related fields are always accessed within a single-thread context
	// and as such do not require lock protection.
	flushState    segmentFlushState
	flushAttempts int
}

func newDatabaseSegment(
	namespace []byte,
	rawSegment segment.MutableSegment,
	opts *segmentOptions,
) *dbSegment {
	s := &dbSegment{
		meta:                 rawSegment.Metadata(),
		raw:                  rawSegment,
		executor:             opts.executor,
		nowFn:                opts.nowFn,
		unloadAfterUnreadFor: opts.unloadAfterUnreadFor,
		lastReadAtNanos:      opts.nowFn().UnixNano(),
	}
	s.retriever = newUpdatingSegmentRetriever(namespace, s, opts.fieldRetriever)
	return s
}

func (s *dbSegment) Metadata() segment.Metadata { return s.meta }

func (s *dbSegment) QueryRaw(
	ctx context.Context,
	q query.ParsedRawQuery,
	res *query.RawResults,
) error {
	s.RLock()
	if s.closed {
		s.RUnlock()
		return errDatabaseSegmentAlreadyClosed
	}
	seg := s.raw
	seg.IncRef()
	s.RUnlock()
	defer seg.Close()

	err := s.executor.ExecuteRaw(ctx, q, seg, s.retriever, res)
	s.updateLastReadNanos()
	return err
}

func (s *dbSegment) QueryGrouped(
	ctx context.Context,
	q query.ParsedGroupedQuery,
	res *query.GroupedResults,
) error {
	s.RLock()
	if s.closed {
		s.RUnlock()
		return errDatabaseSegmentAlreadyClosed
	}
	seg := s.raw
	seg.IncRef()
	s.RUnlock()
	defer seg.Close()

	err := s.executor.ExecuteGrouped(ctx, q, seg, s.retriever, res)
	s.updateLastReadNanos()
	return err
}

func (s *dbSegment) QueryTimeBucket(
	ctx context.Context,
	q query.ParsedTimeBucketQuery,
	res *query.TimeBucketResults,
) error {
	s.RLock()
	if s.closed {
		s.RUnlock()
		return errDatabaseSegmentAlreadyClosed
	}
	seg := s.raw
	seg.IncRef()
	s.RUnlock()
	defer seg.Close()

	err := s.executor.ExecuteTimeBucket(ctx, q, seg, s.retriever, res)
	s.updateLastReadNanos()
	return err
}

func (s *dbSegment) LoadedStatus() segmentLoadedStatus {
	status := atomic.LoadInt32(&s.loadedStatus)
	return segmentLoadedStatus(status)
}

func (s *dbSegment) ShouldUnload() bool {
	s.RLock()
	ok := s.shouldUnloadWithLock()
	s.RUnlock()
	return ok
}

func (s *dbSegment) Unload() error {
	s.Lock()
	defer s.Unlock()

	if !s.shouldUnloadWithLock() {
		return nil
	}
	s.setLoadedStatus(segmentUnloaded)
	s.raw.ClearAll()
	return nil
}

func (s *dbSegment) DataLocation() segmentDataLocation {
	location := atomic.LoadInt32(&s.dataLocation)
	return segmentDataLocation(location)
}

// NB: This is always called from a single thread.
func (s *dbSegment) Flush(persistFns persist.Fns) error {
	s.RLock()
	if s.closed {
		s.RUnlock()
		return errDatabaseSegmentAlreadyClosed
	}

	if !(s.LoadedStatus() == segmentFullyLoaded && s.DataLocation() == inMemoryOnly) {
		// NB: This should never happen.
		s.RUnlock()
		return errFlushingNotInMemoryOnlySegment
	}

	segmentFields := make([]indexfield.DocsField, 0, s.raw.Metadata().NumFields)
	s.raw.ForEach(func(f *segment.Field) {
		if df := f.DocsField(); df != nil {
			segmentFields = append(segmentFields, df)
		}
	})
	s.RUnlock()
	defer func() {
		// Close the docs field shallow copies.
		for i := range segmentFields {
			segmentFields[i].Close()
			segmentFields[i] = nil
		}
	}()

	s.flushState = flushing
	err := persistFns.WriteFields(segmentFields)
	if err == nil {
		s.setDataLocation(availableOnDisk)
		s.flushState = flushSuccess
	} else {
		s.flushState = flushFailed
	}
	s.flushAttempts++

	return err
}

func (s *dbSegment) FlushIsDone() bool {
	return s.flushState == flushSuccess ||
		(s.flushState == flushFailed && s.flushAttempts >= defaultMaxFlushRetries)
}

func (s *dbSegment) Close() {
	s.Lock()
	defer s.Unlock()

	if s.closed {
		return
	}
	s.raw.Close()
	s.raw = nil
	s.retriever = nil
	s.executor = nil
}

func (s *dbSegment) setLoadedStatus(v segmentLoadedStatus) {
	status := int32(v)
	atomic.StoreInt32(&s.loadedStatus, status)
}

// nolint: unparam
func (s *dbSegment) setDataLocation(v segmentDataLocation) {
	status := int32(v)
	atomic.StoreInt32(&s.dataLocation, status)
}

func (s *dbSegment) updateLastReadNanos() {
	atomic.StoreInt64(&s.lastReadAtNanos, s.nowFn().UnixNano())
}

func (s *dbSegment) shouldUnloadWithLock() bool {
	if s.closed {
		return false
	}

	// Do not unload segments that is not yet done flushing.
	if !s.FlushIsDone() {
		return false
	}

	// If there are other ongoing work referencing this segment other than myself,
	// we should keep the segment in memory without unloading it.
	if s.raw.RefCount() > 1 {
		return false
	}

	// If the segment is already unloaded, do nothing.
	if status := s.LoadedStatus(); status == segmentUnloaded {
		return false
	}

	// If the segment was read recently, it's likely the segment is going to be read
	// again in the future, and as a result we keep it loaded in memory.
	nowNanos := s.nowFn().UnixNano()
	unreadDuration := s.unloadAfterUnreadFor.Nanoseconds()
	unloadAfter := atomic.LoadInt64(&s.lastReadAtNanos) + unreadDuration
	return nowNanos >= unloadAfter
}
