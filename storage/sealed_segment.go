package storage

import (
	"sync/atomic"
	"time"

	"github.com/xichen2020/eventdb/persist"
	"github.com/xichen2020/eventdb/query"

	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/context"
	"github.com/m3db/m3x/instrument"
)

type flushingSegment interface {
	// SetFlushState sets the flush state of the segment.
	SetFlushState(v segmentFlushState)

	// IncNumFlushes increments the number of flushing attempts.
	IncNumFlushes()

	// Flush flushes the segment to persistent storage.
	Flush(persistFns persist.Fns) error

	// FlushIsDone returns true if the segment is done flushing.
	FlushIsDone() bool
}

type sealedSegment interface {
	immutableSegmentBase

	// QueryRaw returns results for a given raw query.
	QueryRaw(
		ctx context.Context,
		q query.ParsedRawQuery,
		res *query.RawResults,
	) error

	// QueryGrouped returns results for a given grouped query.
	QueryGrouped(
		ctx context.Context,
		q query.ParsedGroupedQuery,
		res *query.GroupedResults,
	) error

	// QueryTimeBucket returns results for a given time bucket query.
	QueryTimeBucket(
		ctx context.Context,
		q query.ParsedTimeBucketQuery,
		res *query.TimeBucketResults,
	) error

	// ShouldUnload returns true if the segment is eligible for unloading.
	ShouldUnload() bool

	// Unload unloads all fields from memory.
	Unload() error
}

type sealedFlushingSegment interface {
	flushingSegment
	sealedSegment
}

const (
	defaultMaxFlushRetries = 3
)

type segmentFlushState int

// nolint: deadcode,varcheck,megacheck
const (
	notFlushed segmentFlushState = iota
	flushing
	flushSuccess
	flushFailed
)

type sealedFlushingSegmentOptions struct {
	nowFn                clock.NowFn
	unloadAfterUnreadFor time.Duration
	instrumentOptions    instrument.Options
}

// Concurrent access to the sealed flushing segment is protected by the shard lock.
// In particluar, read lock should be acquired when there is a new reader reading from
// the segment, and write lock should be acquired when performing operations that require
// all readers should finish before the operations can proceed (e.g., Unload or Close).
type sealedFlushingSeg struct {
	immutableSegment

	nowFn                clock.NowFn
	unloadAfterUnreadFor time.Duration

	lastReadAtNanos int64

	// The flush related fields are always accessed within a single-thread context
	// and as such do not require lock protection.
	flushState    segmentFlushState
	flushAttempts int
}

func newSealedFlushingSegment(
	segment immutableSegment,
	opts sealedFlushingSegmentOptions,
) *sealedFlushingSeg {
	return &sealedFlushingSeg{
		immutableSegment:     segment,
		nowFn:                opts.nowFn,
		unloadAfterUnreadFor: opts.unloadAfterUnreadFor,
		lastReadAtNanos:      opts.nowFn().UnixNano(),
	}
}

func (s *sealedFlushingSeg) QueryRaw(
	ctx context.Context,
	q query.ParsedRawQuery,
	res *query.RawResults,
) error {
	err := s.immutableSegment.QueryRaw(ctx, q, res)
	atomic.StoreInt64(&s.lastReadAtNanos, s.nowFn().UnixNano())
	return err
}

func (s *sealedFlushingSeg) QueryGrouped(
	ctx context.Context,
	q query.ParsedGroupedQuery,
	res *query.GroupedResults,
) error {
	err := s.immutableSegment.QueryGrouped(ctx, q, res)
	atomic.StoreInt64(&s.lastReadAtNanos, s.nowFn().UnixNano())
	return err
}

func (s *sealedFlushingSeg) QueryTimeBucket(
	ctx context.Context,
	q query.ParsedTimeBucketQuery,
	res *query.TimeBucketResults,
) error {
	err := s.immutableSegment.QueryTimeBucket(ctx, q, res)
	atomic.StoreInt64(&s.lastReadAtNanos, s.nowFn().UnixNano())
	return err
}

func (s *sealedFlushingSeg) ShouldUnload() bool {
	// Do not unload segments that is not yet done flushing.
	if !s.FlushIsDone() {
		return false
	}

	// If there are accessors currently accessing this segment, don't unload.
	if s.NumAccessors() > 0 {
		return false
	}

	// If the segment is already unloaded, do nothing.
	if status := s.immutableSegment.LoadedStatus(); status == segmentUnloaded {
		return false
	}

	// If the segment was read recently, it's likely the segment is going to be read
	// again in the future, and as a result we keep it loaded in memory.
	nowNanos := s.nowFn().UnixNano()
	unreadDuration := s.unloadAfterUnreadFor.Nanoseconds()
	unloadAfter := s.lastReadAtNanos + unreadDuration
	return nowNanos >= unloadAfter
}

func (s *sealedFlushingSeg) Unload() error {
	if !s.ShouldUnload() {
		return nil
	}
	return s.immutableSegment.Unload()
}

func (s *sealedFlushingSeg) SetFlushState(v segmentFlushState) { s.flushState = v }
func (s *sealedFlushingSeg) IncNumFlushes()                    { s.flushAttempts++ }

func (s *sealedFlushingSeg) Flush(persistFns persist.Fns) error {
	return s.immutableSegment.Flush(persistFns)
}

func (s *sealedFlushingSeg) FlushIsDone() bool {
	return s.flushState == flushSuccess ||
		(s.flushState == flushFailed && s.flushAttempts >= defaultMaxFlushRetries)
}
