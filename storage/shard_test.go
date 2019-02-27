package storage

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xichen2020/eventdb/persist"
	"github.com/xichen2020/eventdb/query"

	"github.com/golang/mock/gomock"
)

func TestShardRemoveFlushDoneSegmentsAllFlushed(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := NewOptions()
	segmentOpts := sealedFlushingSegmentOptions{
		nowFn:             opts.ClockOptions().NowFn(),
		instrumentOptions: opts.InstrumentOptions(),
	}
	var segments []sealedFlushingSegment
	for i := 0; i < 5; i++ {
		segmentBase := NewMockimmutableSegmentBase(ctrl)
		segmentBase.EXPECT().ID().Return(fmt.Sprintf("segment%d", i)).AnyTimes()
		segmentBase.EXPECT().MinTimeNanos().Return(int64(1234)).AnyTimes()
		segmentBase.EXPECT().MaxTimeNanos().Return(int64(5678)).AnyTimes()
		immutableSegment := &mockImmutableSegment{immutableSegmentBase: segmentBase}
		ss := newSealedFlushingSegment(immutableSegment, segmentOpts)
		segments = append(segments, ss)
	}
	s := newDatabaseShard(nil, 0, opts, nil)
	s.unflushed = segments

	successIndices := []int{0, 1, 2}
	s.removeFlushDoneSegments(successIndices, 3)
	require.Equal(t, 2, len(s.unflushed))
	require.Equal(t, "segment3", s.unflushed[0].ID())
	require.Equal(t, "segment4", s.unflushed[1].ID())
}

func TestShardRemoveFlushDoneSegmentsPartialFlushed(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := NewOptions()
	segmentOpts := sealedFlushingSegmentOptions{
		nowFn:             opts.ClockOptions().NowFn(),
		instrumentOptions: opts.InstrumentOptions(),
	}
	var segments []sealedFlushingSegment
	for i := 0; i < 5; i++ {
		segmentBase := NewMockimmutableSegmentBase(ctrl)
		segmentBase.EXPECT().ID().Return(fmt.Sprintf("segment%d", i)).AnyTimes()
		segmentBase.EXPECT().MinTimeNanos().Return(int64(1234)).AnyTimes()
		segmentBase.EXPECT().MaxTimeNanos().Return(int64(5678)).AnyTimes()
		immutableSegment := &mockImmutableSegment{immutableSegmentBase: segmentBase}
		ss := newSealedFlushingSegment(immutableSegment, segmentOpts)
		segments = append(segments, ss)
	}
	s := newDatabaseShard(nil, 0, opts, nil)
	s.unflushed = segments

	successIndices := []int{0, 2}
	s.removeFlushDoneSegments(successIndices, 4)
	require.Equal(t, 3, len(s.unflushed))
	require.Equal(t, "segment1", s.unflushed[0].ID())
	require.Equal(t, "segment3", s.unflushed[1].ID())
	require.Equal(t, "segment4", s.unflushed[2].ID())
}

func TestShardClose(t *testing.T) {
	shard := newDatabaseShard([]byte("testNamespace"), 0, NewOptions(), NewNamespaceOptions())

	// Closing once should result in no error.
	require.NoError(t, shard.Close())
	require.True(t, shard.closed)
	require.Nil(t, shard.active)
	require.Nil(t, shard.sealedByMaxTimeAsc)

	// Closing twice should result in an error.
	require.Equal(t, errShardAlreadyClosed, shard.Close())
}

// NB(xichen): Ugly, hand-written mocks for immutable segments because
// mockgen doesn't support generating mocks for interfaces that
// embed unexported interfaces.
type mockImmutableSegment struct {
	immutableSegmentBase

	queryRawFn func(
		ctx context.Context,
		q query.ParsedRawQuery,
		res *query.RawResults,
	) error

	queryGroupedFn func(
		ctx context.Context,
		q query.ParsedGroupedQuery,
		res *query.GroupedResults,
	) error

	queryTimeBucketFn func(
		ctx context.Context,
		q query.ParsedTimeBucketQuery,
		res *query.TimeBucketResults,
	) error

	loadedStatusFn func() segmentLoadedStatus
	unloadFn       func() error
	flushFn        func(persistFns persist.Fns) error
}

func (m *mockImmutableSegment) QueryRaw(
	ctx context.Context,
	q query.ParsedRawQuery,
	res *query.RawResults,
) error {
	return m.queryRawFn(ctx, q, res)
}

func (m *mockImmutableSegment) QueryGrouped(
	ctx context.Context,
	q query.ParsedGroupedQuery,
	res *query.GroupedResults,
) error {
	return m.queryGroupedFn(ctx, q, res)
}

func (m *mockImmutableSegment) QueryTimeBucket(
	ctx context.Context,
	q query.ParsedTimeBucketQuery,
	res *query.TimeBucketResults,
) error {
	return m.queryTimeBucketFn(ctx, q, res)
}

func (m *mockImmutableSegment) LoadedStatus() segmentLoadedStatus {
	return m.loadedStatusFn()
}

func (m *mockImmutableSegment) Unload() error {
	return m.unloadFn()
}

func (m *mockImmutableSegment) Flush(persistFns persist.Fns) error {
	return m.flushFn(persistFns)
}
