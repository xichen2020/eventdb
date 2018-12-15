package storage

import (
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestRemoveFlushDoneSegmentsAllFlushed(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := NewOptions()
	var segments []*sealedSegment
	for i := 0; i < 5; i++ {
		segment := NewMockimmutableDatabaseSegment(ctrl)
		segment.EXPECT().ID().Return(fmt.Sprintf("segment%d", i)).AnyTimes()
		segment.EXPECT().MinTimeNanos().Return(int64(1234)).AnyTimes()
		segment.EXPECT().MaxTimeNanos().Return(int64(5678)).AnyTimes()
		ss := newSealedSegment([]byte("testNamespace"), 0, segment, opts)
		segments = append(segments, ss)
	}
	s := newDatabaseShard(nil, 0, opts, nil)
	s.unflushed = segments

	successIndices := []int{0, 1, 2}
	s.removeFlushDoneSegments(successIndices, 3)
	require.Equal(t, 2, len(s.unflushed))
	require.Equal(t, "segment3", s.unflushed[0].segment.ID())
	require.Equal(t, "segment4", s.unflushed[1].segment.ID())
}

func TestRemoveFlushDoneSegmentsPartialFlushed(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := NewOptions()
	var segments []*sealedSegment
	for i := 0; i < 5; i++ {
		segment := NewMockimmutableDatabaseSegment(ctrl)
		segment.EXPECT().ID().Return(fmt.Sprintf("segment%d", i)).AnyTimes()
		segment.EXPECT().MinTimeNanos().Return(int64(1234)).AnyTimes()
		segment.EXPECT().MaxTimeNanos().Return(int64(5678)).AnyTimes()
		ss := newSealedSegment([]byte("testNamespace"), 0, segment, opts)
		segments = append(segments, ss)
	}
	s := newDatabaseShard(nil, 0, opts, nil)
	s.unflushed = segments

	successIndices := []int{0, 2}
	s.removeFlushDoneSegments(successIndices, 4)
	require.Equal(t, 3, len(s.unflushed))
	require.Equal(t, "segment1", s.unflushed[0].segment.ID())
	require.Equal(t, "segment3", s.unflushed[1].segment.ID())
	require.Equal(t, "segment4", s.unflushed[2].segment.ID())
}
