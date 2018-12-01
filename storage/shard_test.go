package storage

import (
	"fmt"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestRemoveFlushedSegmentsAllFlushed(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var segments []immutableDatabaseSegment
	for i := 0; i < 5; i++ {
		segment := NewMockimmutableDatabaseSegment(ctrl)
		segment.EXPECT().ID().Return(fmt.Sprintf("segment%d", i)).AnyTimes()
		segments = append(segments, segment)
	}
	opts := NewOptions()
	s := newDatabaseShard(nil, 0, opts)
	s.sealed = segments

	successIndices := []int{0, 1, 2}
	s.removeFlushedSegments(successIndices, 3)
	require.Equal(t, 2, len(s.sealed))
	require.Equal(t, "segment3", s.sealed[0].ID())
	require.Equal(t, "segment4", s.sealed[1].ID())
}

func TestRemoveFlushedSegmentsPartialFlushed(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var segments []immutableDatabaseSegment
	for i := 0; i < 5; i++ {
		segment := NewMockimmutableDatabaseSegment(ctrl)
		segment.EXPECT().ID().Return(fmt.Sprintf("segment%d", i)).AnyTimes()
		segments = append(segments, segment)
	}
	opts := NewOptions()
	s := newDatabaseShard(nil, 0, opts)
	s.sealed = segments

	successIndices := []int{0, 2}
	s.removeFlushedSegments(successIndices, 4)
	require.Equal(t, 3, len(s.sealed))
	require.Equal(t, "segment1", s.sealed[0].ID())
	require.Equal(t, "segment3", s.sealed[1].ID())
	require.Equal(t, "segment4", s.sealed[2].ID())
}
