package storage

import (
	"errors"
	"fmt"
	"sync"

	"github.com/xichen2020/eventdb/event"
	"github.com/xichen2020/eventdb/x/hash"
)

const (
	defaultInitialNumDocs   = 4096
	defaultInitialNumFields = 64
)

var (
	// errSegmentAlreadySealed is raised when trying to mutabe a sealed segment.
	errSegmentAlreadySealed = errors.New("segment is already sealed")
)

// mutableDatabaseSegment is an immutable database segment.
type immutableDatabaseSegment interface {
}

// mutableDatabaseSegment is a mutable database segment.
type mutableDatabaseSegment interface {
	immutableDatabaseSegment

	// Write writes an event to the mutable segment.
	Write(ev event.Event) error

	// Seal seals the mutable segment and make it immutable.
	Seal() immutableDatabaseSegment

	// Close closes the mutable segment.
	Close() error
}

type dbSegment struct {
	sync.RWMutex

	opts *Options

	// NB: We refer to an event containing a collection of fields a document
	// in conventional information retrieval terminology.
	sealed       bool
	numDocs      int32
	minTimeNanos int64
	maxTimeNanos int64
	fields       map[hash.Hash]*fieldWriter
	rawDocs      [][]byte
}

func newMutableDatabaseSegment(
	opts *Options,
) *dbSegment {
	return &dbSegment{
		opts:    opts,
		numDocs: 0,
		rawDocs: make([][]byte, 0, defaultInitialNumDocs),
		fields:  make(map[hash.Hash]*fieldWriter, defaultInitialNumFields),
	}
}

func (s *dbSegment) Write(ev event.Event) error {
	s.Lock()
	if s.sealed {
		s.Unlock()
		return errSegmentAlreadySealed
	}
	if s.minTimeNanos > ev.TimeNanos {
		s.minTimeNanos = ev.TimeNanos
	}
	if s.maxTimeNanos < ev.TimeNanos {
		s.maxTimeNanos = ev.TimeNanos
	}
	docID := s.numDocs
	s.numDocs++
	s.rawDocs = append(s.rawDocs, ev.RawData)

	for ev.FieldIter.Next() {
		f := ev.FieldIter.Current()
		pathHash := hash.StringArrayHash(f.Path, s.opts.NestedFieldSeparator())
		w, exists := s.fields[pathHash]
		if !exists {
			w = newFieldWriter(f.Path)
			s.fields[pathHash] = w
		}
		w.addValue(docID, f.Value)
	}
	ev.FieldIter.Close()
	s.Unlock()
	return nil
}

func (s *dbSegment) Seal() immutableDatabaseSegment {
	panic(fmt.Errorf("not implemented"))
}

func (s *dbSegment) Close() error {
	return fmt.Errorf("not implemented")
}
