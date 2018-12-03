package storage

import (
	"errors"
	"fmt"
	"sync"

	"github.com/xichen2020/eventdb/event"
	"github.com/xichen2020/eventdb/persist"
	"github.com/xichen2020/eventdb/x/hash"
	"github.com/xichen2020/eventdb/x/unsafe"

	"github.com/pborman/uuid"
)

const (
	defaultInitialNumDocs   = 4096
	defaultInitialNumFields = 64
)

var (
	// errSegmentAlreadySealed is raised when trying to mutabe a sealed segment.
	errSegmentAlreadySealed = errors.New("segment is already sealed")
)

// immutableDatabaseSegment is an immutable database segment.
type immutableDatabaseSegment interface {
	// ID returns the segment ID.
	ID() string

	// MinTimeNanos returns the earliest event timestamp in this segment.
	// If the segment is empty, this returns 0.
	MinTimeNanos() int64

	// MaxTimeNanos returns the latest event timestamp in this segment.
	// If the segment is empty, this returns 0.
	MaxTimeNanos() int64

	// NumDocuments returns the number of documents (a.k.a. events) in this segment.
	NumDocuments() int32

	// Flush flushes the immutable segment to persistent storage.
	Flush(persistFns persist.Fns) error
}

// mutableDatabaseSegment is a mutable database segment.
type mutableDatabaseSegment interface {
	immutableDatabaseSegment

	// Write writes an event to the mutable segment.
	Write(ev event.Event) error

	// Seal seals the mutable segment and makes it immutable.
	Seal()

	// Close closes the mutable segment.
	Close() error
}

// TODO(xichen): Pool timestamp array and raw docs byte array.
// TODO(xichen): Treat tiemstamp and rawDocs as normal fields.
type dbSegment struct {
	sync.RWMutex

	id   string
	opts *Options

	// NB: We refer to an event containing a collection of fields a document
	// in conventional information retrieval terminology.
	sealed       bool
	numDocs      int32
	minTimeNanos int64
	maxTimeNanos int64
	timeNanos    []int64
	rawDocs      []string
	fields       map[hash.Hash]*fieldWriter
}

func newDatabaseSegment(
	opts *Options,
) *dbSegment {
	return &dbSegment{
		id:      uuid.New(),
		opts:    opts,
		rawDocs: make([]string, 0, defaultInitialNumDocs),
		fields:  make(map[hash.Hash]*fieldWriter, defaultInitialNumFields),
	}
}

func (s *dbSegment) ID() string { return s.id }

func (s *dbSegment) MinTimeNanos() int64 { return s.minTimeNanos }

func (s *dbSegment) MaxTimeNanos() int64 { return s.maxTimeNanos }

func (s *dbSegment) NumDocuments() int32 { return s.numDocs }

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
	s.timeNanos = append(s.timeNanos, ev.TimeNanos)
	s.rawDocs = append(s.rawDocs, unsafe.ToString(ev.RawData))

	for ev.FieldIter.Next() {
		f := ev.FieldIter.Current()
		// We store timestamp field separately.
		if len(f.Path) == 1 && f.Path[0] == s.opts.TimestampFieldName() {
			continue
		}
		pathHash := hash.StringArrayHash(f.Path, s.opts.FieldPathSeparator())
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

func (s *dbSegment) Seal() {
	s.Lock()
	s.sealed = true
	s.Unlock()
}

func (s *dbSegment) Flush(persistFns persist.Fns) error {
	if err := persistFns.WriteTimestamps(s.timeNanos); err != nil {
		return err
	}
	if err := persistFns.WriteRawDocs(s.rawDocs); err != nil {
		return err
	}
	for _, fw := range s.fields {
		// If we encounter an error when persisting a single field, don't continue
		// as the file on disk could be in a corrupt state.
		if err := fw.flush(persistFns); err != nil {
			return err
		}
	}
	return nil
}

func (s *dbSegment) Close() error {
	return fmt.Errorf("not implemented")
}
