package fs

import (
	"errors"
	"sync"

	"github.com/xichen2020/eventdb/persist"

	"github.com/m3db/m3x/clock"
	"github.com/pilosa/pilosa/roaring"
)

var (
	errPersistManagerNotIdle                        = errors.New("persist manager cannot start persist, not idle")
	errPersistManagerNotPersisting                  = errors.New("persist manager cannot finish persisting, not persisting")
	errPersistManagerCannotPrepareDataNotPersisting = errors.New("persist manager cannot prepare data, not persisting")
)

type persistManagerStatus int

const (
	persistManagerIdle persistManagerStatus = iota
	persistManagerPersisting
)

type persistManager struct {
	sync.RWMutex

	opts           *Options
	filePathPrefix string
	writer         segmentWriter
	nowFn          clock.NowFn

	status persistManagerStatus
	pp     persist.PreparedPersister
}

// NewPersistManager creates a new filesystem persist manager.
// TODO(xichen): Persistence rate limiting.
func NewPersistManager(opts *Options) persist.Manager {
	if opts == nil {
		opts = NewOptions()
	}
	pm := &persistManager{
		opts:           opts,
		filePathPrefix: opts.FilePathPrefix(),
		nowFn:          opts.ClockOptions().NowFn(),
		writer:         newSegmentWriter(opts),
		status:         persistManagerIdle,
	}
	pm.pp = persist.PreparedPersister{
		Persist: persist.Fns{
			WriteNullField:   pm.writeNullField,
			WriteBoolField:   pm.writeBoolField,
			WriteIntField:    pm.writeIntField,
			WriteDoubleField: pm.writeDoubleField,
			WriteStringField: pm.writeStringField,
		},
		Close: pm.close,
	}
	return pm
}

func (pm *persistManager) reset() {
	pm.status = persistManagerIdle
}

// StartPersist is called to begin the persistence process.
func (pm *persistManager) StartPersist() (persist.Persister, error) {
	pm.Lock()
	defer pm.Unlock()

	if pm.status != persistManagerIdle {
		return nil, errPersistManagerNotIdle
	}
	pm.status = persistManagerPersisting

	return pm, nil
}

// Prepare returns a prepared persist object which can be used to persist data.
func (pm *persistManager) Prepare(opts persist.PrepareOptions) (persist.PreparedPersister, error) {
	var prepared persist.PreparedPersister

	pm.RLock()
	status := pm.status
	pm.RUnlock()

	if status != persistManagerPersisting {
		return prepared, errPersistManagerCannotPrepareDataNotPersisting
	}

	writerOpts := writerOpenOptions{
		Namespace:    opts.Namespace,
		Shard:        opts.Shard,
		MinTimeNanos: opts.MinTimeNanos,
		MaxTimeNanos: opts.MaxTimeNanos,
		NumDocuments: opts.NumDocuments,
	}
	if err := pm.writer.Open(writerOpts); err != nil {
		return prepared, err
	}

	return pm.pp, nil
}

func (pm *persistManager) writeNullField(fieldPath []string, docIDs *roaring.Bitmap) error {
	return pm.writer.WriteNullField(fieldPath, docIDs)
}

func (pm *persistManager) writeBoolField(fieldPath []string, docIDs *roaring.Bitmap, vals []bool) error {
	return pm.writer.WriteBoolField(fieldPath, docIDs, vals)
}

func (pm *persistManager) writeIntField(fieldPath []string, docIDs *roaring.Bitmap, vals []int) error {
	return pm.writer.WriteIntField(fieldPath, docIDs, vals)
}

func (pm *persistManager) writeDoubleField(fieldPath []string, docIDs *roaring.Bitmap, vals []float64) error {
	return pm.writer.WriteDoubleField(fieldPath, docIDs, vals)
}

func (pm *persistManager) writeStringField(fieldPath []string, docIDs *roaring.Bitmap, vals []string) error {
	return pm.writer.WriteStringField(fieldPath, docIDs, vals)
}

func (pm *persistManager) close() error {
	return pm.writer.Close()
}

// Done is called to finish the data persistence process.
func (pm *persistManager) Done() error {
	pm.Lock()
	defer pm.Unlock()

	if pm.status != persistManagerPersisting {
		return errPersistManagerNotPersisting
	}

	// Reset state
	pm.reset()

	return nil
}
