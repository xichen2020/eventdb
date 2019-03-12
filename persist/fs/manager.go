package fs

import (
	"errors"
	"sync"

	indexfield "github.com/xichen2020/eventdb/index/field"
	"github.com/xichen2020/eventdb/persist"
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
		writer:         newSegmentWriter(opts),
		status:         persistManagerIdle,
	}
	pm.pp = persist.PreparedPersister{
		Persist: persist.Fns{
			WriteFields: pm.writeFields,
		},
		Close: pm.writer.Finish,
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

	writerOpts := writerStartOptions{
		Namespace:   opts.Namespace,
		SegmentMeta: opts.SegmentMeta,
	}
	if err := pm.writer.Start(writerOpts); err != nil {
		return prepared, err
	}

	return pm.pp, nil
}

func (pm *persistManager) writeFields(fields []indexfield.DocsField) error {
	return pm.writer.WriteFields(fields)
}

// Finish is called to finish the data persistence process.
func (pm *persistManager) Finish() error {
	pm.Lock()
	defer pm.Unlock()

	if pm.status != persistManagerPersisting {
		return errPersistManagerNotPersisting
	}

	// Reset state
	pm.reset()

	return nil
}

// Close is a no-op for now.
func (pm *persistManager) Close() error {
	return nil
}
