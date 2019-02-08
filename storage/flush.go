package storage

import (
	"errors"
	"fmt"
	"sync"

	"github.com/xichen2020/eventdb/persist"

	"github.com/m3db/m3x/clock"
	xerrors "github.com/m3db/m3x/errors"
)

type databaseFlushManager interface {
	// Flush attempts to flush the database.
	Flush() error
}

var (
	errFlushOperationsInProgress = errors.New("flush operations already in progress")
)

type flushManagerState int

const (
	flushManagerIdle flushManagerState = iota
	// flushManagerNotIdle is used to protect the flush manager from concurrent use
	// when we haven't begun either a flush or snapshot.
	flushManagerNotIdle
	flushManagerFlushInProgress
)

type flushManager struct {
	sync.Mutex

	database database
	opts     *Options
	pm       persist.Manager

	nowFn clock.NowFn
	state flushManagerState
}

func newFlushManager(database database, opts *Options) *flushManager {
	return &flushManager{
		database: database,
		opts:     opts,
		pm:       opts.PersistManager(),
		nowFn:    opts.ClockOptions().NowFn(),
	}
}

func (m *flushManager) Flush() error {
	// Ensure only a single flush is happening at a time.
	m.Lock()
	if m.state != flushManagerIdle {
		m.Unlock()
		return errFlushOperationsInProgress
	}
	m.state = flushManagerNotIdle
	m.Unlock()

	defer m.setState(flushManagerIdle)

	persister, err := m.pm.StartPersist()
	if err != nil {
		return err
	}
	defer persister.Finish()

	namespaces, err := m.database.GetOwnedNamespaces()
	if err != nil {
		return err
	}

	m.setState(flushManagerFlushInProgress)

	var multiErr xerrors.MultiError
	for _, n := range namespaces {
		// NB(xichen): we still want to proceed if a namespace fails to flush its data.
		// Probably want to emit a counter here, but for now just log it.
		if err := n.Flush(persister); err != nil {
			detailedErr := fmt.Errorf("namespace %s failed to flush data: %v", n.ID(), err)
			multiErr = multiErr.Add(detailedErr)
		}
	}

	return multiErr.FinalError()
}

func (m *flushManager) setState(state flushManagerState) {
	m.Lock()
	m.state = state
	m.Unlock()
}
