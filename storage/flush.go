package storage

import (
	"sync"
	"time"

	"github.com/xichen2020/eventdb/persist"

	"github.com/m3db/m3x/clock"
)

type databaseFlushManager interface {
	// ComputeFlushTargets computes the list of namespaces eligible for flushing.
	ComputeFlushTargets(namespaces []databaseNamespace) []databaseNamespace

	// Flush attempts to flush the database if deemed necessary.
	Flush(namespace databaseNamespace) error
}

type flushManager struct {
	sync.Mutex

	database database
	pm       persist.Manager
	opts     *Options

	nowFn   clock.NowFn
	sleepFn sleepFn
}

func newFlushManager(database database, opts *Options) *flushManager {
	return &flushManager{
		database: database,
		// pm: fs.NewPersistManager()
		opts:    opts,
		nowFn:   opts.ClockOptions().NowFn(),
		sleepFn: time.Sleep,
	}
}

// NB: This is just a no-op implementation for now.
// In reality, this should be determined based on memory usage of each namespace.
func (mgr *flushManager) ComputeFlushTargets(namespaces []databaseNamespace) []databaseNamespace {
	return namespaces
}

func (mgr *flushManager) Flush(namespace databaseNamespace) error {
	return namespace.Flush(mgr.pm)
}
