package storage

import (
	"errors"
	"sync"
	"time"

	"github.com/m3db/m3x/clock"
	xerrors "github.com/m3db/m3x/errors"
	"github.com/uber-go/tally"
)

type databaseFileSystemManager interface {
	// Run attempts to flush the database if deemed necessary.
	Run() error
}

var (
	errEmptyNamespaces = errors.New("empty namespaces")
	errRunInProgress   = errors.New("another run is in progress")
)

type runStatus int

// nolint:deadcode,megacheck,varcheck
const (
	runNotStarted runStatus = iota
	runInProgress
)

type fileSystemManagerMetrics struct {
	runDuration tally.Timer
}

func newFileSystemManagerMetrics(scope tally.Scope) fileSystemManagerMetrics {
	return fileSystemManagerMetrics{
		runDuration: scope.Timer("duration"),
	}
}

type fileSystemManager struct {
	sync.Mutex
	databaseFlushManager

	database database
	opts     *Options

	status  runStatus
	metrics fileSystemManagerMetrics
	nowFn   clock.NowFn
	sleepFn sleepFn
}

func newFileSystemManager(database database, opts *Options) *fileSystemManager {
	scope := opts.InstrumentOptions().MetricsScope().SubScope("fs")

	return &fileSystemManager{
		database:             database,
		databaseFlushManager: newFlushManager(database, opts),
		opts:                 opts,
		nowFn:                opts.ClockOptions().NowFn(),
		sleepFn:              time.Sleep,
		metrics:              newFileSystemManagerMetrics(scope),
	}
}

func (mgr *fileSystemManager) Run() error {
	mgr.Lock()
	defer mgr.Unlock()

	if mgr.status == runInProgress {
		return errRunInProgress
	}

	namespaces, err := mgr.database.GetOwnedNamespaces()
	if err != nil {
		return err
	}
	if len(namespaces) == 0 {
		return errEmptyNamespaces
	}

	// Determine which namespaces are in scope for flushing.
	toFlush := mgr.ComputeFlushTargets(namespaces)
	if len(toFlush) == 0 {
		// Nothing to do.
		return nil
	}

	var (
		start    = mgr.nowFn()
		multiErr xerrors.MultiError
	)
	for _, n := range namespaces {
		multiErr = multiErr.Add(mgr.Flush(n))
	}

	took := mgr.nowFn().Sub(start)
	mgr.metrics.runDuration.Record(took)
	return multiErr.FinalError()
}
