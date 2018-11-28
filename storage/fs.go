package storage

import (
	"errors"
	"sync"

	"github.com/uber-go/tally"
)

type databaseFileSystemManager interface {
	// Run attempts to flush the database if deemed necessary.
	Run() error
}

var (
	errRunInProgress = errors.New("another run is in progress")
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
}

func newFileSystemManager(database database, opts *Options) *fileSystemManager {
	scope := opts.InstrumentOptions().MetricsScope().SubScope("fs")

	return &fileSystemManager{
		database:             database,
		databaseFlushManager: newFlushManager(database, opts),
		opts:                 opts,
		metrics:              newFileSystemManagerMetrics(scope),
	}
}

func (mgr *fileSystemManager) Run() error {
	mgr.Lock()
	defer mgr.Unlock()

	if mgr.status == runInProgress {
		return errRunInProgress
	}

	return mgr.Flush()
}
