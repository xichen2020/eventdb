package storage

import (
	"sync"

	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/log"
	"github.com/uber-go/tally"
)

type databaseFileSystemManager interface {
	// Run attempts to flush the database if deemed necessary.
	Run() bool
}

type runStatus int

// nolint:deadcode,megacheck,varcheck
const (
	runNotStarted runStatus = iota
	runInProgress
)

type fileSystemManagerMetrics struct {
	flushDuration tally.Timer
}

func newFileSystemManagerMetrics(scope tally.Scope) fileSystemManagerMetrics {
	return fileSystemManagerMetrics{
		flushDuration: scope.Timer("flush-duration"),
	}
}

type fileSystemManager struct {
	sync.Mutex
	databaseFlushManager

	database database
	opts     *Options
	logger   log.Logger
	nowFn    clock.NowFn

	status  runStatus
	metrics fileSystemManagerMetrics
}

func newFileSystemManager(database database, opts *Options) *fileSystemManager {
	scope := opts.InstrumentOptions().MetricsScope()
	return &fileSystemManager{
		database:             database,
		databaseFlushManager: newFlushManager(database, opts),
		opts:                 opts,
		logger:               opts.InstrumentOptions().Logger(),
		nowFn:                opts.ClockOptions().NowFn(),
		metrics:              newFileSystemManagerMetrics(scope),
	}
}

func (mgr *fileSystemManager) Run() bool {
	mgr.Lock()
	defer mgr.Unlock()

	if mgr.status == runInProgress {
		return false
	}

	mgr.status = runInProgress

	flushStart := mgr.nowFn()
	if err := mgr.Flush(); err != nil {
		mgr.logger.Errorf("error when flushing data: %v", err)
	}
	took := mgr.nowFn().Sub(flushStart)
	mgr.metrics.flushDuration.Record(took)

	mgr.status = runNotStarted
	return true
}
