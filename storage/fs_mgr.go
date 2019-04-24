package storage

import (
	"sync"

	"github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/log"
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

type fileSystemManager struct {
	sync.Mutex

	database database
	opts     *Options
	logger   log.Logger
	nowFn    clock.NowFn

	status runStatus
}

func newFileSystemManager(database database, opts *Options) *fileSystemManager {
	return &fileSystemManager{
		database: database,
		opts:     opts,
		logger:   opts.InstrumentOptions().Logger(),
		nowFn:    opts.ClockOptions().NowFn(),
	}
}

func (mgr *fileSystemManager) Run() bool {
	mgr.Lock()
	defer mgr.Unlock()

	if mgr.status == runInProgress {
		return false
	}

	mgr.status = runInProgress

	// TODO(xichen): Add background compaction here.
	mgr.status = runNotStarted

	return true
}
