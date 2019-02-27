package storage

import (
	"context"
	"errors"
	"time"

	"github.com/m3db/m3x/clock"
	xerrors "github.com/m3db/m3x/errors"
	"github.com/uber-go/tally"
)

type databaseTickManager interface {
	// Tick performs a tick against the database.
	Tick() error
}

var (
	errEmptyNamespaces = errors.New("empty namespaces")
	errTickInProgress  = errors.New("another tick is in progress")
)

type tickManagerMetrics struct {
	tickWorkDuration tally.Timer
}

func newTickManagerMetrics(scope tally.Scope) tickManagerMetrics {
	return tickManagerMetrics{
		tickWorkDuration: scope.Timer("work-duration"),
	}
}

type tickManager struct {
	database database
	opts     *Options
	nowFn    clock.NowFn
	sleepFn  sleepFn

	metrics tickManagerMetrics
}

func newTickManager(database database, opts *Options) databaseTickManager {
	return &tickManager{
		database: database,
		opts:     opts,
		nowFn:    opts.ClockOptions().NowFn(),
		sleepFn:  time.Sleep,
		metrics:  newTickManagerMetrics(opts.InstrumentOptions().MetricsScope()),
	}
}

func (mgr *tickManager) Tick() error {
	namespaces, err := mgr.database.GetOwnedNamespaces()
	if err != nil {
		return err
	}
	if len(namespaces) == 0 {
		return errEmptyNamespaces
	}

	var (
		start    = mgr.nowFn()
		multiErr xerrors.MultiError
	)
	for _, n := range namespaces {
		// TODO(xichen): Set up timeout and cancellation logic.
		multiErr = multiErr.Add(n.Tick(context.Background()))
	}

	took := mgr.nowFn().Sub(start)
	mgr.metrics.tickWorkDuration.Record(took)
	tickMinInterval := mgr.opts.TickMinInterval()
	if took >= tickMinInterval {
		return nil
	}
	mgr.sleepFn(tickMinInterval - took)

	return multiErr.FinalError()
}
