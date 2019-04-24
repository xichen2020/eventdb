package storage

import (
	"errors"
	"sync"
	"time"

	"github.com/m3db/m3/src/x/clock"
)

// databaseMediator mediates actions among various database managers.
type databaseMediator interface {
	// Open opens the mediator.
	Open() error

	// Close closes the mediator.
	Close() error
}

type mediatorState int

const (
	tickCheckInterval = 5 * time.Second

	mediatorNotOpen mediatorState = iota
	mediatorOpen
	mediatorClosed
)

var (
	errMediatorAlreadyOpen   = errors.New("mediator is already open")
	errMediatorNotOpen       = errors.New("mediator is not open")
	errMediatorAlreadyClosed = errors.New("mediator is already closed")
)

type sleepFn func(time.Duration)

type mediator struct {
	sync.RWMutex
	databaseTickManager
	databaseFileSystemManager

	database Database
	opts     *Options
	nowFn    clock.NowFn
	sleepFn  sleepFn

	state    mediatorState
	closedCh chan struct{}
}

func newMediator(database database, opts *Options) databaseMediator {
	scope := opts.InstrumentOptions().MetricsScope()
	tickMgrOpts := opts.InstrumentOptions().SetMetricsScope(scope.SubScope("tick"))
	fsMgrOpts := opts.InstrumentOptions().SetMetricsScope(scope.SubScope("fs"))
	return &mediator{
		database:                  database,
		databaseTickManager:       newTickManager(database, opts.SetInstrumentOptions(tickMgrOpts)),
		databaseFileSystemManager: newFileSystemManager(database, opts.SetInstrumentOptions(fsMgrOpts)),
		opts:     opts,
		nowFn:    opts.ClockOptions().NowFn(),
		sleepFn:  time.Sleep,
		state:    mediatorNotOpen,
		closedCh: make(chan struct{}),
	}
}

func (m *mediator) Open() error {
	m.Lock()
	defer m.Unlock()
	if m.state != mediatorNotOpen {
		return errMediatorAlreadyOpen
	}
	m.state = mediatorOpen
	go m.ongoingTick()
	return nil
}

func (m *mediator) Tick() error {
	if err := m.databaseTickManager.Tick(); err != nil {
		return err
	}
	m.databaseFileSystemManager.Run()
	return nil
}

func (m *mediator) Close() error {
	m.Lock()
	defer m.Unlock()

	if m.state == mediatorNotOpen {
		return errMediatorNotOpen
	}
	if m.state == mediatorClosed {
		return errMediatorAlreadyClosed
	}
	m.state = mediatorClosed
	close(m.closedCh)
	return nil
}

func (m *mediator) ongoingTick() {
	for {
		select {
		case <-m.closedCh:
			return
		default:
			if err := m.Tick(); err == errTickInProgress {
				m.sleepFn(tickCheckInterval)
			} else if err != nil {
				log := m.opts.InstrumentOptions().Logger()
				log.Errorf("error within ongoingTick: %v", err)
			}
		}
	}
}
