package storage

import (
	"errors"
	"sync"
	"time"

	"github.com/m3db/m3x/clock"
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
	runCheckInterval = 5 * time.Second
	minRunInterval   = time.Minute

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

	database Database
	databaseFileSystemManager

	opts     *Options
	nowFn    clock.NowFn
	sleepFn  sleepFn
	state    mediatorState
	closedCh chan struct{}
}

func newMediator(database database, opts *Options) databaseMediator {
	return &mediator{
		database:                  database,
		databaseFileSystemManager: newFileSystemManager(database, opts),
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
	go m.runLoop()
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

func (m *mediator) runLoop() {
	for {
		select {
		case <-m.closedCh:
			return
		default:
			m.runOnce()
		}
	}
}

func (m *mediator) runOnce() {
	start := m.nowFn()
	if err := m.databaseFileSystemManager.Run(); err == errRunInProgress {
		// NB(xichen): if we attempt to run while another run
		// is in progress, throttle a little to avoid constantly
		// checking whether the ongoing run is finished.
		m.sleepFn(runCheckInterval)
	} else if err != nil {
		// On critical error, we retry immediately.
		log := m.opts.InstrumentOptions().Logger()
		log.Errorf("error within ongoingTick: %v", err)
	} else {
		// Otherwise, we make sure the subsequent run is at least
		// minRunInterval apart from the last one.
		took := m.nowFn().Sub(start)
		if took > minRunInterval {
			return
		}
		m.sleepFn(minRunInterval - took)
	}
}
