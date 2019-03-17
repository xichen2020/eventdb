package storage

import (
	"errors"
	"sync"

	indexfield "github.com/xichen2020/eventdb/index/field"
	"github.com/xichen2020/eventdb/index/segment"
	"github.com/xichen2020/eventdb/persist"

	"github.com/m3db/m3x/clock"
	xerrors "github.com/m3db/m3x/errors"
	"github.com/m3db/m3x/instrument"
	xlog "github.com/m3db/m3x/log"
	"github.com/uber-go/tally"
)

type segmentPayload struct {
	namespace []byte
	seg       segment.ImmutableSegment
	wg        sync.WaitGroup
	resultErr error
}

type databaseFlushManager interface {
	// Open opens the flush manager.
	Open() error

	// Enqueue enqueues a segment payload to flush.
	Enqueue(p *segmentPayload) error

	// Close closes the flush manager.
	Close() error
}

type flushManagerState int

const (
	flushManagerNotOpen flushManagerState = iota
	flushManagerOpen
	flushManagerClosed
)

var (
	errFlushManagerAlreadyOpenOrClosed = errors.New("flush manager is already open or closed")
	errFlushManagerNotOpenOrClosed     = errors.New("flush manager is not open or closed")
	errFlushManagerFlushQueueFull      = errors.New("flush manager flush queue is full")
)

type flushManagerMetrics struct {
	flush                  instrument.MethodMetrics
	enqueueSuccess         tally.Counter
	enqueueFullQueueErrors tally.Counter
}

func newFlushManagerMetrics(scope tally.Scope) flushManagerMetrics {
	enqueueScope := scope.Tagged(map[string]string{"action": "enqueue"})
	return flushManagerMetrics{
		flush:          instrument.NewMethodMetrics(scope, "flush", 1.0),
		enqueueSuccess: enqueueScope.Counter("success"),
		enqueueFullQueueErrors: enqueueScope.Tagged(map[string]string{
			"reason": "full-queue",
		}).Counter("errors"),
	}
}

type flushManager struct {
	sync.RWMutex

	opts   *Options
	nowFn  clock.NowFn
	logger xlog.Logger
	pm     persist.Manager

	state     flushManagerState
	doneCh    chan struct{}
	unflushed chan *segmentPayload
	wg        sync.WaitGroup
	metrics   flushManagerMetrics
}

func newFlushManager(opts *Options) *flushManager {
	instrumentOpts := opts.InstrumentOptions()
	return &flushManager{
		opts:      opts,
		nowFn:     opts.ClockOptions().NowFn(),
		logger:    instrumentOpts.Logger(),
		pm:        opts.PersistManager(),
		doneCh:    make(chan struct{}),
		unflushed: make(chan *segmentPayload, opts.FlushQueueSize()),
		metrics:   newFlushManagerMetrics(instrumentOpts.MetricsScope()),
	}
}

func (m *flushManager) Open() error {
	m.Lock()
	defer m.Unlock()

	if m.state != flushManagerNotOpen {
		return errFlushManagerAlreadyOpenOrClosed
	}
	m.state = flushManagerOpen

	m.wg.Add(1)
	go func() {
		m.flushLoop()
		m.wg.Done()
	}()

	return nil
}

func (m *flushManager) Enqueue(p *segmentPayload) error {
	m.RLock()
	defer m.RUnlock()

	if m.state != flushManagerOpen {
		return errFlushManagerNotOpenOrClosed
	}
	select {
	case m.unflushed <- p:
		m.metrics.enqueueSuccess.Inc(1)
	default:
		m.metrics.enqueueFullQueueErrors.Inc(1)
		return errFlushManagerFlushQueueFull
	}
	return nil
}

func (m *flushManager) Close() error {
	m.Lock()
	defer m.Unlock()

	if m.state != flushManagerOpen {
		return errFlushManagerNotOpenOrClosed
	}
	m.state = flushManagerClosed
	close(m.doneCh)
	m.wg.Wait()

	return nil
}

func (m *flushManager) flushLoop() {
	for {
		select {
		case <-m.doneCh:
			return
		case p := <-m.unflushed:
			callStart := m.nowFn()
			p.resultErr = m.flushSegment(p)
			p.wg.Done()
			dur := m.nowFn().Sub(callStart)
			if p.resultErr == nil {
				m.metrics.flush.ReportSuccess(dur)
			} else {
				m.logger.WithFields(
					xlog.NewField("namespace", string(p.namespace)),
					xlog.NewErrField(p.resultErr),
				).Error("error flushing segment")
				m.metrics.flush.ReportError(dur)
			}
		}
	}
}

func (m *flushManager) flushSegment(p *segmentPayload) error {
	segmentMeta := p.seg.Metadata()
	if segmentMeta.NumDocs == 0 {
		return nil
	}

	persister, err := m.pm.StartPersist()
	if err != nil {
		return err
	}
	defer persister.Finish()

	var multiErr xerrors.MultiError
	prepareOpts := persist.PrepareOptions{
		Namespace:   p.namespace,
		SegmentMeta: segmentMeta,
	}
	prepared, err := persister.Prepare(prepareOpts)
	if err != nil {
		return err
	}

	// TODO(xichen): Pool these.
	segmentFields := make([]indexfield.DocsField, 0, segmentMeta.NumFields)
	p.seg.ForEach(func(f *segment.Field) {
		if df := f.DocsField(); df != nil {
			segmentFields = append(segmentFields, df)
		}
	})
	defer func() {
		// Close the docs field shallow copies.
		for i := range segmentFields {
			segmentFields[i].Close()
			segmentFields[i] = nil
		}
	}()

	if err := prepared.Persist.WriteFields(segmentFields); err != nil {
		multiErr = multiErr.Add(err)
	}
	if err := prepared.Close(); err != nil {
		multiErr = multiErr.Add(err)
	}
	return multiErr.FinalError()
}
