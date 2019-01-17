package handlers

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"

	"github.com/xichen2020/eventdb/document"
	jsonparser "github.com/xichen2020/eventdb/parser/json"
	"github.com/xichen2020/eventdb/parser/json/value"
	"github.com/xichen2020/eventdb/query"
	"github.com/xichen2020/eventdb/storage"
	"github.com/xichen2020/eventdb/x/unsafe"

	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/context"
	xerrors "github.com/m3db/m3x/errors"
	"github.com/m3db/m3x/instrument"
	"github.com/uber-go/tally"
)

// Service provides handlers for serving HTTP requests.
type Service interface {
	// Health returns service health.
	Health(w http.ResponseWriter, r *http.Request)

	// Write writes one or more JSON events. Documents are delimited with newline.
	Write(w http.ResponseWriter, r *http.Request)

	// Query performs an document query.
	Query(w http.ResponseWriter, r *http.Request)
}

const (
	defaultInitialNumNamespaces = 4
	batchSizeBucketVersion      = 1
	bucketSize                  = 200
	numBuckets                  = 20
)

var (
	errRequestMustBeGet  = xerrors.NewInvalidParamsError(errors.New("request must be GET"))
	errRequestMustBePost = xerrors.NewInvalidParamsError(errors.New("request must be POST"))

	delimiter = []byte("\n")
)

type serviceMetrics struct {
	// `/query` endpoint metrics.
	query instrument.MethodMetrics

	// `/write` endpoint metrics.
	write         instrument.MethodMetrics
	parseDocs     tally.Timer
	batchSizeHist tally.Histogram
}

func newServiceMetrics(scope tally.Scope, samplingRate float64) serviceMetrics {
	batchSizeBuckets := tally.MustMakeLinearValueBuckets(0, bucketSize, numBuckets)
	return serviceMetrics{
		query:     instrument.NewMethodMetrics(scope, "query", samplingRate),
		write:     instrument.NewMethodMetrics(scope, "write", samplingRate),
		parseDocs: instrument.MustCreateSampledTimer(scope.Timer("parse-docs"), samplingRate),
		batchSizeHist: scope.Tagged(map[string]string{
			"bucket-version": strconv.Itoa(batchSizeBucketVersion),
		}).Histogram("batch-size", batchSizeBuckets),
	}
}

type service struct {
	db          storage.Database
	dbOpts      *storage.Options
	contextPool context.Pool
	parserPool  *jsonparser.ParserPool
	idFn        IDFn
	namespaceFn NamespaceFn
	timeNanosFn TimeNanosFn
	nowFn       clock.NowFn

	metrics serviceMetrics
}

// NewService creates a new service.
func NewService(db storage.Database, opts *Options) Service {
	if opts == nil {
		opts = NewOptions()
	}
	instrumentOpts := opts.InstrumentOptions()
	scope := instrumentOpts.MetricsScope()
	samplingRate := instrumentOpts.MetricsSamplingRate()
	return &service{
		db:          db,
		dbOpts:      db.Options(),
		contextPool: db.Options().ContextPool(),
		parserPool:  opts.ParserPool(),
		idFn:        opts.IDFn(),
		namespaceFn: opts.NamespaceFn(),
		timeNanosFn: opts.TimeNanosFn(),
		nowFn:       opts.ClockOptions().NowFn(),
		metrics:     newServiceMetrics(scope, samplingRate),
	}
}

func (s *service) Health(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	if httpMethod := strings.ToUpper(r.Method); httpMethod != http.MethodGet {
		writeErrorResponse(w, errRequestMustBeGet)
		return
	}
	writeSuccessResponse(w)
}

func (s *service) Write(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	writeStart := s.nowFn()

	w.Header().Set("Content-Type", "application/json")
	if httpMethod := strings.ToUpper(r.Method); httpMethod != http.MethodPost {
		writeErrorResponse(w, errRequestMustBePost)
		s.metrics.write.ReportError(s.nowFn().Sub(writeStart))
		return
	}

	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		err = fmt.Errorf("cannot read body: %v", err)
		writeErrorResponse(w, err)
		s.metrics.write.ReportError(s.nowFn().Sub(writeStart))
		return
	}

	if err := s.writeBatch(data); err != nil {
		err = fmt.Errorf("cannot write document batch for %s: %v", data, err)
		writeErrorResponse(w, err)
		s.metrics.write.ReportError(s.nowFn().Sub(writeStart))
		return
	}

	writeSuccessResponse(w)
	s.metrics.write.ReportSuccess(s.nowFn().Sub(writeStart))
}

func (s *service) Query(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	queryStart := s.nowFn()

	w.Header().Set("Content-Type", "application/json")
	if httpMethod := strings.ToUpper(r.Method); httpMethod != http.MethodPost {
		writeErrorResponse(w, errRequestMustBePost)
		s.metrics.query.ReportError(s.nowFn().Sub(queryStart))
		return
	}

	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		err = fmt.Errorf("cannot read body: %v", err)
		writeErrorResponse(w, err)
		s.metrics.query.ReportError(s.nowFn().Sub(queryStart))
		return
	}

	var q query.RawQuery
	if err := json.Unmarshal(data, &q); err != nil {
		err = fmt.Errorf("unable to unmarshal request into a raw query: %v", err)
		writeErrorResponse(w, err)
		s.metrics.query.ReportError(s.nowFn().Sub(queryStart))
		return
	}

	parseOpts := query.ParseOptions{
		FieldPathSeparator:    s.dbOpts.FieldPathSeparator(),
		FieldHashFn:           s.dbOpts.FieldHashFn(),
		TimestampFieldPath:    s.dbOpts.TimestampFieldPath(),
		RawDocSourceFieldPath: s.dbOpts.RawDocSourceFieldPath(),
	}
	pq, err := q.Parse(parseOpts)
	if err != nil {
		err = fmt.Errorf("unable to parse raw query %v: %v", q, err)
		writeErrorResponse(w, err)
		s.metrics.query.ReportError(s.nowFn().Sub(queryStart))
		return
	}

	if pq.IsRaw() {
		// Performing a raw query.
		rq, err := pq.RawQuery()
		if err != nil {
			err = fmt.Errorf("error creating raw query from %v", pq)
			writeErrorResponse(w, err)
			s.metrics.query.ReportError(s.nowFn().Sub(queryStart))
			return
		}

		ctx := s.contextPool.Get()
		defer ctx.Close()

		res, err := s.db.QueryRaw(ctx, rq)
		if err != nil {
			err = fmt.Errorf("error performing raw query %v against database namespace %s: %v", rq, rq.Namespace, err)
			writeErrorResponse(w, err)
			s.metrics.query.ReportError(s.nowFn().Sub(queryStart))
			return
		}

		writeResponse(w, res, nil)
		s.metrics.query.ReportSuccess(s.nowFn().Sub(queryStart))
		return
	}

	// Performing a grouped query.
	ctx := s.contextPool.Get()
	defer ctx.Close()

	gq := pq.GroupedQuery()
	res, err := s.db.QueryGrouped(ctx, gq)
	if err != nil {
		err = fmt.Errorf("error performing grouped query %v against database namespace %s: %v", gq, gq.Namespace, err)
		writeErrorResponse(w, err)
		s.metrics.query.ReportError(s.nowFn().Sub(queryStart))
		return
	}

	writeResponse(w, res, nil)
	s.metrics.query.ReportSuccess(s.nowFn().Sub(queryStart))
}

func (s *service) writeBatch(data []byte) error {
	// TODO(xichen): Pool the parser array and document array.
	var (
		batchSize       int
		start           int
		toReturn        []jsonparser.Parser
		docBytes        []byte
		docsByNamespace = make(map[string][]document.Document, defaultInitialNumNamespaces)
	)

	// NB(xichen): Return all parsers back to pool only after events are written.
	cleanup := func() {
		for _, p := range toReturn {
			s.parserPool.Put(p)
		}
	}
	defer cleanup()

	parseDocsStart := s.nowFn()
	for start < len(data) {
		end := bytes.Index(data, delimiter)
		if end < 0 {
			end = len(data)
		}
		docBytes = data[start:end]

		p := s.parserPool.Get()
		toReturn = append(toReturn, p)
		ns, doc, err := s.newDocumentFromBytes(p, docBytes)
		if err != nil {
			return fmt.Errorf("cannot parse document from %s: %v", docBytes, err)
		}
		nsStr := unsafe.ToString(ns)
		docsByNamespace[nsStr] = append(docsByNamespace[nsStr], doc)
		start = end + 1
		batchSize++
	}
	s.metrics.parseDocs.Record(s.nowFn().Sub(parseDocsStart))
	s.metrics.batchSizeHist.RecordValue(float64(batchSize))

	var multiErr xerrors.MultiError
	for nsStr, events := range docsByNamespace {
		nsBytes := unsafe.ToBytes(nsStr)
		if err := s.db.WriteBatch(nsBytes, events); err != nil {
			multiErr = multiErr.Add(err)
		}
	}

	return multiErr.FinalError()
}

func (s *service) newDocumentFromBytes(p jsonparser.Parser, data []byte) ([]byte, document.Document, error) {
	v, err := p.ParseBytes(data)
	if err != nil {
		err = fmt.Errorf("cannot parse document %s: %v", data, err)
		return nil, document.Document{}, err
	}

	// NB: Perhaps better to specify as a URL param.
	// Extract document namespace from JSON.
	namespaceFieldName := s.dbOpts.NamespaceFieldName()
	namespaceVal, ok := v.Get(namespaceFieldName)
	if !ok {
		err = fmt.Errorf("cannot find namespace field %s for document %v", namespaceFieldName, data)
		return nil, document.Document{}, err
	}
	namespace, err := s.namespaceFn(namespaceVal)
	if err != nil {
		err = fmt.Errorf("cannot determine namespace for document %s: %v", data, err)
		return nil, document.Document{}, err
	}

	// Extract document timestamp from JSON.
	timestampFieldPath := s.dbOpts.TimestampFieldPath()
	tsVal, ok := v.Get(timestampFieldPath...)
	if !ok {
		err = fmt.Errorf("cannot find timestamp field %s for document %v", timestampFieldPath, data)
		return nil, document.Document{}, err
	}
	timeNanos, err := s.timeNanosFn(tsVal)
	if err != nil {
		err = fmt.Errorf("cannot determine timestamp for document %s: %v", data, err)
		return nil, document.Document{}, err
	}

	id, err := s.idFn(v)
	if err != nil {
		err = fmt.Errorf("cannot determine ID for document %s: %v", data, err)
		return nil, document.Document{}, err
	}

	// TODO(xichen): Pool the iterators.
	fieldIter := value.NewFieldIterator(v)
	doc := document.Document{
		ID:        id,
		TimeNanos: timeNanos,
		FieldIter: fieldIter,
		RawData:   data,
	}
	return namespace, doc, nil
}
