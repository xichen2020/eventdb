package handlers

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/xichen2020/eventdb/event"
	"github.com/xichen2020/eventdb/parser/json"
	"github.com/xichen2020/eventdb/parser/json/value"
	"github.com/xichen2020/eventdb/storage"
	"github.com/xichen2020/eventdb/x/unsafe"

	xerrors "github.com/m3db/m3x/errors"
)

// Service provides handlers for serving HTTP requests.
type Service interface {
	// Health returns service health.
	Health(w http.ResponseWriter, r *http.Request)

	// Write writes one or more JSON events. Events are delimited with newline.
	Write(w http.ResponseWriter, r *http.Request)
}

const (
	defaultInitialNumNamespaces = 4
)

var (
	errRequestMustBeGet  = xerrors.NewInvalidParamsError(errors.New("request must be GET"))
	errRequestMustBePost = xerrors.NewInvalidParamsError(errors.New("request must be POST"))

	delimiter = []byte("\n")
)

type service struct {
	db          storage.Database
	dbOpts      *storage.Options
	parserPool  *json.ParserPool
	idFn        IDFn
	namespaceFn NamespaceFn
	timeNanosFn TimeNanosFn
}

// NewService creates a new service.
func NewService(db storage.Database, opts *Options) Service {
	if opts == nil {
		opts = NewOptions()
	}
	return &service{
		db:          db,
		dbOpts:      db.Options(),
		parserPool:  opts.ParserPool(),
		idFn:        opts.IDFn(),
		namespaceFn: opts.NamespaceFn(),
		timeNanosFn: opts.TimeNanosFn(),
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

	w.Header().Set("Content-Type", "application/json")
	if httpMethod := strings.ToUpper(r.Method); httpMethod != http.MethodPost {
		writeErrorResponse(w, errRequestMustBePost)
		return
	}

	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		err = fmt.Errorf("cannot read body: %v", err)
		writeErrorResponse(w, err)
		return
	}

	if err := s.writeBatch(data); err != nil {
		err = fmt.Errorf("cannot write event batch for %s: %v", data, err)
		writeErrorResponse(w, err)
		return
	}

	writeSuccessResponse(w)
}

func (s *service) writeBatch(data []byte) error {
	// TODO(xichen): Pool the parser array and event array.
	var (
		start             int
		toReturn          []json.Parser
		eventBytes        []byte
		eventsByNamespace = make(map[string][]event.Event, defaultInitialNumNamespaces)
	)

	// NB(xichen): Return all parsers back to pool only after events are written.
	cleanup := func() {
		for _, p := range toReturn {
			s.parserPool.Put(p)
		}
	}
	defer cleanup()

	for start < len(data) {
		end := bytes.Index(data, delimiter)
		if end < 0 {
			end = len(data)
		}
		eventBytes = data[start:end]

		p := s.parserPool.Get()
		toReturn = append(toReturn, p)
		ns, ev, err := s.newEventFromBytes(p, eventBytes)
		if err != nil {
			return fmt.Errorf("cannot parse event from %s: %v", eventBytes, err)
		}
		nsStr := unsafe.ToString(ns)
		eventsByNamespace[nsStr] = append(eventsByNamespace[nsStr], ev)
		start = end + 1
	}

	var multiErr xerrors.MultiError
	for nsStr, events := range eventsByNamespace {
		nsBytes := unsafe.ToBytes(nsStr)
		if err := s.db.WriteBatch(nsBytes, events); err != nil {
			multiErr = multiErr.Add(err)
		}
	}

	return multiErr.FinalError()
}

func (s *service) newEventFromBytes(p json.Parser, data []byte) ([]byte, event.Event, error) {
	v, err := p.ParseBytes(data)
	if err != nil {
		err = fmt.Errorf("cannot parse event %s: %v", data, err)
		return nil, event.Event{}, err
	}

	// NB: Perhaps better to specify as a URL param.
	// Extract event namespace from JSON.
	namespaceFieldName := s.dbOpts.NamespaceFieldName()
	namespaceVal, ok := v.Get(namespaceFieldName)
	if !ok {
		err = fmt.Errorf("cannot find namespace field %s for event %v", namespaceFieldName, data)
		return nil, event.Event{}, err
	}
	namespace, err := s.namespaceFn(namespaceVal)
	if err != nil {
		err = fmt.Errorf("cannot determine namespace for event %s: %v", data, err)
		return nil, event.Event{}, err
	}

	// Extract event timestamp from JSON.
	timestampFieldName := s.dbOpts.TimestampFieldName()
	tsVal, ok := v.Get(timestampFieldName)
	if !ok {
		err = fmt.Errorf("cannot find timestamp field %s for event %v", timestampFieldName, data)
		return nil, event.Event{}, err
	}
	timeNanos, err := s.timeNanosFn(tsVal)
	if err != nil {
		err = fmt.Errorf("cannot determine timestamp for event %s: %v", data, err)
		return nil, event.Event{}, err
	}

	id, err := s.idFn(v)
	if err != nil {
		err = fmt.Errorf("cannot determine ID for event %s: %v", data, err)
		return nil, event.Event{}, err
	}

	// TODO(xichen): Pool the iterators.
	fieldIter := value.NewFieldIterator(v)
	ev := event.Event{
		ID:        id,
		TimeNanos: timeNanos,
		FieldIter: fieldIter,
		RawData:   data,
	}
	return namespace, ev, nil
}
