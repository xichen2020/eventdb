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

	xerrors "github.com/m3db/m3x/errors"
)

// Service provides handlers for serving HTTP requests.
type Service interface {
	// Health returns service health.
	Health(w http.ResponseWriter, r *http.Request)

	// Write writes a single JSON event.
	Write(w http.ResponseWriter, r *http.Request)

	// WriteBulk writes muiltple new line delimited JSON events.
	WriteBulk(w http.ResponseWriter, r *http.Request)
}

var (
	errRequestMustBeGet  = xerrors.NewInvalidParamsError(errors.New("request must be GET"))
	errRequestMustBePost = xerrors.NewInvalidParamsError(errors.New("request must be POST"))

	writeBulkDelimiter = []byte("\n")
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

	s.writeBulk(w, data)
}

func (s *service) WriteBulk(w http.ResponseWriter, r *http.Request) {
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

	s.writeBulk(w, data)
}

func (s *service) writeBulk(w http.ResponseWriter, data []byte) {
	p := s.parserPool.Get()
	// TODO(xichen): Check this is correct wrt the parser lifetime.
	defer s.parserPool.Put(p)

	// []byte is not a valid map key type so using string instead.
	eventMapping := make(map[string][]event.Event)
	var (
		start int
		stop  int
	)
	for stop < len(data) {
		stop = bytes.Index(data[start:], writeBulkDelimiter)
		if stop == -1 {
			stop = len(data)
		}

		v, err := p.ParseBytes(data[start:stop])
		if err != nil {
			err = fmt.Errorf("cannot parse event %s: %v", data, err)
			writeErrorResponse(w, err)
			return
		}

		// NB: Perhaps better to specify as a URL param.
		// Extract event namespace from JSON.
		namespaceFieldName := s.dbOpts.NamespaceFieldName()
		namespaceVal, ok := v.Get(namespaceFieldName)
		if !ok {
			err = fmt.Errorf("cannot find namespace field %s for event %v", namespaceFieldName, data[start:stop])
			writeErrorResponse(w, err)
			return
		}
		namespace, err := s.namespaceFn(namespaceVal)
		if err != nil {
			err = fmt.Errorf("cannot determine namespace for event %s: %v", data[start:stop], err)
			writeErrorResponse(w, err)
			return
		}

		// Extract event timestamp from JSON.
		timestampFieldName := s.dbOpts.TimestampFieldName()
		tsVal, ok := v.Get(timestampFieldName)
		if !ok {
			err = fmt.Errorf("cannot find timestamp field %s for event %v", timestampFieldName, data[start:stop])
			writeErrorResponse(w, err)
			return
		}
		timeNanos, err := s.timeNanosFn(tsVal)
		if err != nil {
			err = fmt.Errorf("cannot determine timestamp for event %s: %v", data[start:stop], err)
			writeErrorResponse(w, err)
			return
		}

		id, err := s.idFn(v)
		if err != nil {
			err = fmt.Errorf("cannot determine ID for event %s: %v", data[start:stop], err)
			writeErrorResponse(w, err)
			return
		}

		// TODO(xichen): Pool the iterators.
		fieldIter := value.NewFieldIterator(v)
		namespaceString := string(namespace)
		if _, ok := eventMapping[namespaceString]; !ok {
			eventMapping[namespaceString] = make([]event.Event, 0)
		}
		eventMapping[namespaceString] = append(eventMapping[namespaceString], event.Event{
			ID:        id,
			TimeNanos: timeNanos,
			FieldIter: fieldIter,
			RawData:   data[start:stop],
		})

		// Housekeeping to move forward through the data slice.
		start = stop
	}

	// Bulk write for each namespace.
	multiErr := xerrors.NewMultiError()
	for ns, evs := range eventMapping {
		if err := s.db.WriteBulk([]byte(ns), evs); err != nil {
			multiErr = multiErr.Add(err)
		}
	}

	err := multiErr.FinalError()
	if err != nil {
		err = fmt.Errorf("cannot write event %s: %v", data, err)
		writeErrorResponse(w, err)
		return
	}

	writeSuccessResponse(w)
}
