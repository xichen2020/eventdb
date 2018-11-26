package handlers

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/xichen2020/eventdb/event"
	"github.com/xichen2020/eventdb/parser/json"
	"github.com/xichen2020/eventdb/storage"
	"github.com/xichen2020/eventdb/value"

	xerrors "github.com/m3db/m3x/errors"
)

// Service provides handlers for serving HTTP requests.
type Service interface {
	// Health returns service health.
	Health(w http.ResponseWriter, r *http.Request)

	// Write writes a single JSON event.
	Write(w http.ResponseWriter, r *http.Request)
}

var (
	errRequestMustBeGet  = xerrors.NewInvalidParamsError(errors.New("request must be GET"))
	errRequestMustBePost = xerrors.NewInvalidParamsError(errors.New("request must be POST"))
)

type service struct {
	db          storage.Database
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

	p := s.parserPool.Get()
	// TODO(xichen): Check this is correct wrt the parser lifetime.
	defer s.parserPool.Put(p)

	v, err := p.ParseBytes(data)
	if err != nil {
		err = fmt.Errorf("cannot parse event %s: %v", data, err)
		writeErrorResponse(w, err)
		return
	}

	// NB: Perhaps better to specify as a URL param.
	namespace, err := s.namespaceFn(v)
	if err != nil {
		err = fmt.Errorf("cannot determine namespace for event %s: %v", data, err)
		writeErrorResponse(w, err)
		return
	}

	id, err := s.idFn(v)
	if err != nil {
		err = fmt.Errorf("cannot determine ID for event %s: %v", data, err)
		writeErrorResponse(w, err)
		return
	}

	timeNanos, err := s.timeNanosFn(v)
	if err != nil {
		err = fmt.Errorf("cannot determine timestamp for event %s: %v", data, err)
		writeErrorResponse(w, err)
		return
	}

	// TODO(xichen): Pool the iterators.
	fieldIter := value.NewFieldIterator(v)
	ev := event.Event{
		ID:        id,
		TimeNanos: timeNanos,
		FieldIter: fieldIter,
		RawData:   data,
	}

	err = s.db.Write(namespace, ev)
	if err != nil {
		err = fmt.Errorf("cannot write event %s: %v", data, err)
		writeErrorResponse(w, err)
		return
	}

	writeSuccessResponse(w)
}
