package handlers

import (
	"time"

	"github.com/xichen2020/eventdb/parser/json"
	"github.com/xichen2020/eventdb/parser/json/value"
	"github.com/xichen2020/eventdb/x/unsafe"

	"github.com/pborman/uuid"
)

// IDFn determines the ID of a JSON document.
type IDFn func(value *value.Value) ([]byte, error)

// NamespaceFn determines the namespace a JSON document belongs to.
type NamespaceFn func(value *value.Value) ([]byte, error)

// TimeNanosFn determines the timestamp of a JSON document in nanoseconds.
type TimeNanosFn func(value *value.Value) (int64, error)

// Options provide a set of options for service handlers.
type Options struct {
	parserPool  *json.ParserPool
	idFn        IDFn
	namespaceFn NamespaceFn
	timeNanosFn TimeNanosFn
}

// NewOptions create a new set of options.
func NewOptions() *Options {
	o := &Options{
		idFn:        defaultIDFn,
		namespaceFn: defaultNamespaceFn,
		timeNanosFn: defaultTimeNanosFn,
	}
	o.initPools()
	return o
}

// SetParserPool sets the pool for JSON parsers.
func (o *Options) SetParserPool(value *json.ParserPool) *Options {
	opts := *o
	opts.parserPool = value
	return &opts
}

// ParserPool returns the pool for JSON parsers.
func (o *Options) ParserPool() *json.ParserPool {
	return o.parserPool
}

// SetIDFn sets the ID function.
func (o *Options) SetIDFn(value IDFn) *Options {
	opts := *o
	opts.idFn = value
	return &opts
}

// IDFn returns the ID function.
func (o *Options) IDFn() IDFn {
	return o.idFn
}

// SetNamespaceFn sets the namespace function.
func (o *Options) SetNamespaceFn(value NamespaceFn) *Options {
	opts := *o
	opts.namespaceFn = value
	return &opts
}

// NamespaceFn returns the ID function.
func (o *Options) NamespaceFn() NamespaceFn {
	return o.namespaceFn
}

// SetTimeNanosFn sets the timestamp function.
func (o *Options) SetTimeNanosFn(value TimeNanosFn) *Options {
	opts := *o
	opts.timeNanosFn = value
	return &opts
}

// TimeNanosFn returns the timestamp function.
func (o *Options) TimeNanosFn() TimeNanosFn {
	return o.timeNanosFn
}

// defaultIDFn simply generates a UUID as the document ID.
func defaultIDFn(*value.Value) ([]byte, error) {
	id := uuid.NewUUID().String()
	return unsafe.ToBytes(id), nil
}

// defaultNamespaceFn parses the namespace value as a string.
func defaultNamespaceFn(v *value.Value) ([]byte, error) {
	ns, err := v.String()
	if err != nil {
		return nil, err
	}
	return unsafe.ToBytes(ns), nil
}

// defaultTimeNanosFn parses the time value as a string in RFC3339 format.
func defaultTimeNanosFn(v *value.Value) (int64, error) {
	str, err := v.String()
	if err != nil {
		return 0, err
	}
	t, err := time.Parse(time.RFC3339, str)
	if err != nil {
		return 0, err
	}
	return t.UnixNano(), nil
}

func (o *Options) initPools() {
	// Initialize JSON parser pool.
	pp := json.NewParserPool(nil)
	pp.Init(func() json.Parser { return json.NewParser(nil) })
	o.parserPool = pp
}
