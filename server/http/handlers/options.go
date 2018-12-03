package handlers

import (
	"github.com/xichen2020/eventdb/parser/json"
	"github.com/xichen2020/eventdb/parser/json/value"
)

// IDFn determines the ID of a JSON event.
type IDFn func(value *value.Value) ([]byte, error)

// NamespaceFn determines the namespace a JSON event belongs to.
type NamespaceFn func(value *value.Value) ([]byte, error)

// TimeNanosFn determines the timestamp of a JSON event in nanoseconds.
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
	o := &Options{}
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

func (o *Options) initPools() {
	// Initialize JSON parser pool.
	pp := json.NewParserPool(nil)
	pp.Init(func() json.Parser { return json.NewParser(nil) })
	o.parserPool = pp
}
