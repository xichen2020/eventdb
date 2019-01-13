package template

import (
	"github.com/xichen2020/eventdb/document/field"

	"github.com/mauricelam/genny/generic"
)

// GenericValue is a generic type.
type GenericValue generic.Type

// valueAsUnionFn converts a value to a value union.
type valueAsUnionFn func(v GenericValue) field.ValueUnion

// ForwardValueIterator allows iterating over a stream of GenericValue.
type ForwardValueIterator interface {
	generic.Type

	Next() bool
	Err() error
	Current() GenericValue
	Close()
}

// SeekableValueIterator allows iterating and seeking over a stream of GenericValue.
type SeekableValueIterator interface {
	ForwardValueIterator

	SeekForward(n int) error
}
