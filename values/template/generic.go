package template

import (
	"io"

	"github.com/mauricelam/genny/generic"
)

// GenericValue is a generic type.
type GenericValue generic.Type

// ForwardValueIterator allows iterating over a stream of GenericValue.
type ForwardValueIterator interface {
	generic.Type
	io.Closer
	Next() bool
	Err() error
	Current() GenericValue
}
