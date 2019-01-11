package template

import (
	"github.com/mauricelam/genny/generic"
	"github.com/xichen2020/eventdb/values/iterator"
)

// GenericValue is a generic type.
type GenericValue generic.Type

// ForwardValueIterator allows iterating over a stream of GenericValue.
type ForwardValueIterator interface {
	generic.Type

	Next() bool
	Err() error
	Current() GenericValue
	Close()
}

// ValueFilter performs filtering against values.
type ValueFilter interface {
	generic.Type

	Match(v GenericValue) bool
}

// BoolValueCollection is a bool value collection.
type BoolValueCollection interface {
	generic.Type

	Iter() (iterator.ForwardBoolIterator, error)
}

// IntValueCollection is a int value collection.
type IntValueCollection interface {
	generic.Type

	Iter() (iterator.ForwardIntIterator, error)
}

// DoubleValueCollection is a double value collection.
type DoubleValueCollection interface {
	generic.Type

	Iter() (iterator.ForwardDoubleIterator, error)
}

// StringValueCollection is a string value collection.
type StringValueCollection interface {
	generic.Type

	Iter() (iterator.ForwardStringIterator, error)
}

// TimeValueCollection is a time value collection.
type TimeValueCollection interface {
	generic.Type

	Iter() (iterator.ForwardTimeIterator, error)
}
