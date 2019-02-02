package impl

import "github.com/xichen2020/eventdb/values/iterator"

var (
	emptyPositionIter iterator.PositionIterator = emptyPositionIterator{}
)

// emptyPositionIterator is a position iterator for an empty sequence of positions.
type emptyPositionIterator struct{}

// NewEmptyPositionIterator creates an empty selection iterator.
func NewEmptyPositionIterator() iterator.PositionIterator { return emptyPositionIter }

func (it emptyPositionIterator) Next() bool    { return false }
func (it emptyPositionIterator) Position() int { return -1 }
func (it emptyPositionIterator) Err() error    { return nil }
