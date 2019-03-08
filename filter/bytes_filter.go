package filter

import (
	"bytes"

	"github.com/xichen2020/eventdb/values/iterator"
	xbytes "github.com/xichen2020/eventdb/x/bytes"
)

// BytesFilter matches against bytes values.
type BytesFilter interface {
	// Match returns true if the given value is considered a match.
	Match(v iterator.Bytes) bool
}

type bytesToBytesFilterFn func([]byte) bytesFilterFn

type bytesFilterFn func(v []byte) bool

func (fn bytesFilterFn) Match(v iterator.Bytes) bool { return fn(v.Data) }

func equalsBytesBytes(rhs []byte) bytesFilterFn {
	return func(lhs []byte) bool {
		return bytes.Equal(lhs, rhs)
	}
}

func notEqualsBytesBytes(rhs []byte) bytesFilterFn {
	return func(lhs []byte) bool {
		return !bytes.Equal(lhs, rhs)
	}
}

func largerThanBytesBytes(rhs []byte) bytesFilterFn {
	return func(lhs []byte) bool {
		return xbytes.GreaterThan(lhs, rhs)
	}
}

func largerThanOrEqualBytesBytes(rhs []byte) bytesFilterFn {
	return func(lhs []byte) bool {
		return xbytes.GreaterThanOrEqual(lhs, rhs)
	}
}

func smallerThanBytesBytes(rhs []byte) bytesFilterFn {
	return func(lhs []byte) bool {
		return xbytes.LessThan(lhs, rhs)
	}
}

func smallerThanOrEqualBytesBytes(rhs []byte) bytesFilterFn {
	return func(lhs []byte) bool {
		return xbytes.LessThanOrEqual(lhs, rhs)
	}
}

func startsWithBytesBytes(rhs []byte) bytesFilterFn {
	return func(lhs []byte) bool {
		return bytes.HasPrefix(lhs, rhs)
	}
}

func doesNotStartWithBytesBytes(rhs []byte) bytesFilterFn {
	return func(lhs []byte) bool {
		return !bytes.HasPrefix(lhs, rhs)
	}
}

func endsWithBytesBytes(rhs []byte) bytesFilterFn {
	return func(lhs []byte) bool {
		return bytes.HasSuffix(lhs, rhs)
	}
}

func doesNotEndWithBytesBytes(rhs []byte) bytesFilterFn {
	return func(lhs []byte) bool {
		return !bytes.HasSuffix(lhs, rhs)
	}
}

func containsBytesBytes(rhs []byte) bytesFilterFn {
	return func(lhs []byte) bool {
		return bytes.Contains(lhs, rhs)
	}
}

func doesNotContainBytesBytes(rhs []byte) bytesFilterFn {
	return func(lhs []byte) bool {
		return !bytes.Contains(lhs, rhs)
	}
}
