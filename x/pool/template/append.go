package template

import "github.com/mauricelam/genny/generic"

// GenericBucketizedValueArrayPool is a generic bucketized value array pool.
type GenericBucketizedValueArrayPool interface {
	generic.Type

	Get(capacity int) []GenericValue
	Put(values []GenericValue, capacity int)
}

// AppendValue appends an item to an item collection getting a new array from the
// pool if the array is at capacity.
func AppendValue(arr []GenericValue, v GenericValue, pool GenericBucketizedValueArrayPool) []GenericValue {
	if len(arr) == cap(arr) {
		newArray := pool.Get(cap(arr) * 2)
		n := copy(newArray[:len(arr)], arr)
		pool.Put(arr, cap(arr))
		arr = newArray[:n]
	}

	return append(arr, v)
}
