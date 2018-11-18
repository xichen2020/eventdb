package value

import "strconv"

// Object is an object containing a list of key value pairs.
type Object struct {
	kvs []kv
}

// NewObject creates a new object.
func NewObject(kvs []kv) Object { return Object{kvs: kvs} }

// MarshalTo appends marshaled o to dst and returns the result.
func (o *Object) MarshalTo(dst []byte) ([]byte, error) {
	var err error
	dst = append(dst, '{')
	for i, kv := range o.kvs {
		dst = strconv.AppendQuote(dst, kv.k)
		dst = append(dst, ':')
		dst, err = kv.v.MarshalTo(dst)
		if err != nil {
			return nil, err
		}
		if i != len(o.kvs)-1 {
			dst = append(dst, ',')
		}
	}
	dst = append(dst, '}')
	return dst, nil
}

// Len returns the number of items in the object.
func (o *Object) Len() int { return len(o.kvs) }

// Get returns the value for the given key in the o, and a bool indicating
// whether the value is found.
func (o *Object) Get(key string) (*Value, bool) {
	for _, kv := range o.kvs {
		if kv.k == key {
			return kv.v, true
		}
	}
	return nil, false
}

// Visit calls f for each item in the object in the original order.
//
// f cannot hold key and/or v after returning.
func (o Object) Visit(f func(key string, v *Value)) {
	for _, kv := range o.kvs {
		f(kv.k, kv.v)
	}
}

type kv struct {
	k string
	v *Value
}
