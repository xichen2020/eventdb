package value

import "strconv"

// Object is an object containing a list of key value pairs.
type Object struct {
	kvs KVArray
}

// NewObject creates a new object.
func NewObject(kvs KVArray) Object { return Object{kvs: kvs} }

// MarshalTo appends marshaled o to dst and returns the result.
func (o *Object) MarshalTo(dst []byte) ([]byte, error) {
	var err error
	dst = append(dst, '{')
	for i, kv := range o.kvs.raw {
		dst = strconv.AppendQuote(dst, kv.k)
		dst = append(dst, ':')
		dst, err = kv.v.MarshalTo(dst)
		if err != nil {
			return nil, err
		}
		if i != len(o.kvs.raw)-1 {
			dst = append(dst, ',')
		}
	}
	dst = append(dst, '}')
	return dst, nil
}

// Len returns the number of items in the object.
func (o *Object) Len() int { return len(o.kvs.raw) }

// Get returns the value for the given key in the o, and a bool indicating
// whether the value is found.
func (o *Object) Get(key string) (*Value, bool) {
	for _, kv := range o.kvs.raw {
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
	for _, kv := range o.kvs.raw {
		f(kv.k, kv.v)
	}
}

// Close closes the object.
func (o *Object) Close() {
	o.kvs.Close()
	o.kvs = KVArray{}
}

// KV is a key value pair.
type KV struct {
	k string
	v *Value
}

// KVArray is an array of key value pairs.
type KVArray struct {
	raw []KV
	p   *KVArrayPool
}

// NewKVArray creates a new KV array.
func NewKVArray(raw []KV, p *KVArrayPool) KVArray {
	return KVArray{raw: raw, p: p}
}

// Reset reset the kv array.
func (a *KVArray) Reset() { a.raw = a.raw[:0] }

// Close closes the kv array.
func (a KVArray) Close() {
	for i := range a.raw {
		a.raw[i].v.Close()
		a.raw[i].v = nil
	}
	if a.p != nil {
		a.p.Put(a)
	}
}
