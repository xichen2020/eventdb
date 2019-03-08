package json

import (
	"errors"
	"fmt"
	"strconv"
	"unicode"
	"unicode/utf16"
	"unicode/utf8"

	"github.com/xichen2020/eventdb/parser/json/value"
)

const (
	trueBytes  = "true"
	falseBytes = "false"
	nullBytes  = "null"
)

var (
	emptyObjectValue = value.NewObjectValue(value.Object{}, nil)
	emptyArrayValue  = value.NewArrayValue(value.Array{}, nil)
	nullValue        = value.NewNullValue(nil)
	trueValue        = value.NewBoolValue(true, nil)
	falseValue       = value.NewBoolValue(false, nil)
)

// Parser parses JSON-encoded values.
type Parser interface {
	// Parse parses a JSON-encoded value and returns the parse result.
	// The value returned remains valid till the next Parse or ParseBytes call.
	Parse(str string) (*value.Value, error)

	// ParseBytes parses a byte slice and returns the parse result.
	// The value returned remains valid till the next Parse or ParseBytes call.
	ParseBytes(b []byte) (*value.Value, error)
}

type cache struct {
	values      []value.Value
	valueArrays []value.Array
	kvArrays    []value.KVArray
}

func (c *cache) reset() {
	for i := 0; i < len(c.values); i++ {
		c.values[i].Reset()
	}
	c.values = c.values[:0]

	for i := 0; i < len(c.valueArrays); i++ {
		c.valueArrays[i].Reset()
	}
	c.valueArrays = c.valueArrays[:0]

	for i := 0; i < len(c.kvArrays); i++ {
		c.kvArrays[i].Reset()
	}
	c.kvArrays = c.kvArrays[:0]
}

func (c *cache) getValue() *value.Value {
	if cap(c.values) > len(c.values) {
		c.values = c.values[:len(c.values)+1]
	} else {
		c.values = append(c.values, value.Value{})
	}
	v := &c.values[len(c.values)-1]
	v.Reset()
	return v
}

func (c *cache) getValueArray() *value.Array {
	if cap(c.valueArrays) > len(c.valueArrays) {
		c.valueArrays = c.valueArrays[:len(c.valueArrays)+1]
	} else {
		c.valueArrays = append(c.valueArrays, value.NewArray(nil, nil))
	}
	v := &c.valueArrays[len(c.valueArrays)-1]
	v.Reset()
	return v
}

func (c *cache) getKVArray() *value.KVArray {
	if cap(c.kvArrays) > len(c.kvArrays) {
		c.kvArrays = c.kvArrays[:len(c.kvArrays)+1]
	} else {
		c.kvArrays = append(c.kvArrays, value.NewKVArray(nil, nil))
	}
	v := &c.kvArrays[len(c.kvArrays)-1]
	v.Reset()
	return v
}

// NB: Parser is not thread-safe.
type parser struct {
	maxDepth          int
	objectKeyFilterFn ObjectKeyFilterFn
	cache             cache

	str   string
	pos   int
	depth int
}

// NewParser creates a JSON parser.
func NewParser(opts *Options) Parser {
	if opts == nil {
		opts = NewOptions()
	}
	return &parser{
		maxDepth:          opts.MaxDepth(),
		objectKeyFilterFn: opts.ObjectKeyFilterFn(),
	}
}

func (p *parser) Parse(str string) (*value.Value, error) {
	p.reset()
	p.str = str
	v, err := p.parseValue()
	if err != nil {
		return nil, fmt.Errorf("cannot parse string %s: %v", str, err)
	}
	p.skipWS()
	if !p.eos() {
		return nil, fmt.Errorf("unexpected tail after parsing: %s", p.str[p.pos:])
	}
	return v, nil
}

func (p *parser) ParseBytes(b []byte) (*value.Value, error) {
	return p.Parse(string(b))
}

func (p *parser) reset() {
	p.cache.reset()
	p.str = ""
	p.pos = 0
	p.depth = 0
}

func (p *parser) parseValue() (*value.Value, error) {
	p.skipWS()
	if p.eos() {
		return nil, newParseError("value", p.pos, errors.New("cannot parse empty string"))
	}

	var (
		pos = p.pos
		v   *value.Value
		err error
	)

	switch p.current() {
	case '{':
		p.pos++
		v, err = p.parseObject()
		if err != nil {
			return nil, newParseError("object", pos, err)
		}
	case '[':
		p.pos++
		v, err = p.parseArray()
		if err != nil {
			return nil, newParseError("array", pos, err)
		}
	case '"':
		p.pos++
		v, err = p.parseBytes()
		if err != nil {
			return nil, newParseError("string", pos, err)
		}
	case 't': // true
		v, err = p.parseTrue()
		if err != nil {
			return nil, newParseError("true", pos, err)
		}
	case 'f': // false
		v, err = p.parseFalse()
		if err != nil {
			return nil, newParseError("false", pos, err)
		}
	case 'n': // null
		v, err = p.parseNull()
		if err != nil {
			return nil, newParseError("null", pos, err)
		}
	default:
		v, err = p.parseNumber()
		if err != nil {
			return nil, newParseError("number", pos, err)
		}
	}
	return v, nil
}

func (p *parser) parseObject() (*value.Value, error) {
	p.skipWS()
	if p.eos() {
		return nil, newParseError("object", p.pos, errors.New("missing }"))
	}

	if p.current() == '}' {
		p.pos++
		return emptyObjectValue, nil
	}

	if p.depth >= p.maxDepth {
		// Skip parsing values due to crossing maximum depth threshold.
		if err := p.skipObjectValue(); err != nil {
			return nil, err
		}
		// NB: If the parser successfully skips the object value without
		// encountering any errors, return an empty object value.
		return emptyObjectValue, nil
	}

	var kvs *value.KVArray
	for {
		p.skipWS()
		if p.eos() || p.current() != '"' {
			return nil, newParseError("object key", p.pos, errors.New(`missing opening " for object key`))
		}
		p.pos++

		// Parse out the key.
		k, err := p.parseBytesAsRaw()
		if err != nil {
			return nil, newParseError("object key", p.pos, err)
		}

		// Expect the key value separator.
		p.skipWS()
		if p.eos() || p.current() != ':' {
			return nil, newParseError("key value separator", p.pos, errors.New("missing : after object key"))
		}
		p.pos++

		// Parse out the value.
		p.skipWS()
		p.depth++
		v, err := p.parseValue()
		if err != nil {
			return nil, newParseError("object value", p.pos, err)
		}
		p.depth--

		// Consume the separator.
		p.skipWS()
		if p.eos() {
			return nil, newParseError("object separator", p.pos, errors.New("unexpected end of object"))
		}

		// If the object key matches the filter, exclude the key value pair from the parse result.
		if !(p.objectKeyFilterFn != nil && p.objectKeyFilterFn(string(k))) {
			if kvs == nil {
				kvs = p.cache.getKVArray()
			}
			kvs.Append(value.NewKV(string(k), v))
		}

		if p.current() == ',' {
			p.pos++
			continue
		}

		if p.current() == '}' {
			p.pos++
			v := p.cache.getValue()
			v.SetObject(value.NewObject(*kvs))
			return v, nil
		}

		return nil, newParseError("object separator", p.pos, errors.New("unexpected end of object"))
	}
}

// Precondition: The parser has seen a '{', and expects to see '}'.
func (p *parser) skipObjectValue() error {
	var (
		inBytes       bool
		numLeftParens = 1
		d             [20]byte // Temporary buffer to absorb escaped bytes
	)
	for !p.eos() {
		switch p.current() {
		case '\\':
			if inBytes {
				off, _, err := processEscape(p.str[p.pos:], d[:])
				if err != nil {
					return newParseError("escape string", p.pos, err)
				}
				p.pos += off
				continue
			} else {
				return newParseError("object", p.pos, errors.New("unexpected escape char"))
			}
		case '"':
			inBytes = !inBytes
		case '{':
			if !inBytes {
				numLeftParens++
			}
		case '}':
			if !inBytes {
				numLeftParens--
			}
			if numLeftParens == 0 {
				p.pos++
				return nil
			}
		}
		p.pos++
	}
	return newParseError("object", p.pos, errors.New("missing }"))
}

func (p *parser) parseArray() (*value.Value, error) {
	p.skipWS()
	if p.eos() {
		return nil, newParseError("array", p.pos, nil)
	}

	// NB: possibly reusing objects for empty object values.
	if p.current() == ']' {
		p.pos++
		return emptyArrayValue, nil
	}

	var values *value.Array
	for {
		p.skipWS()
		v, err := p.parseValue()
		if err != nil {
			return nil, newParseError("value", p.pos, err)
		}

		// Consume the separator.
		p.skipWS()
		if p.eos() {
			return nil, newParseError("value", p.pos, errors.New("unexpected end of array"))
		}

		if values == nil {
			values = p.cache.getValueArray()
		}
		values.Append(v)

		if p.current() == ',' {
			p.pos++
			continue
		}

		if p.current() == ']' {
			p.pos++
			v := p.cache.getValue()
			v.SetArray(*values)
			return v, nil
		}

		return nil, newParseError("value", p.pos, errors.New("unexpected end of array"))
	}
}

func (p *parser) parseBytes() (*value.Value, error) {
	b, err := p.parseBytesAsRaw()
	if err != nil {
		return nil, err
	}
	v := p.cache.getValue()
	v.SetBytes(b)
	return v, nil
}

func (p *parser) parseBytesAsRaw() ([]byte, error) {
	data := p.str[p.pos:]
	isValid, hasEscapes, length := findStringLen(data)
	if !isValid {
		return nil, newParseError("string", p.pos, errors.New("unterminated string literal"))
	}
	if !hasEscapes {
		str := data[:length]
		p.pos += length + 1
		return []byte(str), nil
	}

	// NB: the current logic escapes the string upfront as opposed to
	// doing it lazily. This reduces the code complexity at the cost
	// of extra allocations when escaped sequences are encountered.
	// NB: There isn't a good way to pool the bytes here, but in the
	// meantime it'll only require an allocation when the string contains
	// an escape character which presumably is not very common at least for the PoC.
	var (
		escapedBytes = make([]byte, 0, length)
		prev         = 0
	)

	for i := 0; i < len(data); {
		switch data[i] {
		case '"':
			p.pos += i + 1
			escapedBytes = append(escapedBytes, data[prev:i]...)
			return escapedBytes, nil

		case '\\':
			escapedBytes = append(escapedBytes, data[prev:i]...)
			var (
				off int
				err error
			)
			off, escapedBytes, err = processEscape(data[i:], escapedBytes)
			if err != nil {
				return nil, newParseError("string", p.pos, err)
			}
			i += off
			prev = i

		default:
			i++
		}
	}

	return nil, newParseError("string", p.pos, errors.New("string not terminated"))
}

// Adapted from https://github.com/mailru/easyjson/blob/master/jlexer/lexer.go#L208.
func (p *parser) parseNumber() (*value.Value, error) {
	var (
		hasE   bool
		afterE bool
		hasDot bool
		start  = p.pos
	)

	c := p.current()
	if !(c == '-' || (c >= '0' && c <= '9')) {
		return nil, newParseError("number", start, errors.New("invalid number start"))
	}

	p.pos++

outerLoop:
	for ; !p.eos(); p.pos++ {
		c := p.current()
		switch {
		case c >= '0' && c <= '9':
			afterE = false
		case c == '.' && !hasDot:
			hasDot = true
		case (c == 'e' || c == 'E') && !hasE:
			hasE = true
			hasDot = true
			afterE = true
		case (c == '+' || c == '-') && afterE:
			afterE = false
		default:
			if !isTokenEnd(c) {
				return nil, newParseError("number", start, nil)
			}
			break outerLoop
		}
	}

	n, err := parseFloat64(p.str[start:p.pos])
	if err != nil {
		return nil, newParseError("number", start, err)
	}
	v := p.cache.getValue()
	v.SetNumber(n)
	return v, nil
}

func (p *parser) parseTrue() (*value.Value, error) {
	if err := p.expect(trueBytes); err != nil {
		return nil, err
	}
	return trueValue, nil
}

func (p *parser) parseFalse() (*value.Value, error) {
	if err := p.expect(falseBytes); err != nil {
		return nil, err
	}
	return falseValue, nil
}

func (p *parser) parseNull() (*value.Value, error) {
	if err := p.expect(nullBytes); err != nil {
		return nil, err
	}
	return nullValue, nil
}

func (p *parser) expect(pat string) error {
	end := p.pos + len(pat)
	if len(p.str) < end || p.str[p.pos:end] != pat {
		return fmt.Errorf("unexpected value %s", p.str[p.pos:])
	}
	p.pos = end
	return nil
}

func (p *parser) skipWS() {
	// Fast path.
	if p.eos() || p.str[p.pos] > ' ' {
		return
	}
	for ; !p.eos(); p.pos++ {
		c := p.str[p.pos]
		if c != ' ' && c != '\t' && c != '\r' && c != '\n' {
			return
		}
	}
}

func (p *parser) eos() bool     { return p.pos >= len(p.str) }
func (p *parser) current() byte { return p.str[p.pos] }

// findStringLen tries to scan into the string literal for ending quote char to
// determine required size. The size will be exact if no escapes are present and
// may be inexact if there are escaped chars.
func findStringLen(data string) (isValid, hasEscapes bool, length int) {
	delta := 0

	for i := 0; i < len(data); i++ {
		switch data[i] {
		case '\\':
			i++
			delta++
			if i < len(data) && data[i] == 'u' {
				delta++
			}
		case '"':
			return true, (delta > 0), (i - delta)
		}
	}

	return false, false, len(data)
}

// processEscape processes a single escape sequence and returns number of bytes processed.
func processEscape(data string, res []byte) (int, []byte, error) {
	if len(data) < 2 {
		return 0, nil, fmt.Errorf("syntax error at %v", data)
	}

	c := data[1]
	switch c {
	case '"', '/', '\\':
		res = append(res, c)
		return 2, res, nil
	case 'b':
		res = append(res, '\b')
		return 2, res, nil
	case 'f':
		res = append(res, '\f')
		return 2, res, nil
	case 'n':
		res = append(res, '\n')
		return 2, res, nil
	case 'r':
		res = append(res, '\r')
		return 2, res, nil
	case 't':
		res = append(res, '\t')
		return 2, res, nil
	case 'u':
		rr := getu4(data)
		if rr < 0 {
			return 0, nil, errors.New("syntax error")
		}

		read := 6
		if utf16.IsSurrogate(rr) {
			rr1 := getu4(data[read:])
			if dec := utf16.DecodeRune(rr, rr1); dec != unicode.ReplacementChar {
				read += 6
				rr = dec
			} else {
				rr = unicode.ReplacementChar
			}
		}
		var d [4]byte
		s := utf8.EncodeRune(d[:], rr)
		res = append(res, d[:s]...)
		return read, res, nil
	}

	return 0, nil, errors.New("syntax error")
}

// getu4 decodes \uXXXX from the beginning of s, returning the hex value,
// or it returns -1.
func getu4(s string) rune {
	if len(s) < 6 || s[0] != '\\' || s[1] != 'u' {
		return -1
	}
	var val rune
	for i := 2; i < len(s) && i < 6; i++ {
		var v byte
		c := s[i]
		switch c {
		case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
			v = c - '0'
		case 'a', 'b', 'c', 'd', 'e', 'f':
			v = c - 'a' + 10
		case 'A', 'B', 'C', 'D', 'E', 'F':
			v = c - 'A' + 10
		default:
			return -1
		}

		val <<= 4
		val |= rune(v)
	}
	return val
}

func isTokenEnd(c byte) bool {
	return c == ' ' ||
		c == '\t' ||
		c == '\r' ||
		c == '\n' ||
		c == '[' ||
		c == ']' ||
		c == '{' ||
		c == '}' ||
		c == ',' ||
		c == ':'
}

func parseFloat64(s string) (float64, error) {
	return strconv.ParseFloat(s, 64)
}

func newParseError(
	desiredTokenType string,
	pos int,
	err error,
) error {
	if err == nil {
		return fmt.Errorf("cannot parse %s at position %d", desiredTokenType, pos)
	}
	return fmt.Errorf("cannot parse %s at position %d: %v", desiredTokenType, pos, err)
}
