package json

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/xichen2020/eventdb/value"
	"github.com/xichen2020/eventdb/x/unsafe"

	"github.com/stretchr/testify/require"
)

func BenchmarkParseRawString(b *testing.B) {
	for _, s := range []string{`""`, `"a"`, `"abcd"`, `"abcdefghijk"`, `"qwertyuiopasdfghjklzxcvb"`} {
		b.Run(s, func(b *testing.B) {
			benchmarkParseRawString(b, s)
		})
	}
}

func benchmarkParseRawString(b *testing.B, s string) {
	b.ReportAllocs()
	b.SetBytes(int64(len(s)))
	b.RunParallel(func(pb *testing.PB) {
		p := testParserPool.Get()
		for pb.Next() {
			rs, err := p.Parse(s)
			require.NoError(b, err)
			require.Equal(b, s[1:len(s)-1], rs.MustString())
		}
		testParserPool.Put(p)
	})
}

func BenchmarkParseRawNumber(b *testing.B) {
	for _, s := range []string{"1", "1234", "123456", "-1234", "1234567890.1234567", "-1.32434e+12"} {
		b.Run(s, func(b *testing.B) {
			benchmarkParseRawNumber(b, s)
		})
	}
}

func benchmarkParseRawNumber(b *testing.B, s string) {
	b.ReportAllocs()
	b.SetBytes(int64(len(s)))
	b.RunParallel(func(pb *testing.PB) {
		p := testParserPool.Get()
		for pb.Next() {
			_, err := p.Parse(s)
			require.NoError(b, err)
		}
		testParserPool.Put(p)
	})
}

func BenchmarkObjectGet(b *testing.B) {
	for _, itemsCount := range []int{10, 100, 1000, 10000, 100000} {
		b.Run(fmt.Sprintf("items_%d", itemsCount), func(b *testing.B) {
			for _, lookupsCount := range []int{0, 1, 2, 4, 8, 16, 32, 64} {
				b.Run(fmt.Sprintf("lookups_%d", lookupsCount), func(b *testing.B) {
					benchmarkObjectGet(b, itemsCount, lookupsCount)
				})
			}
		})
	}
}

func benchmarkObjectGet(b *testing.B, itemsCount, lookupsCount int) {
	b.StopTimer()
	var ss []string
	for i := 0; i < itemsCount; i++ {
		s := fmt.Sprintf(`"key_%d": "value_%d"`, i, i)
		ss = append(ss, s)
	}
	s := "{" + strings.Join(ss, ",") + "}"
	key := fmt.Sprintf("key_%d", len(ss)/2)
	expectedValue := fmt.Sprintf("value_%d", len(ss)/2)
	b.StartTimer()
	b.ReportAllocs()
	b.SetBytes(int64(len(s)))

	b.RunParallel(func(pb *testing.PB) {
		p := testParserPool.Get()
		for pb.Next() {
			v, err := p.Parse(s)
			require.NoError(b, err)
			o := v.MustObject()
			for i := 0; i < lookupsCount; i++ {
				str, found := o.Get(key)
				require.True(b, found)
				sb := str.MustString()
				require.Equal(b, expectedValue, sb)
			}
		}
		testParserPool.Put(p)
	})
}

func BenchmarkMarshalTo(b *testing.B) {
	b.Run("small", func(b *testing.B) {
		benchmarkMarshalTo(b, smallFixture)
	})
	b.Run("medium", func(b *testing.B) {
		benchmarkMarshalTo(b, mediumFixture)
	})
	b.Run("large", func(b *testing.B) {
		benchmarkMarshalTo(b, largeFixture)
	})
	b.Run("canada", func(b *testing.B) {
		benchmarkMarshalTo(b, canadaFixture)
	})
	b.Run("citm", func(b *testing.B) {
		benchmarkMarshalTo(b, citmFixture)
	})
	b.Run("twitter", func(b *testing.B) {
		benchmarkMarshalTo(b, twitterFixture)
	})
}

func benchmarkMarshalTo(b *testing.B, s string) {
	p := testParserPool.Get()
	v, err := p.Parse(s)
	if err != nil {
		panic(fmt.Errorf("unexpected parse error: %s", err))
	}
	testParserPool.Put(p)

	b.ReportAllocs()
	b.SetBytes(int64(len(s)))
	b.RunParallel(func(pb *testing.PB) {
		p := testParserPool.Get()
		var b []byte
		for pb.Next() {
			// It is ok calling v.MarshalTo from concurrent
			// goroutines, since MarshalTo doesn't modify v.
			var err error
			b, err = v.MarshalTo(b[:0])
			if err != nil {
				panic(fmt.Errorf("unexpected marshal error: %s", err))
			}
		}
		testParserPool.Put(p)
	})
}

func BenchmarkParse(b *testing.B) {
	b.Run("small", func(b *testing.B) {
		benchmarkParse(b, smallFixture)
	})
	b.Run("medium", func(b *testing.B) {
		benchmarkParse(b, mediumFixture)
	})
	b.Run("large", func(b *testing.B) {
		benchmarkParse(b, largeFixture)
	})
	b.Run("canada", func(b *testing.B) {
		benchmarkParse(b, canadaFixture)
	})
	b.Run("citm", func(b *testing.B) {
		benchmarkParse(b, citmFixture)
	})
	b.Run("twitter", func(b *testing.B) {
		benchmarkParse(b, twitterFixture)
	})
}

func benchmarkParse(b *testing.B, s string) {
	b.Run("stdjson-map", func(b *testing.B) {
		benchmarkStdJSONParseMap(b, s)
	})
	b.Run("stdjson-struct", func(b *testing.B) {
		benchmarkStdJSONParseStruct(b, s)
	})
	b.Run("stdjson-empty-struct", func(b *testing.B) {
		benchmarkStdJSONParseEmptyStruct(b, s)
	})
	b.Run("fastjson", func(b *testing.B) {
		benchmarkFastJSONParse(b, s, testParserPool)
	})
	b.Run("fastjson-max-depth", func(b *testing.B) {
		benchmarkFastJSONParse(b, s, testParserWithMaxDepthPool)
	})
	b.Run("fastjson-get", func(b *testing.B) {
		benchmarkFastJSONParseGet(b, s)
	})
}

func benchmarkFastJSONParse(b *testing.B, s string, parsePool *ParserPool) {
	b.ReportAllocs()
	b.SetBytes(int64(len(s)))
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		p := parsePool.Get()
		for pb.Next() {
			v, err := p.Parse(s)
			if err != nil {
				panic(fmt.Errorf("unexpected error: %s", err))
			}
			if v.Type() != value.ObjectType {
				panic(fmt.Errorf("unexpected value type; got %s; want %s", v.Type(), value.ObjectType))
			}
		}
		parsePool.Put(p)
	})
}

func benchmarkFastJSONParseGet(b *testing.B, s string) {
	b.ReportAllocs()
	b.SetBytes(int64(len(s)))
	b.RunParallel(func(pb *testing.PB) {
		p := testParserPool.Get()
		var n int
		for pb.Next() {
			v, err := p.Parse(s)
			if err != nil {
				panic(fmt.Errorf("unexpected error: %s", err))
			}
			nv, found := v.Get("sid")
			if found && nv.Type() == value.NumberType {
				n += int(nv.MustNumber())
			}
			sv, found := v.Get("uuid")
			if found && sv.Type() == value.StringType {
				n += len(sv.MustString())
			}
			p, _ := v.Get("person")
			if p != nil {
				n++
			}
			c, _ := v.Get("company")
			if c != nil {
				n++
			}
			u, _ := v.Get("users")
			if u != nil {
				n++
			}
			av, found := v.Get("features")
			if found && av.Type() == value.ArrayType {
				n += av.MustArray().Len()
			}
			av, found = v.Get("topicSubTopics")
			if found && av.Type() == value.ArrayType {
				n += av.MustArray().Len()
			}
			o, _ := v.Get("search_metadata")
			if o != nil {
				n++
			}
		}
		testParserPool.Put(p)
	})
}

func benchmarkStdJSONParseMap(b *testing.B, s string) {
	b.ReportAllocs()
	b.SetBytes(int64(len(s)))
	bb := unsafe.ToBytes(s)
	b.RunParallel(func(pb *testing.PB) {
		var m map[string]interface{}
		for pb.Next() {
			if err := json.Unmarshal(bb, &m); err != nil {
				panic(fmt.Errorf("unexpected error: %s", err))
			}
		}
	})
}

func benchmarkStdJSONParseStruct(b *testing.B, s string) {
	b.ReportAllocs()
	b.SetBytes(int64(len(s)))
	bb := unsafe.ToBytes(s)
	b.RunParallel(func(pb *testing.PB) {
		var m struct {
			Sid            int
			UUID           string
			Person         map[string]interface{}
			Company        map[string]interface{}
			Users          []interface{}
			Features       []map[string]interface{}
			TopicSubTopics map[string]interface{}
			SearchMetadata map[string]interface{}
		}
		for pb.Next() {
			if err := json.Unmarshal(bb, &m); err != nil {
				panic(fmt.Errorf("unexpected error: %s", err))
			}
		}
	})
}

func benchmarkStdJSONParseEmptyStruct(b *testing.B, s string) {
	b.ReportAllocs()
	b.SetBytes(int64(len(s)))
	bb := unsafe.ToBytes(s)
	b.RunParallel(func(pb *testing.PB) {
		var m struct{}
		for pb.Next() {
			if err := json.Unmarshal(bb, &m); err != nil {
				panic(fmt.Errorf("unexpected error: %s", err))
			}
		}
	})
}

const (
	testDefaultMaxDepth = 1
)

var (
	testParserPool             *ParserPool
	testParserWithMaxDepthPool *ParserPool
)

func init() {
	opts := NewParserPoolOptions().SetSize(32)
	testParserPool = NewParserPool(opts)
	testParserPool.Init(func() Parser { return NewParser(nil) })

	parserOpts := NewOptions().SetMaxDepth(testDefaultMaxDepth)
	testParserWithMaxDepthPool = NewParserPool(opts)
	testParserWithMaxDepthPool.Init(func() Parser { return NewParser(parserOpts) })
}
