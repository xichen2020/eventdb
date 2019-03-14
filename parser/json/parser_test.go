package json

import (
	"fmt"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xichen2020/eventdb/parser/json/value"
)

func TestParseError(t *testing.T) {
	p := NewParser(NewOptions())
	parseErrorTests := []struct {
		name   string
		in     string
		errstr string
	}{
		{"invalid string escape", `"fo\u"`, "syntax error"},
		{"invalid string escape", `"foo\ubarz2134"`, "syntax error"},
		{"invalid number", "123+456", "cannot parse number"},
		{"invalid number", "123.456.789", "cannot parse number"},
		{"empty json", "", "cannot parse empty string"},
		{"empty json", "\n\t    \n", "cannot parse empty string"},
		{"invalid tail", "123 456", "unexpected tail after parsing"},
		{"invalid tail", "[] 1223", "unexpected tail after parsing"},
	}
	for _, tt := range parseErrorTests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := p.Parse(tt.in)
			require.Error(t, err)
			require.True(t, strings.Contains(err.Error(), tt.errstr))
		})
	}
}

func TestParserParseInvalidJSON(t *testing.T) {
	p := NewParser(NewOptions())
	inputs := []string{
		"free",
		"tree",
		"\x00\x10123",
		"1 \n\x01",
		"{\x00}",
		"[\x00]",
		"\"foo\"\x00",
		"{\"foo\"\x00:123}",
		"nil",
		"[foo]",
		"{foo}",
		"[123 34]",
		`{"foo" "bar"}`,
		`{"foo":123 "bar":"baz"}`,
		"-2134.453eec+43",
		`{"foo: 123}`,
		`"{\"foo\": 123}`,
	}
	for _, input := range inputs {
		_, err := p.Parse(input)
		require.Error(t, err)
	}
}

func TestParserParseEmptyObject(t *testing.T) {
	p := NewParser(NewOptions())
	v, err := p.Parse("{}")
	require.NoError(t, err)
	o := v.MustObject()
	require.Equal(t, 0, o.Len())
}

func TestParserParseOneElemObject(t *testing.T) {
	p := NewParser(NewOptions())
	v, err := p.Parse(`  {
		"foo"   : "bar"  }	 `)
	require.NoError(t, err)
	o := v.MustObject()
	val, found := o.Get("foo")
	require.True(t, found)
	require.Equal(t, []byte("bar"), val.MustBytes())
	_, found = o.Get("non-existent-key")
	require.False(t, found)
}

func TestParserParseTwoElemObject(t *testing.T) {
	p := NewParser(NewOptions())
	_, err := p.Parse(`{"foo":null,"bar":"baz"}`)
	require.NoError(t, err)
}

func TestParserParseMultiElemObject(t *testing.T) {
	p := NewParser(NewOptions())
	v, err := p.Parse(`{"foo": [1,2,3  ]  ,"bar":{},"baz":123.456}`)
	require.NoError(t, err)
	o := v.MustObject()
	val, found := o.Get("foo")
	require.True(t, found)
	require.Equal(t, value.ArrayType, val.Type())
	val, found = o.Get("bar")
	require.True(t, found)
	require.Equal(t, value.ObjectType, val.Type())
	val, found = o.Get("baz")
	require.True(t, found)
	require.Equal(t, value.NumberType, val.Type())
	require.Equal(t, `{"foo":[1,2,3],"bar":{},"baz":123.456}`, testMarshalled(t, v))
}

func TestParserParseComplexObjectKey(t *testing.T) {
	p := NewParser(NewOptions())
	str := `{"你好":1, "\\\"世界\"":2, "\\\"\u1234x":"\\f大家\\\\"}`
	v, err := p.Parse(str)
	require.NoError(t, err)
	n, found := v.Get("你好")
	require.True(t, found)
	require.Equal(t, float64(1), n.MustNumber())
	n, found = v.Get(`\"世界"`)
	require.True(t, found)
	require.Equal(t, float64(2), n.MustNumber())
	s, found := v.Get("\\\"\u1234x")
	require.True(t, found)
	require.Equal(t, []byte(`\f大家\\`), s.MustBytes())
}

func TestParserParseComplexObject(t *testing.T) {
	p := NewParser(NewOptions())
	inputs := []string{
		`{"foo":[-1.345678,[[[[[]]]],{}],"bar"],"baz":{"bbb":123}}`,
		strings.TrimSpace(largeFixture),
	}

	for _, input := range inputs {
		v, err := p.Parse(input)
		require.NoError(t, err)
		require.Equal(t, value.ObjectType, v.Type())
		require.Equal(t, input, testMarshalled(t, v))
	}
}

func TestParserParseLargeObject(t *testing.T) {
	const itemsCount = 10000

	// Build big json object.
	var ss []string
	for i := 0; i < itemsCount; i++ {
		s := fmt.Sprintf(`"key_%d": "value_%d"`, i, i)
		ss = append(ss, s)
	}
	s := "{" + strings.Join(ss, ",") + "}"

	// Parse it.
	p := NewParser(NewOptions())
	v, err := p.Parse(s)
	require.NoError(t, err)

	// Look up object items.
	for i := 0; i < itemsCount; i++ {
		k := fmt.Sprintf("key_%d", i)
		expectedV := fmt.Sprintf("value_%d", i)
		sb, found := v.Get(k)
		require.True(t, found)
		require.Equal(t, []byte(expectedV), sb.MustBytes())
	}

	// Verify non-existing key returns false.
	_, found := v.Get("non-existing-key")
	require.False(t, found)
}

func TestParserParseIncompleteObject(t *testing.T) {
	p := NewParser(NewOptions())
	inputs := []string{
		" {  ",
		`{"foo"`,
		`{"foo":`,
		`{"foo":null`,
		`{"foo":null,`,
		`{"foo":null,}`,
		`{"foo":null,"bar"}`,
	}
	for _, input := range inputs {
		_, err := p.Parse(input)
		require.Error(t, err)
	}
}

func TestParseObjectKeyFilterFn(t *testing.T) {
	input := `
	{
		"foo": 123,
		"bar": [
			{
				"baz": {
					"cat": 456,
					"car": 789
				},
				"dar": ["bbb"]
			},
			666
		],
		"rad": ["usa"],
		"pat": {
			"qat": {
				"xw": {
					"woei": "oiwers",
					"234": "sdflk"
				},
				"bw": 123
			},
			"tab": {
				"enter": "return"
			},
			"bzr": 123
		}
	}
`
	filterFn := func(key string) bool { return key == "cat" || key == "qat" }
	expected := `{"foo":123,"bar":[{"baz":{"car":789},"dar":["bbb"]},666],"rad":["usa"],"pat":{"tab":{"enter":"return"},"bzr":123}}`
	opts := NewOptions().SetObjectKeyFilterFn(filterFn)
	p := NewParser(opts)
	v, err := p.Parse(input)
	require.NoError(t, err)
	require.Equal(t, expected, testMarshalled(t, v))
}

func TestParserSkipObjectValue(t *testing.T) {
	inputs := []struct {
		str string
		pos int
	}{
		{str: `"foo": "bar"}`, pos: 13},
		{str: `"foo": "bar", "baz": 123}`, pos: 25},
		{str: `"foo": "ba}r", "baz": 123}`, pos: 26},
		{str: `"foo": "ba\"}r", "baz": 123}`, pos: 28},
		{str: `"foo": "ba\\"}, "baz": 123}`, pos: 14},
		{str: `"foo": ["bar"], "baz": 123}`, pos: 27},
		{str: `"foo": {"bar": [{"baz": {"cat\"}": 123}}], "da": "ca}r"}}`, pos: 57},
	}

	for _, input := range inputs {
		p := NewParser(NewOptions()).(*parser)
		p.str = input.str
		require.NoError(t, p.skipObjectValue())
		require.Equal(t, input.pos, p.pos)
		require.Equal(t, 0, p.depth)
	}
}

func TestParserSkipObjectValueError(t *testing.T) {
	inputs := []string{
		`"foo": "bar"`,
		`"foo\}": "bar"`,
		`"foo": "bar}"`,
		`"foo": \"bar"}`,
	}

	for _, input := range inputs {
		p := NewParser(NewOptions()).(*parser)
		p.str = input
		require.Error(t, p.skipObjectValue())
	}
}

func TestParseMaximumDepth(t *testing.T) {
	input := `
	{
		"foo": 123,
		"bar": [
			{
				"baz": {
					"cat": 456,
					"car": 789
				},
				"dar": ["bbb"]
			},
			666
		],
		"rad": ["usa"],
		"pat": {
			"qat": {
				"xw": {
					"woei": "oiwers",
					"234": "sdflk"
				},
				"bw": 123
			},
			"tab": {
				"enter": "return"
			},
			"bzr": 123
		}
	}
`

	expected := []string{
		`{}`,
		`{"foo":123,"bar":[{},666],"rad":["usa"],"pat":{}}`,
		`{"foo":123,"bar":[{"baz":{},"dar":["bbb"]},666],"rad":["usa"],"pat":{"qat":{},"tab":{},"bzr":123}}`,
		`{"foo":123,"bar":[{"baz":{"cat":456,"car":789},"dar":["bbb"]},666],"rad":["usa"],"pat":{"qat":{"xw":{},"bw":123},"tab":{"enter":"return"},"bzr":123}}`,
		`{"foo":123,"bar":[{"baz":{"cat":456,"car":789},"dar":["bbb"]},666],"rad":["usa"],"pat":{"qat":{"xw":{"woei":"oiwers","234":"sdflk"},"bw":123},"tab":{"enter":"return"},"bzr":123}}`,
	}

	opts := NewOptions()
	for i := 0; i < 5; i++ {
		opts = opts.SetMaxDepth(i)
		p := NewParser(opts)
		v, err := p.Parse(input)
		require.NoError(t, err)
		require.Equal(t, expected[i], testMarshalled(t, v))
	}
}

func TestParserParseEmptyArray(t *testing.T) {
	p := NewParser(NewOptions())
	v, err := p.Parse("[]")
	require.NoError(t, err)
	a := v.MustArray()
	require.Equal(t, 0, a.Len())
}

func TestParserParseOneElemArray(t *testing.T) {
	p := NewParser(NewOptions())
	v, err := p.Parse(`   [{"bar":[  [],[[]]   ]} ]  `)
	require.NoError(t, err)
	a := v.MustArray()
	require.Equal(t, 1, a.Len())
	require.Equal(t, value.ObjectType, a.Raw()[0].Type())
	require.Equal(t, `[{"bar":[[],[[]]]}]`, testMarshalled(t, v))
}

func TestParserParseSmallArray(t *testing.T) {
	p := NewParser(NewOptions())
	_, err := p.Parse("[123,{},[]]")
	require.NoError(t, err)
}

func TestParseMultiElemArray(t *testing.T) {
	p := NewParser(NewOptions())
	v, err := p.Parse(`   [1,"foo",{"bar":[     ],"baz":""}    ,[  "x" ,	"y"   ]     ]   `)
	require.NoError(t, err)
	a := v.MustArray()
	require.Equal(t, 4, a.Len())
	require.Equal(t, value.NumberType, a.Raw()[0].Type())
	require.Equal(t, value.BytesType, a.Raw()[1].Type())
	require.Equal(t, value.ObjectType, a.Raw()[2].Type())
	require.Equal(t, value.ArrayType, a.Raw()[3].Type())
	require.Equal(t, `[1,"foo",{"bar":[],"baz":""},["x","y"]]`, testMarshalled(t, v))
}

func TestParserParseIncompleteArray(t *testing.T) {
	p := NewParser(NewOptions())
	inputs := []string{
		"  [ ",
		"[123",
		"[123,",
		"[123,]",
		"[123,{}",
		"[123,{},]",
	}
	for _, input := range inputs {
		_, err := p.Parse(input)
		require.Error(t, err)
	}
}

func TestParserParseBytes(t *testing.T) {
	p := NewParser(NewOptions())
	inputs := []struct {
		str      string
		expected string
	}{
		{
			str:      `"foo\\\""`,
			expected: "foo\\\"",
		},
		{
			str:      `"foo bar"`,
			expected: "foo bar",
		},
		{
			str:      `"foobar"`,
			expected: "foobar",
		},
		{
			str:      `"{\"foo\": 123}"`,
			expected: `{"foo": 123}`,
		},
		{
			str:      `"\n\t\\foo\"bar\u3423x\/\b\f\r\\"`,
			expected: "\n\t\\foo\"bar\u3423x/\b\f\r\\",
		},
		{
			str:      `"x\\"`,
			expected: `x\`,
		},
		{
			str:      `"x\\y"`,
			expected: `x\y`,
		},
		{
			str:      `"\\\\\\\\"`,
			expected: `\\\\`,
		},
		{
			str:      `"\""`,
			expected: `"`,
		},
		{
			str:      `"\\"`,
			expected: `\`,
		},
		{
			str:      `"\\\""`,
			expected: `\"`,
		},
		{
			str:      `"\\\"世界"`,
			expected: `\"世界`,
		},
		{
			str:      `"你好\n\"\\世界"`,
			expected: "你好\n\"\\世界",
		},
		{
			str:      `"q\u1234we"`,
			expected: "q\u1234we",
		},
	}

	for _, input := range inputs {
		v, err := p.Parse(input.str)
		require.NoError(t, err)
		require.Equal(t, []byte(input.expected), v.MustBytes())
	}
}

func TestParserParseBytesError(t *testing.T) {
	p := NewParser(NewOptions()).(*parser)
	inputs := []string{
		`"\"`,
		`"unclosed string`,
		`"foo\qwe"`,
		`"foo\\\\\"大家\n\r\t`,
		`"\"x\uyz\""`,
		`"\u12\"你好"`,
	}

	for _, input := range inputs {
		_, err := p.Parse(input)
		require.Error(t, err)
	}
}

func TestParserParseIncompleteBytes(t *testing.T) {
	p := NewParser(NewOptions())
	inputs := []string{
		`  "foo`,
		`"foo\`,
		`"foo\"`,
		`"foo\\\"`,
		`"foo'`,
		`"foo'bar'`,
	}
	for _, input := range inputs {
		_, err := p.Parse(input)
		require.Error(t, err)
	}
}

func TestParserParseNull(t *testing.T) {
	p := NewParser(NewOptions())
	v, err := p.Parse("null")
	require.NoError(t, err)
	require.Equal(t, value.NullType, v.Type())
}

func TestParserParseTrue(t *testing.T) {
	p := NewParser(NewOptions())
	v, err := p.Parse("true")
	require.NoError(t, err)
	require.Equal(t, value.BoolType, v.Type())
	require.Equal(t, true, v.MustBool())
}

func TestParserParseFalse(t *testing.T) {
	p := NewParser(NewOptions())
	v, err := p.Parse("false")
	require.NoError(t, err)
	require.Equal(t, value.BoolType, v.Type())
	require.Equal(t, false, v.MustBool())
}

func TestParserParseInteger(t *testing.T) {
	p := NewParser(NewOptions())
	v, err := p.Parse("12345")
	require.NoError(t, err)
	require.Equal(t, value.NumberType, v.Type())
	require.Equal(t, float64(12345), v.MustNumber())
}

func TestParserParseSmallFloat(t *testing.T) {
	p := NewParser(NewOptions())
	v, err := p.Parse("-12.345")
	require.NoError(t, err)
	n, err := v.Number()
	require.NoError(t, err)
	require.Equal(t, -12.345, n)
}

func TestParserParseLargeFloat(t *testing.T) {
	p := NewParser(NewOptions())
	v, err := p.Parse("-2134.453E+43")
	require.NoError(t, err)
	_, err = v.Number()
	require.NoError(t, err)
}

func TestParserParseNumber(t *testing.T) {
	p := NewParser(NewOptions()).(*parser)
	inputs := []struct {
		str      string
		expected float64
		tail     string
	}{
		{str: "0", expected: 0, tail: ""},
		{str: "123", expected: 123, tail: ""},
		{str: "-123", expected: -123, tail: ""},
		{str: "-12.345 aa", expected: -12.345, tail: " aa"},
	}

	for _, input := range inputs {
		p.reset()
		p.str = input.str
		v, err := p.parseNumber()
		require.NoError(t, err)
		require.Equal(t, value.NumberType, v.Type())
		require.Equal(t, input.expected, v.MustNumber())
		require.Equal(t, input.tail, p.str[p.pos:])
	}
}

func TestParserParseNumberError(t *testing.T) {
	p := NewParser(NewOptions()).(*parser)
	inputs := []string{
		"xyz",
		" ",
		"[",
		",",
		"{",
		"\"",
	}
	for _, input := range inputs {
		p.reset()
		p.str = input
		_, err := p.parseNumber()
		require.Error(t, err)
	}
}

func testMarshalled(t *testing.T, v *value.Value) string {
	marshalled, err := v.MarshalTo(nil)
	require.NoError(t, err)
	return string(marshalled)
}

func getFromTestFile(filename string) string {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		panic(fmt.Errorf("cannot read %s: %s", filename, err))
	}
	return string(data)
}

var (
	// Small, medium and large fixtures are from https://github.com/buger/jsonparser/blob/f04e003e4115787c6272636780bc206e5ffad6c4/benchmark/benchmark.go.
	smallFixture  = getFromTestFile("testdata/small.json")
	mediumFixture = getFromTestFile("testdata/medium.json")
	largeFixture  = getFromTestFile("testdata/large.json")

	// Canada, citm and twitter fixtures are from https://github.com/serde-rs/json-benchmark/tree/0db02e043b3ae87dc5065e7acb8654c1f7670c43/data.
	canadaFixture  = getFromTestFile("testdata/canada.json")
	citmFixture    = getFromTestFile("testdata/citm_catalog.json")
	twitterFixture = getFromTestFile("testdata/twitter.json")
)
