package integration

import (
	"time"

	"github.com/xichen2020/eventdb/document"
	"github.com/xichen2020/eventdb/document/field"
	"github.com/xichen2020/eventdb/filter"
	"github.com/xichen2020/eventdb/parser/json"
	"github.com/xichen2020/eventdb/parser/json/value"
	"github.com/xichen2020/eventdb/query"
	"github.com/xichen2020/eventdb/x/safe"
	"github.com/xichen2020/eventdb/x/strings"
	xtime "github.com/xichen2020/eventdb/x/time"
)

type conditionFn func() bool

func waitUntil(fn conditionFn, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return true
		}
		time.Sleep(time.Second)
	}
	return false
}

func newDocumentFromRaw(
	docID string,
	rawStr string,
	timestampFieldPath []string,
	timestampFormat string,
) (document.Document, error) {
	parser := json.NewParser(nil)
	v, err := parser.Parse(rawStr)
	if err != nil {
		return document.Document{}, err
	}

	var (
		fields field.Fields
		it     = value.NewFieldIterator(v)
	)
	for it.Next() {
		f := it.Current()
		cloned := f.Clone()
		// Skip timestamp field since that's already captured by the `TimeNanos` field.
		if strings.Equal(timestampFieldPath, cloned.Path) {
			continue
		}
		fields = append(fields, cloned)
	}
	it.Close()

	tsVal, ok := v.Get(timestampFieldPath...)
	if !ok {
		return document.Document{}, err
	}
	tsNanos, err := parseTimestamp(tsVal, timestampFormat)
	if err != nil {
		return document.Document{}, err
	}

	return document.Document{
		ID:        b(docID),
		TimeNanos: tsNanos,
		RawData:   b(rawStr),
		Fields:    fields,
	}, nil
}

func parseTimestamp(v *value.Value, timestampFormat string) (int64, error) {
	b, err := v.Bytes()
	if err != nil {
		return 0, err
	}
	t, err := time.Parse(timestampFormat, safe.ToString(b))
	if err != nil {
		return 0, err
	}
	return t.UnixNano(), nil
}

func b(str string) []byte                                       { return []byte(str) }
func pInt(v int) *int                                           { return &v }
func pInt64(v int64) *int64                                     { return &v }
func pString(v string) *string                                  { return &v }
func pOrderBy(ob query.SortOrder) *query.SortOrder              { return &ob }
func pFilterCombinator(fc filter.Combinator) *filter.Combinator { return &fc }
func pTimeUnit(v xtime.Unit) *xtime.Unit                        { return &v }
func pDuration(v xtime.Duration) *xtime.Duration                { return &v }
