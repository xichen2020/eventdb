package storage

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"github.com/xichen2020/eventdb/document"
	"github.com/xichen2020/eventdb/document/field"
	"github.com/xichen2020/eventdb/parser/json"
	"github.com/xichen2020/eventdb/parser/json/value"

	"github.com/pborman/uuid"
)

const (
	testJSONDataFilePath = "./testdata/testdata.json"
	testDataPath         = "./testdata/eventdb"
	testDataSeparator    = '\n'
)

var (
	testNamespace = []byte("testNamespace")
)

type testDatabase struct {
	Database
}

func newTestDatabase() (*testDatabase, error) {
	namespaces := []NamespaceMetadata{
		NamespaceMetadata{
			id:   testNamespace,
			opts: NewNamespaceOptions(),
		},
	}

	dbOpts := NewOptions().
		SetNamespaceFieldName(string(testNamespace))

	db := NewDatabase(namespaces, dbOpts)
	if err := db.Open(); err != nil {
		return nil, err
	}

	return &testDatabase{db}, nil
}

// Close removes all filesystem resources owned by the test database.
func (tdb *testDatabase) Close() error {
	tdb.Database.Close()
	return os.RemoveAll(testDataPath)
}

func createTestDocuments() ([]byte, []document.Document, error) {
	p := json.NewParser(json.NewOptions())

	data, err := ioutil.ReadFile(testJSONDataFilePath)
	if err != nil {
		return nil, nil, err
	}

	var (
		end  = bytes.IndexByte(data, testDataSeparator)
		docs = make([]document.Document, 0)
	)
	if end < 0 {
		end = len(data)
	}
	for end > 0 {
		v, err := p.ParseBytes(data[:end])
		if err != nil {
			err = fmt.Errorf("cannot parse document %s: %v", data, err)
			return nil, nil, err
		}

		var (
			fields    = make([]field.Field, 0, 64)
			fieldIter = value.NewFieldIterator(v)
		)
		for fieldIter.Next() {
			curr := fieldIter.Current()
			// Need to copy here as the field only remains valid till the next iteration.
			fields = append(fields, curr.Clone())
		}
		fieldIter.Close()

		doc := document.Document{
			ID:        uuid.NewUUID(),
			TimeNanos: time.Now().UnixNano(), // This doesn't need to line up to what's in the raw data.
			Fields:    fields,
			RawData:   data,
		}
		docs = append(docs, doc)

		data = data[end+1:]
		end = bytes.IndexByte(data, testDataSeparator)
	}
	return []byte(testNamespace), docs, nil
}
