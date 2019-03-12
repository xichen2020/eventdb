package executor

import (
	"fmt"

	"github.com/xichen2020/eventdb/index"
	indexfield "github.com/xichen2020/eventdb/index/field"
	"github.com/xichen2020/eventdb/query"
)

// NB: This method owns `maskingDocIDSetIt` and handles closing regardless of success or failure.
func collectTimeBucketResults(
	timestampField indexfield.TimeField,
	maskingDocIDSetIter index.DocIDSetIterator,
	res *query.TimeBucketResults,
) error {
	timestampIter, err := timestampField.Fetch(maskingDocIDSetIter)
	if err != nil {
		maskingDocIDSetIter.Close()
		return fmt.Errorf("error fetching timestamp data: %v", err)
	}
	defer timestampIter.Close()

	// TODO(xichen): This decodes the timestamp field again if the timestamp field is loaded from
	// disk as we are currently not caching decoded values, and even if we do, we still do another
	// iteration of the timestamp values which we could have got from the filter iterators.
	for timestampIter.Next() {
		res.AddAt(timestampIter.Value())
	}
	if err := timestampIter.Err(); err != nil {
		return fmt.Errorf("error iterating over timestamp data: %v", err)
	}
	return nil
}
