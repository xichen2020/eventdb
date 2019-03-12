package storage

import (
	"fmt"

	indexfield "github.com/xichen2020/eventdb/index/field"
	"github.com/xichen2020/eventdb/index/segment"
	"github.com/xichen2020/eventdb/persist"

	xerrors "github.com/m3db/m3x/errors"
)

// updatingSegmentRetriever retrieves fields from persistent storage, and updates
// the corresponding segment with the retrieved fields so they no longer need to
// be retrieved for future queries.
type updatingSegmentRetriever struct {
	namespace []byte
	dbSegment *dbSegment
	retriever persist.FieldRetriever
}

func newUpdatingSegmentRetriever(
	namespace []byte,
	dbSegment *dbSegment,
	retriever persist.FieldRetriever,
) *updatingSegmentRetriever {
	return &updatingSegmentRetriever{
		namespace: namespace,
		dbSegment: dbSegment,
		retriever: retriever,
	}
}

func (r *updatingSegmentRetriever) Retrieve(
	seg segment.ImmutableSegment,
	toRetrieve []persist.RetrieveFieldOptions,
) ([]indexfield.DocsField, error) {
	if len(toRetrieve) == 0 {
		return nil, nil
	}

	// NB: If no error, len(retrieved) == len(toRetrieve).
	retrieved, err := r.retrieve(seg.Metadata(), toRetrieve)
	if err != nil {
		return nil, err
	}

	if err := r.update(seg, retrieved, toRetrieve); err != nil {
		for i := range retrieved {
			if retrieved[i] != nil {
				retrieved[i].Close()
				retrieved[i] = nil
			}
		}
		return nil, err
	}

	return retrieved, nil
}

func (r *updatingSegmentRetriever) retrieve(
	segmentMeta segment.Metadata,
	toRetrieve []persist.RetrieveFieldOptions,
) ([]indexfield.DocsField, error) {
	res := make([]indexfield.DocsField, 0, len(toRetrieve))
	for _, retrieveOpts := range toRetrieve {
		// NB(xichen): This assumes that the loaded field is never nil if err == nil.
		retrieved, err := r.retriever.RetrieveField(r.namespace, segmentMeta, retrieveOpts)
		if err != nil {
			for i := range res {
				res[i].Close()
				res[i] = nil
			}
			return nil, err
		}
		res = append(res, retrieved)
	}
	return res, nil
}

// Precondition: len(retrieved) == len(retrieveOpts).
// Postcondition: If no error, `fields` contains and owns the fields loaded for `metas`,
// and should be closed when processing is done.
func (r *updatingSegmentRetriever) update(
	seg segment.ImmutableSegment,
	retrieved []indexfield.DocsField,
	retrieveOptsList []persist.RetrieveFieldOptions,
) error {
	var (
		multiErr xerrors.MultiError
		updated  bool
	)
	for i, retrieveOpts := range retrieveOptsList {
		f, exists := seg.FieldAt(retrieveOpts.FieldPath)
		if !exists {
			err := fmt.Errorf("field %v loaded but does not exist in the segment", retrieveOpts.FieldPath)
			multiErr = multiErr.Add(err)
			continue
		}
		f.Insert(retrieved[i])
		updated = true
	}

	if updated {
		r.dbSegment.setLoadedStatus(segmentPartiallyLoaded)
	}

	return multiErr.FinalError()
}
