package db

import (
	"context"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/integration/mtest"
)

// TestFindAll_SingleBatch verifies the case that already worked before the fix:
// a result set small enough to arrive in the first batch (cursorID 0, nothing to
// fetch via getMore) must keep decoding successfully.
func TestFindAll_SingleBatch(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().CreateClient(false))

	mt.RunOpts("single batch", mtest.NewOptions().ClientType(mtest.Mock), func(mt *mtest.T) {
		ns := mt.DB.Name() + "." + mt.Coll.Name()
		docs := []bson.D{
			{{Key: "post_id", Value: 1}},
			{{Key: "post_id", Value: 2}},
		}
		// cursorID 0 in the find response means the entire result set fit in the
		// first batch; no getMore is ever issued.
		find := mtest.CreateCursorResponse(0, ns, mtest.FirstBatch, docs...)
		mt.AddMockResponses(find)

		tracedDB := NewTracedDatabase(mt.DB)
		coll := tracedDB.Collection(mt.Coll.Name())

		var results []bson.M
		err := coll.FindAll(context.Background(), bson.D{}, &results)
		if err != nil {
			t.Fatalf("FindAll returned unexpected error for a single-batch result: %v", err)
		}
		if len(results) != 2 {
			t.Fatalf("expected 2 documents, got %d", len(results))
		}
	})
}

// TestFindAll_MultiBatch reproduces the production bug: a result set that spans
// more than one cursor batch requires a getMore round trip. FindAll() must
// succeed by keeping the context alive through that round trip, not just
// through the initial Find() call.
func TestFindAll_MultiBatch(t *testing.T) {
	mt := mtest.New(t, mtest.NewOptions().CreateClient(false))

	mt.RunOpts("multi batch", mtest.NewOptions().ClientType(mtest.Mock), func(mt *mtest.T) {
		ns := mt.DB.Name() + "." + mt.Coll.Name()
		cursorID := int64(77)
		firstBatch := []bson.D{{{Key: "post_id", Value: 1}}}
		nextBatch := []bson.D{{{Key: "post_id", Value: 2}}}

		// Nonzero cursorID on the find response means more documents are
		// pending and the driver must issue a getMore to fetch them. The
		// getMore response below returns cursorID 0, ending the cursor.
		find := mtest.CreateCursorResponse(cursorID, ns, mtest.FirstBatch, firstBatch...)
		getMore := mtest.CreateCursorResponse(0, ns, mtest.NextBatch, nextBatch...)
		mt.AddMockResponses(find, getMore)

		tracedDB := NewTracedDatabase(mt.DB)
		coll := tracedDB.Collection(mt.Coll.Name())

		var results []bson.M
		err := coll.FindAll(context.Background(), bson.D{}, &results)
		if err != nil {
			t.Fatalf("FindAll returned unexpected error for a multi-batch result (getMore round trip): %v", err)
		}
		if len(results) != 2 {
			t.Fatalf("expected 2 documents across 2 batches, got %d", len(results))
		}
	})
}
