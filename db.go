package db

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// TracedCollectionWrapper wraps mongo.Collection to automatically create spans for all operations
type TracedCollectionWrapper struct {
	*mongo.Collection
	dbName         string
	collectionName string
}

// NewTracedCollectionWrapper creates a new TracedCollectionWrapper with database and collection names
func NewTracedCollectionWrapper(collection *mongo.Collection, dbName string, collectionName string) *TracedCollectionWrapper {
	return &TracedCollectionWrapper{
		Collection:     collection,
		dbName:         dbName,
		collectionName: collectionName,
	}
}

// queryToString converts a MongoDB query (filter, document, pipeline, etc.) to a JSON string representation
func queryToString(query interface{}) string {
	if query == nil {
		return "{}"
	}

	// Handle empty bson.M
	if doc, ok := query.(bson.M); ok {
		if len(doc) == 0 {
			return "{}"
		}
		// Try to convert to BSON Extended JSON
		if jsonBytes, err := bson.MarshalExtJSON(doc, false, false); err == nil {
			return string(jsonBytes)
		}
		// Fallback to regular JSON
		if jsonBytes, err := json.Marshal(doc); err == nil {
			return string(jsonBytes)
		}
	}

	// Handle slice/array (for InsertMany, Aggregate pipeline)
	if arr, ok := query.([]interface{}); ok {
		if len(arr) == 0 {
			return "[]"
		}
		if jsonBytes, err := json.Marshal(arr); err == nil {
			return string(jsonBytes)
		}
	}

	// Try to convert to JSON for other types
	if jsonBytes, err := json.Marshal(query); err == nil {
		return string(jsonBytes)
	}

	// Fallback to string representation
	return fmt.Sprintf("%v", query)
}

// buildMongoShellCommand builds a MongoDB shell command string that can be copied and run in mongo shell
func buildMongoShellCommand(operation string, collectionName string, query interface{}, additionalParams ...interface{}) string {
	queryStr := queryToString(query)

	switch operation {
	case "find":
		return fmt.Sprintf("db.%s.find(%s)", collectionName, queryStr)
	case "findOne":
		return fmt.Sprintf("db.%s.findOne(%s)", collectionName, queryStr)
	case "insertOne":
		return fmt.Sprintf("db.%s.insertOne(%s)", collectionName, queryStr)
	case "insertMany":
		return fmt.Sprintf("db.%s.insertMany(%s)", collectionName, queryStr)
	case "updateOne":
		if len(additionalParams) > 0 {
			updateStr := queryToString(additionalParams[0])
			return fmt.Sprintf("db.%s.updateOne(%s, %s)", collectionName, queryStr, updateStr)
		}
		return fmt.Sprintf("db.%s.updateOne(%s, {})", collectionName, queryStr)
	case "updateMany":
		if len(additionalParams) > 0 {
			updateStr := queryToString(additionalParams[0])
			return fmt.Sprintf("db.%s.updateMany(%s, %s)", collectionName, queryStr, updateStr)
		}
		return fmt.Sprintf("db.%s.updateMany(%s, {})", collectionName, queryStr)
	case "deleteOne":
		return fmt.Sprintf("db.%s.deleteOne(%s)", collectionName, queryStr)
	case "deleteMany":
		return fmt.Sprintf("db.%s.deleteMany(%s)", collectionName, queryStr)
	case "replaceOne":
		if len(additionalParams) > 0 {
			replacementStr := queryToString(additionalParams[0])
			return fmt.Sprintf("db.%s.replaceOne(%s, %s)", collectionName, queryStr, replacementStr)
		}
		return fmt.Sprintf("db.%s.replaceOne(%s, {})", collectionName, queryStr)
	case "countDocuments":
		return fmt.Sprintf("db.%s.countDocuments(%s)", collectionName, queryStr)
	case "aggregate":
		return fmt.Sprintf("db.%s.aggregate(%s)", collectionName, queryStr)
	case "findOneAndUpdate":
		if len(additionalParams) > 0 {
			updateStr := queryToString(additionalParams[0])
			return fmt.Sprintf("db.%s.findOneAndUpdate(%s, %s)", collectionName, queryStr, updateStr)
		}
		return fmt.Sprintf("db.%s.findOneAndUpdate(%s, {})", collectionName, queryStr)
	case "findOneAndReplace":
		if len(additionalParams) > 0 {
			replacementStr := queryToString(additionalParams[0])
			return fmt.Sprintf("db.%s.findOneAndReplace(%s, %s)", collectionName, queryStr, replacementStr)
		}
		return fmt.Sprintf("db.%s.findOneAndReplace(%s, {})", collectionName, queryStr)
	case "findOneAndDelete":
		return fmt.Sprintf("db.%s.findOneAndDelete(%s)", collectionName, queryStr)
	case "estimatedDocumentCount":
		return fmt.Sprintf("db.%s.estimatedDocumentCount()", collectionName)
	case "distinct":
		if len(additionalParams) > 0 {
			fieldName := fmt.Sprintf("%v", additionalParams[0])
			return fmt.Sprintf("db.%s.distinct(%s, %s)", collectionName, fieldName, queryStr)
		}
		return fmt.Sprintf("db.%s.distinct(\"field\", %s)", collectionName, queryStr)
	default:
		return fmt.Sprintf("db.%s.%s(%s)", collectionName, operation, queryStr)
	}
}

// createSpan creates a span for a database operation
func (tc *TracedCollectionWrapper) createSpan(ctx context.Context, operation string, functionName string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	tracer := otel.Tracer("backend-service")

	// Use function name as span name (e.g., "Find", "InsertOne")
	spanName := functionName

	// Use database.collection format for db.name (X-Ray will use this as segment name - the green span)
	dbNameWithCollection := fmt.Sprintf("%s.%s", tc.dbName, tc.collectionName)

	baseAttrs := []attribute.KeyValue{
		attribute.String("db.system", "mongodb"),
		// Use database.collection format for db.name - X-Ray will use this as segment name
		attribute.String("db.name", dbNameWithCollection),
		// Store original database name in a separate attribute
		attribute.String("db.database", tc.dbName),
		attribute.String("db.collection", tc.collectionName),
		attribute.String("db.operation", operation),
	}
	baseAttrs = append(baseAttrs, attrs...)

	ctx, span := tracer.Start(ctx, spanName, trace.WithAttributes(baseAttrs...))

	return ctx, span
}

// Find wraps mongo.Collection.Find with automatic span creation
// Returns cursor and context with active span for setting result count later
func (tc *TracedCollectionWrapper) Find(ctx context.Context, filter interface{}, opts ...*options.FindOptions) (*mongo.Cursor, context.Context, error) {
	queryStr := buildMongoShellCommand("find", tc.collectionName, filter)
	ctx, span := tc.createSpan(ctx, "find", "Find", attribute.String("db.query.string", queryStr))
	// Don't defer span.End() here - let SetQueryResultCount handle it

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	cursor, err := tc.Collection.Find(ctx, filter, opts...)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to query DocumentDB")
		span.End() // Close span on error
		return nil, ctx, err
	}

	// Return context with active span so result count can be set later
	return cursor, ctx, nil
}

// SetQueryResultCount sets the result count attribute on the current span and closes it
func SetQueryResultCount(ctx context.Context, count int) {
	span := trace.SpanFromContext(ctx)
	if span.IsRecording() {
		span.SetAttributes(
			attribute.Int("db.query.result.count", count),
		)
		span.SetStatus(codes.Ok, "Query successful")
	}
	span.End() // Close the span after setting attributes
}

// FindAll wraps Find + All operations with automatic count setting
// This function performs Find, decodes all results, sets the count automatically, and closes the span
// results must be a pointer to a slice (e.g., &[]bson.M, &[]YourStruct)
func (tc *TracedCollectionWrapper) FindAll(ctx context.Context, filter interface{}, results interface{}, opts ...*options.FindOptions) error {
	// Call Find to get cursor and context with active span
	cursor, dbCtx, err := tc.Find(ctx, filter, opts...)
	if err != nil {
		return err
	}
	defer cursor.Close(dbCtx)

	// Decode all documents
	if err = cursor.All(dbCtx, results); err != nil {
		span := trace.SpanFromContext(dbCtx)
		if span.IsRecording() {
			span.RecordError(err)
			span.SetStatus(codes.Error, "Failed to decode documents")
			span.End()
		}
		return err
	}

	// Get count from results using reflection
	var count int
	rv := reflect.ValueOf(results)
	if rv.Kind() == reflect.Ptr {
		rv = rv.Elem()
	}
	if rv.Kind() == reflect.Slice {
		count = rv.Len()
	}

	// Set result count on span and close it automatically
	SetQueryResultCount(dbCtx, count)
	return nil
}

// FindOne wraps mongo.Collection.FindOne with automatic span creation
func (tc *TracedCollectionWrapper) FindOne(ctx context.Context, filter interface{}, opts ...*options.FindOneOptions) *mongo.SingleResult {
	queryStr := buildMongoShellCommand("findOne", tc.collectionName, filter)
	ctx, span := tc.createSpan(ctx, "findOne", "FindOne", attribute.String("db.query.string", queryStr))
	defer span.End()

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	result := tc.Collection.FindOne(ctx, filter, opts...)
	if result.Err() != nil {
		span.RecordError(result.Err())
		span.SetStatus(codes.Error, "Failed to query DocumentDB")
	} else {
		span.SetStatus(codes.Ok, "Query successful")
	}

	return result
}

// InsertOne wraps mongo.Collection.InsertOne with automatic span creation
func (tc *TracedCollectionWrapper) InsertOne(ctx context.Context, document interface{}, opts ...*options.InsertOneOptions) (*mongo.InsertOneResult, error) {
	// Extract document fields for span attributes if possible
	var attrs []attribute.KeyValue
	if doc, ok := document.(bson.M); ok {
		if name, ok := doc["name"].(string); ok {
			attrs = append(attrs, attribute.String("db.document.name", name))
		}
		if lastname, ok := doc["lastname"].(string); ok {
			attrs = append(attrs, attribute.String("db.document.lastname", lastname))
		}
	}

	// Build MongoDB shell command
	queryStr := buildMongoShellCommand("insertOne", tc.collectionName, document)
	attrs = append(attrs, attribute.String("db.query.string", queryStr))
	ctx, span := tc.createSpan(ctx, "insertOne", "InsertOne", attrs...)
	defer span.End()

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	result, err := tc.Collection.InsertOne(ctx, document, opts...)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to insert document")
		return nil, err
	}

	// Set span attributes for successful insert
	insertedIDStr := ""
	if id, ok := result.InsertedID.(string); ok {
		insertedIDStr = id
	} else {
		insertedIDStr = fmt.Sprintf("%v", result.InsertedID)
	}
	span.SetAttributes(attribute.String("db.insert.id", insertedIDStr))
	span.SetStatus(codes.Ok, "Insert successful")

	return result, nil
}

// InsertMany wraps mongo.Collection.InsertMany with automatic span creation
func (tc *TracedCollectionWrapper) InsertMany(ctx context.Context, documents []interface{}, opts ...*options.InsertManyOptions) (*mongo.InsertManyResult, error) {
	// Build MongoDB shell command
	queryStr := buildMongoShellCommand("insertMany", tc.collectionName, documents)
	ctx, span := tc.createSpan(ctx, "insertMany", "InsertMany",
		attribute.Int("db.insert.count", len(documents)),
		attribute.String("db.query.string", queryStr),
	)
	defer span.End()

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	result, err := tc.Collection.InsertMany(ctx, documents, opts...)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to insert documents")
		return nil, err
	}

	span.SetAttributes(attribute.Int("db.insert.inserted_count", len(result.InsertedIDs)))
	span.SetStatus(codes.Ok, "Insert successful")

	return result, nil
}

// UpdateOne wraps mongo.Collection.UpdateOne with automatic span creation
func (tc *TracedCollectionWrapper) UpdateOne(ctx context.Context, filter interface{}, update interface{}, opts ...*options.UpdateOptions) (*mongo.UpdateResult, error) {
	// Build MongoDB shell command
	queryStr := buildMongoShellCommand("updateOne", tc.collectionName, filter, update)
	ctx, span := tc.createSpan(ctx, "updateOne", "UpdateOne", attribute.String("db.query.string", queryStr))
	defer span.End()

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	result, err := tc.Collection.UpdateOne(ctx, filter, update, opts...)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to update document")
		return nil, err
	}

	span.SetAttributes(
		attribute.Int64("db.update.matched_count", result.MatchedCount),
		attribute.Int64("db.update.modified_count", result.ModifiedCount),
	)
	span.SetStatus(codes.Ok, "Update successful")

	return result, nil
}

// UpdateMany wraps mongo.Collection.UpdateMany with automatic span creation
func (tc *TracedCollectionWrapper) UpdateMany(ctx context.Context, filter interface{}, update interface{}, opts ...*options.UpdateOptions) (*mongo.UpdateResult, error) {
	// Build MongoDB shell command
	queryStr := buildMongoShellCommand("updateMany", tc.collectionName, filter, update)
	ctx, span := tc.createSpan(ctx, "updateMany", "UpdateMany", attribute.String("db.query.string", queryStr))
	defer span.End()

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	result, err := tc.Collection.UpdateMany(ctx, filter, update, opts...)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to update documents")
		return nil, err
	}

	span.SetAttributes(
		attribute.Int64("db.update.matched_count", result.MatchedCount),
		attribute.Int64("db.update.modified_count", result.ModifiedCount),
	)
	span.SetStatus(codes.Ok, "Update successful")

	return result, nil
}

// DeleteOne wraps mongo.Collection.DeleteOne with automatic span creation
func (tc *TracedCollectionWrapper) DeleteOne(ctx context.Context, filter interface{}, opts ...*options.DeleteOptions) (*mongo.DeleteResult, error) {
	queryStr := buildMongoShellCommand("deleteOne", tc.collectionName, filter)
	ctx, span := tc.createSpan(ctx, "deleteOne", "DeleteOne", attribute.String("db.query.string", queryStr))
	defer span.End()

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	result, err := tc.Collection.DeleteOne(ctx, filter, opts...)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to delete document")
		return nil, err
	}

	span.SetAttributes(attribute.Int64("db.delete.deleted_count", result.DeletedCount))
	span.SetStatus(codes.Ok, "Delete successful")

	return result, nil
}

// DeleteMany wraps mongo.Collection.DeleteMany with automatic span creation
func (tc *TracedCollectionWrapper) DeleteMany(ctx context.Context, filter interface{}, opts ...*options.DeleteOptions) (*mongo.DeleteResult, error) {
	queryStr := buildMongoShellCommand("deleteMany", tc.collectionName, filter)
	ctx, span := tc.createSpan(ctx, "deleteMany", "DeleteMany", attribute.String("db.query.string", queryStr))
	defer span.End()

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	result, err := tc.Collection.DeleteMany(ctx, filter, opts...)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to delete documents")
		return nil, err
	}

	span.SetAttributes(attribute.Int64("db.delete.deleted_count", result.DeletedCount))
	span.SetStatus(codes.Ok, "Delete successful")

	return result, nil
}

// ReplaceOne wraps mongo.Collection.ReplaceOne with automatic span creation
func (tc *TracedCollectionWrapper) ReplaceOne(ctx context.Context, filter interface{}, replacement interface{}, opts ...*options.ReplaceOptions) (*mongo.UpdateResult, error) {
	// Build MongoDB shell command
	queryStr := buildMongoShellCommand("replaceOne", tc.collectionName, filter, replacement)
	ctx, span := tc.createSpan(ctx, "replaceOne", "ReplaceOne", attribute.String("db.query.string", queryStr))
	defer span.End()

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	result, err := tc.Collection.ReplaceOne(ctx, filter, replacement, opts...)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to replace document")
		return nil, err
	}

	span.SetAttributes(
		attribute.Int64("db.update.matched_count", result.MatchedCount),
		attribute.Int64("db.update.modified_count", result.ModifiedCount),
	)
	span.SetStatus(codes.Ok, "Replace successful")

	return result, nil
}

// CountDocuments wraps mongo.Collection.CountDocuments with automatic span creation
func (tc *TracedCollectionWrapper) CountDocuments(ctx context.Context, filter interface{}, opts ...*options.CountOptions) (int64, error) {
	queryStr := buildMongoShellCommand("countDocuments", tc.collectionName, filter)
	ctx, span := tc.createSpan(ctx, "countDocuments", "CountDocuments", attribute.String("db.query.string", queryStr))
	defer span.End()

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	count, err := tc.Collection.CountDocuments(ctx, filter, opts...)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to count documents")
		return 0, err
	}

	span.SetAttributes(attribute.Int64("db.query.result.count", count))
	span.SetStatus(codes.Ok, "Count successful")

	return count, nil
}

// Aggregate wraps mongo.Collection.Aggregate with automatic span creation
func (tc *TracedCollectionWrapper) Aggregate(ctx context.Context, pipeline interface{}, opts ...*options.AggregateOptions) (*mongo.Cursor, error) {
	queryStr := buildMongoShellCommand("aggregate", tc.collectionName, pipeline)
	ctx, span := tc.createSpan(ctx, "aggregate", "Aggregate", attribute.String("db.query.string", queryStr))
	defer span.End()

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	cursor, err := tc.Collection.Aggregate(ctx, pipeline, opts...)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to aggregate documents")
		return nil, err
	}

	span.SetStatus(codes.Ok, "Aggregate successful")
	return cursor, nil
}

// FindOneAndUpdate wraps mongo.Collection.FindOneAndUpdate with automatic span creation
func (tc *TracedCollectionWrapper) FindOneAndUpdate(ctx context.Context, filter interface{}, update interface{}, opts ...*options.FindOneAndUpdateOptions) *mongo.SingleResult {
	queryStr := buildMongoShellCommand("findOneAndUpdate", tc.collectionName, filter, update)
	ctx, span := tc.createSpan(ctx, "findOneAndUpdate", "FindOneAndUpdate", attribute.String("db.query.string", queryStr))
	defer span.End()

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	result := tc.Collection.FindOneAndUpdate(ctx, filter, update, opts...)
	if result.Err() != nil {
		span.RecordError(result.Err())
		span.SetStatus(codes.Error, "Failed to find and update document")
	} else {
		span.SetStatus(codes.Ok, "FindOneAndUpdate successful")
	}

	return result
}

// FindOneAndReplace wraps mongo.Collection.FindOneAndReplace with automatic span creation
func (tc *TracedCollectionWrapper) FindOneAndReplace(ctx context.Context, filter interface{}, replacement interface{}, opts ...*options.FindOneAndReplaceOptions) *mongo.SingleResult {
	queryStr := buildMongoShellCommand("findOneAndReplace", tc.collectionName, filter, replacement)
	ctx, span := tc.createSpan(ctx, "findOneAndReplace", "FindOneAndReplace", attribute.String("db.query.string", queryStr))
	defer span.End()

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	result := tc.Collection.FindOneAndReplace(ctx, filter, replacement, opts...)
	if result.Err() != nil {
		span.RecordError(result.Err())
		span.SetStatus(codes.Error, "Failed to find and replace document")
	} else {
		span.SetStatus(codes.Ok, "FindOneAndReplace successful")
	}

	return result
}

// FindOneAndDelete wraps mongo.Collection.FindOneAndDelete with automatic span creation
func (tc *TracedCollectionWrapper) FindOneAndDelete(ctx context.Context, filter interface{}, opts ...*options.FindOneAndDeleteOptions) *mongo.SingleResult {
	queryStr := buildMongoShellCommand("findOneAndDelete", tc.collectionName, filter)
	ctx, span := tc.createSpan(ctx, "findOneAndDelete", "FindOneAndDelete", attribute.String("db.query.string", queryStr))
	defer span.End()

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	result := tc.Collection.FindOneAndDelete(ctx, filter, opts...)
	if result.Err() != nil {
		span.RecordError(result.Err())
		span.SetStatus(codes.Error, "Failed to find and delete document")
	} else {
		span.SetStatus(codes.Ok, "FindOneAndDelete successful")
	}

	return result
}

// BulkWrite wraps mongo.Collection.BulkWrite with automatic span creation
func (tc *TracedCollectionWrapper) BulkWrite(ctx context.Context, models []mongo.WriteModel, opts ...*options.BulkWriteOptions) (*mongo.BulkWriteResult, error) {
	// Build query string from models
	modelsStr := queryToString(models)
	queryStr := fmt.Sprintf("db.%s.bulkWrite(%s)", tc.collectionName, modelsStr)
	ctx, span := tc.createSpan(ctx, "bulkWrite", "BulkWrite",
		attribute.String("db.query.string", queryStr),
		attribute.Int("db.bulk.operations", len(models)),
	)
	defer span.End()

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	result, err := tc.Collection.BulkWrite(ctx, models, opts...)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to bulk write documents")
		return nil, err
	}

	span.SetAttributes(
		attribute.Int64("db.bulk.inserted_count", result.InsertedCount),
		attribute.Int64("db.bulk.matched_count", result.MatchedCount),
		attribute.Int64("db.bulk.modified_count", result.ModifiedCount),
		attribute.Int64("db.bulk.deleted_count", result.DeletedCount),
		attribute.Int64("db.bulk.upserted_count", result.UpsertedCount),
	)
	span.SetStatus(codes.Ok, "BulkWrite successful")

	return result, nil
}

// EstimatedDocumentCount wraps mongo.Collection.EstimatedDocumentCount with automatic span creation
func (tc *TracedCollectionWrapper) EstimatedDocumentCount(ctx context.Context, opts ...*options.EstimatedDocumentCountOptions) (int64, error) {
	queryStr := buildMongoShellCommand("estimatedDocumentCount", tc.collectionName, nil)
	ctx, span := tc.createSpan(ctx, "estimatedDocumentCount", "EstimatedDocumentCount", attribute.String("db.query.string", queryStr))
	defer span.End()

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	count, err := tc.Collection.EstimatedDocumentCount(ctx, opts...)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to estimate document count")
		return 0, err
	}

	span.SetAttributes(attribute.Int64("db.query.result.count", count))
	span.SetStatus(codes.Ok, "EstimatedDocumentCount successful")

	return count, nil
}

// Distinct wraps mongo.Collection.Distinct with automatic span creation
func (tc *TracedCollectionWrapper) Distinct(ctx context.Context, fieldName string, filter interface{}, opts ...*options.DistinctOptions) ([]interface{}, error) {
	queryStr := buildMongoShellCommand("distinct", tc.collectionName, filter, fieldName)
	ctx, span := tc.createSpan(ctx, "distinct", "Distinct",
		attribute.String("db.query.string", queryStr),
		attribute.String("db.distinct.field", fieldName),
	)
	defer span.End()

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	values, err := tc.Collection.Distinct(ctx, fieldName, filter, opts...)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "Failed to get distinct values")
		return nil, err
	}

	span.SetAttributes(attribute.Int("db.distinct.result.count", len(values)))
	span.SetStatus(codes.Ok, "Distinct successful")

	return values, nil
}
