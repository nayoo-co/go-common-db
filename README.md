# go-common-db

A Go shared library for MongoDB database operations with automatic OpenTelemetry tracing support.

## Features

- Wraps MongoDB collection operations with automatic span creation
- Supports all common MongoDB operations (Find, Insert, Update, Delete, Aggregate, etc.)
- Automatic query string generation for debugging
- OpenTelemetry integration for distributed tracing
- Context-based timeout handling (10 seconds default)

## Installation

```bash
go get github.com/nayoo/go-common-db
```

## Usage

### Basic Setup

```go
package main

import (
    "context"
    "go.mongodb.org/mongo-driver/mongo"
    "go.mongodb.org/mongo-driver/mongo/options"
    "github.com/nayoo/go-common-db"
)

func main() {
    // Connect to MongoDB
    client, err := mongo.Connect(context.Background(), options.Client().ApplyURI("mongodb://localhost:27017"))
    if err != nil {
        panic(err)
    }
    defer client.Disconnect(context.Background())

    // Get database
    db := client.Database("mydb")

    // Create traced database wrapper
    tracedDB := db.NewTracedDatabase(db)

    // Get traced collection
    tracedCollection := tracedDB.Collection("users")

    // Use the traced collection for operations
    ctx := context.Background()
    
    // Example: Insert a document
    result, err := tracedCollection.InsertOne(ctx, bson.M{
        "name": "John",
        "age": 30,
    })
    if err != nil {
        panic(err)
    }
    fmt.Printf("Inserted ID: %v\n", result.InsertedID)
}
```

### Available Operations

The library wraps all common MongoDB collection operations:

- **Find** - Query documents
- **FindOne** - Find a single document
- **InsertOne** - Insert a single document
- **InsertMany** - Insert multiple documents
- **UpdateOne** - Update a single document
- **UpdateMany** - Update multiple documents
- **DeleteOne** - Delete a single document
- **DeleteMany** - Delete multiple documents
- **ReplaceOne** - Replace a single document
- **CountDocuments** - Count documents matching a filter
- **Aggregate** - Run aggregation pipeline
- **FindOneAndUpdate** - Find and update a document
- **FindOneAndReplace** - Find and replace a document
- **FindOneAndDelete** - Find and delete a document
- **BulkWrite** - Perform bulk write operations
- **EstimatedDocumentCount** - Get estimated document count
- **Distinct** - Get distinct values for a field

### Setting Query Result Count

For `Find` operations, you can set the result count after processing:

```go
cursor, ctx, err := tracedCollection.Find(ctx, bson.M{"status": "active"})
if err != nil {
    panic(err)
}
defer cursor.Close(ctx)

var results []bson.M
if err = cursor.All(ctx, &results); err != nil {
    panic(err)
}

// Set the result count on the span
db.SetQueryResultCount(ctx, len(results))
```

## OpenTelemetry Integration

All operations automatically create OpenTelemetry spans with the following attributes:

- `db.system`: "mongodb"
- `db.name`: "{database}.{collection}"
- `db.database`: Database name
- `db.collection`: Collection name
- `db.operation`: Operation type (find, insertOne, etc.)
- `db.query.string`: MongoDB shell command representation

Additional attributes are added based on the operation type (e.g., `db.insert.id`, `db.update.matched_count`, etc.).

## Requirements

- Go 1.16 or higher
- MongoDB Go Driver
- OpenTelemetry Go SDK

## License

MIT

