# Orkai Runiq

Orkai Runiq is a background job processor in Go. It is designed to be standalone with zero hard dependencies, while offering optional, interface-driven integration with telemetry and logging engines such as orkai-observability.

## Project Structure

* **queue/**: Contains the core interface declarations and payload structural definitions.
* **test/**: Contains the unit test suite verifying conformance.

## Core Abstractions

### queue/queue.go
* **TraceContext**: Struct encapsulating tracing correlation metadata (TraceID and SpanID).
* **JobEnvelope**: Envelope structure wrapping job parameters and metadata for storage.
* **Storage**: Interface defining the persistence engine operations (Enqueue, Dequeue, Ack, and Fail).
* **Job**: Interface with the Perform(ctx, args) signature which must be implemented by any background task.

### queue/postgres.go
* **PostgresStorage**: PostgreSQL driver implementing the Storage interface, utilizing FOR UPDATE SKIP LOCKED for concurrent dequeue safety and auto-creating schema tables.

### queue/redis.go
* **RedisStorage**: Redis driver implementing the Storage interface, utilizing pipelined list and hash operations.

### test/queue_test.go
* **TestJobEnvelopeSerialization**: Verifies JSON serialization and deserialization of job envelopes.
* **TestJobInterfaceConformance**: Verifies that job structs correctly implement the Perform signature.

### test/storage_test.go
* **TestPostgresStorageFlow**: Validates Postgres enqueuing, dequeuing, and concurrent isolation of tasks under high worker competition (SKIP LOCKED).
* **TestRedisStorageFlow**: Validates Redis atomic list/hash flow operations.

## Usage

### Initializing Storage Drivers

You can choose either PostgreSQL or Redis as the storage backend:

```go
package main

import (
	"context"
	"database/sql"
	"log"

	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
	"github.com/wesleyskap/orkai-runiq/queue"
)

func usePostgres(db *sql.DB) queue.Storage {
	storage, err := queue.NewPostgresStorage(db)
	if err != nil {
		log.Fatalf("failed to init postgres: %v", err)
	}
	return storage
}

func useRedis(client *redis.Client) queue.Storage {
	storage, err := queue.NewRedisStorage(client)
	if err != nil {
		log.Fatalf("failed to init redis: %v", err)
	}
	return storage
}
```

### Enqueuing and Dequeuing Manually

```go
func processJobs(ctx context.Context, storage queue.Storage) {
	// 1. Enqueue a job envelope
	envelope := &queue.JobEnvelope{
		JobID: "job-101",
		Queue: "default",
		Name:  "SendWelcomeEmail",
		Args:  []byte(`{"user_id": 42}`),
	}
	
	if err := storage.Enqueue(ctx, envelope); err != nil {
		log.Printf("failed to enqueue: %v", err)
	}

	// 2. Dequeue a job envelope
	job, err := storage.Dequeue(ctx, "default")
	if err != nil {
		log.Printf("failed to dequeue: %v", err)
	}
	
	if job != nil {
		log.Printf("dequeued job %s: %s", job.JobID, string(job.Args))
		// Acknowledge successful completion
		_ = storage.Ack(ctx, job.JobID)
	}
}
```

## Telemetry Integration

Runiq defines telemetry boundaries using simple, pluggable interfaces. By default, it falls back to Go standard library logging (slog/log) and skips metrics recording. Integration with external telemetry engines (like orkai-observability) can be enabled by supplying custom implementations of logging and tracing interfaces.

## Running Tests

```powershell
go test ./... -v
```

Expected output:
```text
=== RUN   TestJobEnvelopeSerialization
--- PASS: TestJobEnvelopeSerialization (0.00s)
=== RUN   TestJobInterfaceConformance
--- PASS: TestJobInterfaceConformance (0.00s)
=== RUN   TestPostgresStorageFlow
=== RUN   TestPostgresStorageFlow/EnqueueAndDequeue
=== RUN   TestPostgresStorageFlow/SkipLockedConcurrency
--- PASS: TestPostgresStorageFlow (0.09s)
    --- PASS: TestPostgresStorageFlow/EnqueueAndDequeue (0.01s)
    --- PASS: TestPostgresStorageFlow/SkipLockedConcurrency (0.02s)
=== RUN   TestRedisStorageFlow
=== RUN   TestRedisStorageFlow/EnqueueAndDequeue
--- PASS: TestRedisStorageFlow (0.01s)
    --- PASS: TestRedisStorageFlow/EnqueueAndDequeue (0.00s)
PASS
ok  	github.com/wesleyskap/orkai-runiq/test	0.589s
```

