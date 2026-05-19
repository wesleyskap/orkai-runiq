# Orkai Runiq

Orkai Runiq is a background job processor in Go. It is designed to be standalone with zero hard dependencies, while offering optional, interface-driven integration with telemetry and logging engines such as orkai-observability.

## Project Structure

* **queue/**: Contains the core interface declarations and payload structural definitions.
* **test/**: Contains the unit test suite verifying conformance.

## Core Abstractions

### queue/queue.go
* **TraceContext**: Struct encapsulating tracing correlation metadata (TraceID and SpanID).
* **JobEnvelope**: Envelope structure wrapping job parameters and metadata for storage.
* **Storage**: Interface defining the persistence engine operations (Enqueue, Dequeue, Ack, Fail, and GetStats).
* **Job**: Interface with the Perform(ctx, args) signature which must be implemented by any background task.

### queue/postgres.go
* **PostgresStorage**: PostgreSQL driver implementing the Storage interface, utilizing FOR UPDATE SKIP LOCKED for concurrent dequeue safety, auto-creating schema tables, and calculating job stats.

### queue/redis.go
* **RedisStorage**: Redis driver implementing the Storage interface, utilizing pipelined list and hash operations, and tracking queue stats.

### queue/client.go
* **Client**: Client helper for enqueuing jobs with transparent Trace ID propagation.

### queue/worker.go
* **WorkerPool**: Concurrent job processor utilizing buffered channel semaphores, context/trace restoration, and panic recovery.

### queue/server.go
* **Server**: Native Go HTTP server displaying an embedded real-time HTML/CSS dashboard and serving statistics in JSON format.

### test/queue_test.go
* **TestJobEnvelopeSerialization**: Verifies JSON serialization and deserialization of job envelopes.
* **TestJobInterfaceConformance**: Verifies that job structs correctly implement the Perform signature.

### test/storage_test.go
* **TestPostgresStorageFlow**: Validates Postgres enqueuing, dequeuing, and concurrent isolation of tasks under high worker competition (SKIP LOCKED).
* **TestRedisStorageFlow**: Validates Redis atomic list/hash flow operations.

### test/worker_test.go
* **TestClientTraceExtraction**: Verifies Client enqueues jobs with propagated trace details.
* **TestWorkerPoolExecution**: Validates worker startup, context injection, and success telemetry.
* **TestWorkerPoolPanicRecovery**: Validates that worker handles panics without crashing and marks storage failures.

### test/server_test.go
* **TestDashboardStatsEndpoint**: Verifies that the JSON API endpoint returns correct aggregations.
* **TestDashboardUIEndpoint**: Assures the embedded dashboard template page is loaded and served.

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

### Enqueuing and Processing with WorkerPool

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/wesleyskap/orkai-runiq/queue"
)

// 1. Define a job implementing queue.Job
type SendEmailJob struct{}

func (s *SendEmailJob) Perform(ctx context.Context, args []byte) error {
	fmt.Printf("Sending email with args: %s\n", string(args))
	return nil
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 2. Setup your storage (Postgres or Redis)
	// storage := usePostgres(db)
	
	// 3. Initialize WorkerPool and register jobs
	pool := queue.NewWorkerPool(storage, 3)
	pool.Register("SendEmail", &SendEmailJob{})
	
	go func() {
		if err := pool.Start(ctx, "default"); err != nil {
			log.Printf("Worker pool stopped: %v", err)
		}
	}()

	// 4. Initialize Client and enqueue jobs
	client := queue.NewClient(storage)
	err := client.Enqueue(ctx, "default", "SendEmail", []byte(`{"to":"user@example.com"}`))
	if err != nil {
		log.Fatalf("failed to enqueue: %v", err)
	}

	// 5. Initialize and Start Dashboard Server
	server := queue.NewServer(storage, ":8080")
	go func() {
		if err := server.Start(); err != nil {
			log.Printf("Dashboard server stopped: %v", err)
		}
	}()

	time.Sleep(500 * time.Millisecond) // Wait for worker to consume
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
=== RUN   TestDashboardStatsEndpoint
--- PASS: TestDashboardStatsEndpoint (0.00s)
=== RUN   TestDashboardUIEndpoint
--- PASS: TestDashboardUIEndpoint (0.03s)
=== RUN   TestPostgresStorageFlow
=== RUN   TestPostgresStorageFlow/EnqueueAndDequeue
=== RUN   TestPostgresStorageFlow/SkipLockedConcurrency
--- PASS: TestPostgresStorageFlow (0.05s)
    --- PASS: TestPostgresStorageFlow/EnqueueAndDequeue (0.00s)
    --- PASS: TestPostgresStorageFlow/SkipLockedConcurrency (0.02s)
=== RUN   TestRedisStorageFlow
=== RUN   TestRedisStorageFlow/EnqueueAndDequeue
--- PASS: TestRedisStorageFlow (0.02s)
    --- PASS: TestRedisStorageFlow/EnqueueAndDequeue (0.00s)
=== RUN   TestClientTraceExtraction
--- PASS: TestClientTraceExtraction (0.00s)
=== RUN   TestWorkerPoolExecution
--- PASS: TestWorkerPoolExecution (0.10s)
=== RUN   TestWorkerPoolPanicRecovery
--- PASS: TestWorkerPoolPanicRecovery (0.10s)
PASS
ok  	github.com/wesleyskap/orkai-runiq/test	0.837s
```

