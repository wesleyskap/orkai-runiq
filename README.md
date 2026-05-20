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
* **PostgresStorage**: PostgreSQL driver implementing the Storage interface, utilizing FOR UPDATE SKIP LOCKED for concurrent dequeue safety, auto-creating schema tables, tracking `run_at` scheduled times, and calculating job stats (Pending, Active, Processed, and Failed).

### queue/redis.go
* **RedisStorage**: Redis driver implementing the Storage interface, utilizing pipelined list and hash operations, ZSets for future `run_at` schedules, and tracking queue stats (Pending, Active, Processed, and Failed) using dedicated Redis Sets and Lists.

### queue/client.go
* **Client**: Client helper for enqueuing jobs with transparent Trace ID propagation.

### queue/worker.go
* **WorkerPool**: Concurrent job processor utilizing buffered channel semaphores, context/trace restoration, and panic recovery.

### queue/server.go
* **Server**: Native Go HTTP server displaying an embedded real-time HTML/CSS dashboard (with tabbed logs for Pending, Active, Processed, and Failed states updating every 5 seconds) and serving statistics in JSON format.

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

## Job Retries & Scheduling

By default, jobs are executed up to **3 times** with an **exponential backoff delay** (e.g., 10s, 20s, 40s, up to 1 hour) and deterministic jitter if they return an error.

You can customize this behavior by setting `MaxAttempts` and `RunAt` fields on a `JobEnvelope` when enqueuing:

```go
runAt := time.Now().Add(5 * time.Minute)
envelope := &queue.JobEnvelope{
	JobID:       "custom-job-id",
	Queue:       "default",
	Name:        "SendEmail",
	Args:        []byte(`{"to":"user@example.com"}`),
	MaxAttempts: 5,
	RunAt:       &runAt,
}
err := storage.Enqueue(ctx, envelope)
```

Additionally, you can use the client helpers `EnqueueIn` (to execute a task after a duration) and `EnqueueAt` (to execute a task at a specific time):

```go
// Enqueue job to run in 10 minutes
err := client.EnqueueIn(ctx, "default", "SendEmail", []byte(`{"to":"user@example.com"}`), 10*time.Minute)

// Enqueue job to run at a specific timestamp
targetTime := time.Now().Add(2 * time.Hour)
err = client.EnqueueAt(ctx, "default", "SendEmail", []byte(`{"to":"user@example.com"}`), targetTime)
```

## Unique Jobs

Runiq supports unique jobs to prevent enqueuing duplicate tasks while another instance is still pending or processing. You can set the `UniqueKey` and `UniqueTTL` fields:

```go
envelope := &queue.JobEnvelope{
	JobID:     "unique-job-123",
	Queue:     "default",
	Name:      "SendEmail",
	Args:      []byte(`{"to":"user@example.com"}`),
	UniqueKey: "user-email-123",            // The uniqueness lock key
	UniqueTTL: 10 * time.Minute,             // Uniqueness lock duration
}
err := storage.Enqueue(ctx, envelope) // Returns queue.ErrDuplicateJob if a lock already exists

// Or using the client helper:
err = client.EnqueueUnique(ctx, "default", "SendEmail", []byte(`{"to":"user@example.com"}`), "user-email-123", 10*time.Minute)
```

Locks are automatically released when the job completes successfully (`Ack`), fails permanently (exceeds maximum attempts), or is explicitly cancelled via the dashboard or API.

## Active Worker Pool Monitoring

When a `WorkerPool` starts, it automatically registers itself with the storage driver using a unique process identifier (comprising the hostname, PID, and a random token). The worker pool then maintains a periodic background heartbeat ticker (every 5 seconds) to signal its health.

The dashboard UI automatically aggregates these worker heartbeats and renders them in the **Active Processes (Workers)** panel, listing all active processes, their concurrency configurations, and their monitored queues. Dead workers are automatically pruned after 15 seconds of inactivity.

## Weighted Queues

By default, workers poll queues in strict linear priority (the order in which queues are passed to `Start`). To avoid starvation of lower priority queues, Runiq allows you to configure relative weights using the `WithQueueWeights` option:

```go
pool := queue.NewWorkerPool(
	storage, 
	5, 
	queue.WithQueueWeights(map[string]int{
		"critical": 3,
		"default":  1,
	}),
)
```

In the example above, `critical` has a weight of 3 and `default` has a weight of 1. During job fetches, the worker pool cycles search preference (yielding a 3:1 ratio of preference), ensuring that the `default` queue is checked first 25% of the time, while still polling all monitored queues to guarantee zero throughput lag.

## Administration API & Dashboard Actions

Runiq's dashboard contains interactive buttons to manage tasks directly. The server exposes the following endpoints:

* **Retry Job**: `POST /api/jobs/retry?id=<job_id>` resets the attempts counter and schedules a failed job for immediate retry.
* **Cancel Job**: `POST /api/jobs/cancel?id=<job_id>` deletes a pending, scheduled, or failed job from the queue.
* **Clear Queue**: `POST /api/queues/clear?name=<queue_name>` removes all pending, active, scheduled, completed, and failed jobs from a specific queue.

## Telemetry Integration

Runiq defines telemetry boundaries using simple, pluggable interfaces. By default, it falls back to Go standard library logging (slog/log) and skips metrics recording. Integration with external telemetry engines (like orkai-observability) can be enabled by supplying custom implementations of logging and tracing interfaces.

## Running Tests

```powershell
go test ./... -v
```

Expected output:
```text
=== RUN   TestWorkerPoolWeightedRotation
--- PASS: TestWorkerPoolWeightedRotation (0.00s)
=== RUN   TestWorkerPoolStrictPriorityFallback
--- PASS: TestWorkerPoolStrictPriorityFallback (0.00s)
PASS
ok  	github.com/wesleyskap/orkai-runiq/queue	0.386s
=== RUN   TestJobEnvelopeSerialization
--- PASS: TestJobEnvelopeSerialization (0.00s)
=== RUN   TestJobInterfaceConformance
--- PASS: TestJobInterfaceConformance (0.00s)
=== RUN   TestDashboardStatsEndpoint
--- PASS: TestDashboardStatsEndpoint (0.00s)
=== RUN   TestDashboardUIEndpoint
--- PASS: TestDashboardUIEndpoint (0.03s)
=== RUN   TestAdminEndpoints
--- PASS: TestAdminEndpoints (0.00s)
=== RUN   TestClientTraceExtraction
--- PASS: TestClientTraceExtraction (0.00s)
=== RUN   TestClientEnqueueUnique
--- PASS: TestClientEnqueueUnique (0.00s)
=== RUN   TestWorkerPoolExecution
--- PASS: TestWorkerPoolExecution (0.10s)
=== RUN   TestWorkerPoolPanicRecovery
--- PASS: TestWorkerPoolPanicRecovery (0.10s)
=== RUN   TestClientScheduling
--- PASS: TestClientScheduling (0.00s)
=== RUN   TestWorkerProcessRegistration
--- PASS: TestWorkerProcessRegistration (0.05s)
PASS
ok  	github.com/wesleyskap/orkai-runiq/test	2.589s
```

