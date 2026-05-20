# Changelog

All notable changes to the orkai-runiq project will be documented in this file.

## [0.12.0] - 2026-05-20

### Added
- **EnqueueUnique client helper**: Added `EnqueueUnique` method to `queue.Client` to simplify scheduling unique jobs with custom lock keys and expiration.
- Downgraded Go version constraint to `1.22` and aligned OpenTelemetry to `v1.30.0` (SDK `v1.27.0`) and `golang.org/x/sys` to `v0.20.0` for full Go 1.22.0 compatibility.

## [0.11.0] - 2026-05-19

### Added
- **Weighted Queues**: Queue prioritization support in `WorkerPool` using relative weights (via `WithQueueWeights`). Cycles search preference dynamically to prevent lower priority queue starvation.
- **Unit Test Coverage**: Added tests verifying search order rotation with custom weights and fallback to strict linear sequence.

## [0.10.0] - 2026-05-19

### Added
- **Unique Jobs**: Uniqueness lock support in PostgreSQL and Redis storage backends using `UniqueKey` and `UniqueTTL` on job envelopes.
- **Active Worker Pool Monitoring**: Background worker pool process registration and heartbeat mechanism updating every 5 seconds.
- **Active Processes Panel**: Integrated processes dashboard panel in `index.html` to list active worker nodes, their concurrency limits, and monitored queues.
- **Unit Test Coverage**: Added tests for process registration, worker heartbeats, unique lock validations, and OTel metric generation.

## [0.9.0] - 2026-05-19

### Added
- Dashboard administrative endpoints (`POST /api/jobs/retry`, `POST /api/jobs/cancel`, `POST /api/queues/clear`).
- Interactive Action buttons in the Dashboard UI for retrying failed jobs, canceling queued jobs, and clearing queues.
- Extended `Storage` interface and implemented database methods (`Retry`, `Cancel`, `ClearQueue`) in PostgreSQL and Redis storage drivers.
- Automated API server and storage driver test suites in `test/server_test.go` and `test/storage_test.go`.

## [0.8.0] - 2026-05-19

### Added
- New client helper methods `EnqueueIn` (executes job after delay) and `EnqueueAt` (executes job at a specific timestamp).
- Test coverage for client scheduling helpers in `test/worker_test.go`.

## [0.7.0] - 2026-05-19

### Added
- Automatic job retries with exponential backoff and deterministic jitter.
- Integration of `MaxAttempts` and `Attempts` counters in `JobEnvelope` structure (defaulting to 3 max attempts).
- Scheduled queue execution support (`run_at` parameter) in both PostgreSQL and Redis drivers.
- Background scheduled job poller loop (`PollScheduled`) in `WorkerPool` running every 1 second.
- Coverage verification tests in `test/storage_test.go` checking retry and scheduling states under both drivers.

## [0.6.0] - 2026-05-19

### Added
- Track active, failed, and successfully processed jobs dynamically in Redis and PostgreSQL drivers.
- Redesigned UI Dashboard with modern glassmorphic tabbed list navigation (Pending, Active, Processed, Failed).
- Custom 5-second polling rate for real-time dashboard UI updates.
- Extended JSON statistics API to return detailed job list metadata.


## [0.5.0] - 2026-05-19

### Added
- Track active and failed jobs dynamically in Redis driver using Sets to provide full dashboard stats alignment.

## [0.4.0] - 2026-05-19

### Added
- Native HTTP dashboard server (`queue.Server`) to expose statistics API and serve embedded assets.
- Embedded Single Page Application (SPA) dashboard UI in `queue/assets/index.html` featuring a modern glassmorphism dark mode.
- Storage stats aggregation support (`queue.Storage.GetStats`) implemented for PostgreSQL and Redis drivers.
- Conformance test coverage in `test/server_test.go` checking `/api/stats` JSON data and `/` embedded UI page rendering.

## [0.3.0] - 2026-05-19

### Added
- Client driver (`queue.Client`) supporting functional options, dynamic job enqueuing, and automatic Trace ID extraction.
- Worker Pool processor (`queue.WorkerPool`) utilizing a buffered channel semaphore for concurrency limits, trace context restoration, and robust panic recovery.
- Pluggable telemetry boundaries (`queue.Logger` and `queue.Tracer` interfaces) with fallback No-Op implementations.
- Test coverage in `test/worker_test.go` verifying Trace extraction, Worker execution, context propagation, and panic resilience.

## [0.2.0] - 2026-05-19

### Added
- PostgreSQL storage driver (`queue.PostgresStorage`) with auto-migration support and concurrent task retrieval utilizing SQL `FOR UPDATE SKIP LOCKED`.
- Redis storage driver (`queue.RedisStorage`) with pipelined atomic enqueues and list/hash operations.
- Test coverage in `test/storage_test.go` checking basic flow and concurrent `SKIP LOCKED` isolation under high worker competition.

## [0.1.0] - 2026-05-19

### Added
- Initialization of the Go module `orkai-runiq`.
- Setup of `/queue` and `/test` directories to separate logic from testing suites.
- Core declarations of `Storage` and `Job` interfaces in `queue/queue.go`.
- Structural definitions of `JobEnvelope` and `TraceContext` in `queue/queue.go`.
- Initial unit test suite in `test/queue_test.go` to validate JSON serialization and interface conformance.
