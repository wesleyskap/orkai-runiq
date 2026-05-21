# Changelog

All notable changes to the orkai-runiq project will be documented in this file.

## [2.0.0] - 2026-05-20

### Changed
- **Breaking — Storage interface removed**: Monolithic `Storage` (19 methods) replaced by three exported consumer-specific interfaces — `WorkerPoolStorage`, `ClientStorage`, `ServerStorage` — each composed of only the sub-interfaces the consumer actually uses.
- **Breaking — Constructor signatures**: `NewWorkerPool` now accepts `WorkerPoolStorage`, `NewClient` accepts `ClientStorage`, and `NewServer` accepts `ServerStorage`. Existing callers passing the old `Storage` must switch to a narrower interface or concrete storage type.
- **Decomposed large files**: `postgres.go` (722→191 lines) and `redis.go` (789→123 lines) split into admin, process, and batch modules. All files now under 500 lines.
- **Extracted shared helpers**: `computeBackoffDelay()` and `generateJobID()` moved to `helpers.go`; `acquireUniqueLock()` eliminates duplicated unique-key lock logic across `Enqueue`/`EnqueueInBatch` in both backends; `handleBatchAck()` and `deleteUniqueLock()` reduce nesting in `Ack`/`Fail`/`Cancel`.
- **Decomposed `WorkerPool.Start`**: Extracted `startHeartbeat()` and `startScheduledPoller()` named methods.
- **Reordered `JobEnvelope` fields**: Largest to smallest to minimize struct padding.
- **Error messages now include offending values**: `ErrDuplicateJob` wraps lock key and existing job ID; `"job type not registered"` includes job name; process registration log includes process ID.

### Added
- **Unit tests for `computeBackoffDelay`**: Cap at 1h, exponential growth, non-negative invariant.

### Fixed
- **Unique-job assertions**: Updated to `errors.Is` for wrapped `ErrDuplicateJob` checks.

## [1.2.0] - 2026-05-20

### Added
- **Workflow Orchestration (Job Batches)**: Added support for grouping background tasks inside a dynamic execution group (`Batch`) that triggers a final callback job envelope upon successful completion of all constituent tasks.
- **Client Batch API**: Created `NewBatch` initiator, `Enqueue` batch task scheduler, and `Submit` sealer   ensuring race-condition immunity.
- **PostgreSQL Storage Batches**: Created the `runiq_batches` table, added a `batch_id` column to `runiq_jobs`, implemented counting/state updates in `Ack`, and handled fail-fast transitions to `'failed'` in `Fail`.
- **Redis Storage Batches**: Integrated atomic count tracking and state transitions using hashes (`runiq:batch:{batchID}`) and pipeline transaction updates.
- **Batch Test Coverage**: Added comprehensive integration and isolation tests verifying enqueuing, submission, execution, and DLQ failure behaviors.

## [1.1.0] - 2026-05-20

### Added
- **Global Rate Limiting & Max Concurrency Throttling**: Implemented global/cluster-wide per-job limits.
- **Storage driver additions**: Implemented `GetRunningJobsCount`, `CheckRateLimit` and `PostponeJob` in both PostgreSQL and Redis storage backends.
- **Sliding Window Rate-Limiter**: Implemented a precise sliding window rate limiter in Redis (using sorted sets) and PostgreSQL (using a transactional locks log table).
- **Non-blocking Postponement**: Jobs exceeding concurrency or rate limits are automatically postponed (via a 1-second scheduled delay) and worker pools continue to process other ready jobs.
- **Test suite additions**: Integrated unit tests for concurrency/rate limiting and integration assertions for database drivers.

## [1.0.0] - 2026-05-20

### Added
- **Dead Letter Queue (DLQ) & Poison Pill Inspector**: Jobs exceeding `max_attempts` now transition to `'dead'` status instead of `'failed'`.
- **Redis Storage Update**: Pushes permanently failed jobs to `runiq:dead:{queue}` list with LTrim capped at 50 elements.
- **Postgres Storage Update**: Sets the status field to `'dead'` inside `runiq_jobs`.
- **Dashboard UI SPA Enhancement**: Renamed tab, status card, and queue columns to "Dead (DLQ)", allowing error inspection and direct manual Retry/Cancel.
- **Testing Coverage**: Added unit/integration assertions checking the correct transition of exhausted jobs in both storage backends.

## [0.13.0] - 2026-05-20

### Added
- **Cron & Recurring Tasks (Fase 6)**: Added support for scheduling periodic jobs using standard 5-field cron spec expressions.
- **Distributed Cron Locks**: Implemented `LockCronExecution` inside both `PostgreSQL` (using a dedicated locks schema table with automated pruning) and `Redis` storage drivers to guarantee execution safety across multi-replica pool deployments.
- **WorkerPool Integration**: Added background cron scheduler loop to `WorkerPool.Start` evaluating registered entries and enqueuing them safely.
- **Unit and Integration Coverage**: Built MatchCron parser suite, lock collision tests, and matching logic simulations inside `queue/cron_test.go`.

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
