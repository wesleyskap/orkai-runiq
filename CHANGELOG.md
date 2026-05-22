# Changelog

All notable changes to the orkai-runiq project will be documented in this file.

## [2.8.0] - 2026-05-22

### Added
- **Native Delay Queue (`EnqueueWithDelay`)**: Added a convenient client helper to schedule jobs with a relative duration delay.
- **Batch Timeout (`WithBatchTimeout`)**: Support setting a maximum execution duration/timeout for a job batch (`BatchOption`), automatically failing expired batches.
- **Cron with Timezone (`WithCronLocation`)**: Added timezone/location support for recurring tasks (`CronOption`), allowing cron expressions to be evaluated under specific locales.

## [2.7.0] - 2026-05-22

### Added
- **Job Dependencies (DAGs)**: Introduced Directed Acyclic Graph (DAG) task dependency tracking. Jobs can register prerequisites via `DependsOn` and are enqueued as workflows via `client.EnqueueWorkflow()`.
- **Backend Storage Implementations**: Added transactional dependency tables and resolution logic on SQLite (`runiq_job_dependencies`), PostgreSQL (`runiq_job_dependencies`), and Redis (using dependency/dependent tracking sets).
- **Downstream Cascading Lifecycle**: Automatically cascades failures and cancellations downstream to block or cancel child jobs recursively if a parent task transitions to `dead` or is cancelled.
- **Workflow Verification Suite**: Integrated robust integration tests covering sequential execution, complex DAGs, cascading failure, and cascading cancellation.



## [2.6.0] - 2026-05-22

### Added
- **Job Search & Pagination**: Added job name, ID, and trace ID search filtering on the dashboard server. Job listings are now fully paginated.
- **Bulk Job Operations**: Introduced bulk actions to retry, cancel, or purge multiple selected jobs concurrently (`BulkRetry`, `BulkCancel`, `BulkPurge`) from the dashboard UI and REST endpoints.
- **Real-time Server-Sent Events (SSE)**: Replaced interval polling for dashboard statistics with a persistent Server-Sent Events (SSE) `/api/stats/stream` connection, reducing backend overhead.
- **Prometheus Metrics Exposer**: Added a `/metrics` HTTP endpoint displaying queue statuses, processed/failed/pending/running job counters, and queue paused status for Prometheus scraping.
- **CLI: Worker-Only Mode**: Added support for running the Runiq CLI in worker-only mode (`runiq worker ...` or `cfg.workerOnly`) which bypasses the dashboard HTTP server startup.

## [2.5.0] - 2026-05-21

### Added
- **Job Middleware Pipeline**: Added `pool.Use(mws ...func(queue.JobHandler) queue.JobHandler)` supporting middleware wrappers around job execution to ease logger, tracer, and telemetry interceptors insertion.
- **Event Hooks Integration**: Added event lifecycle hook framework `pool.OnEvent(eventType, handler)` alerting external listeners on job state changes (`JobEnqueued`, `JobCompleted`, `JobFailed`, `JobDead`).
- **DLQ Auto-Purge**: Introduced automatic expiration configuration for dead jobs (`WithDLQTTL(duration)`) and a background purging poller routine natively supported on SQLite, PostgreSQL, and Redis.
- **Storage Health Check**: Introduced `Ping(context.Context) error` on the `Pinger` interface (embedded in all storage definitions) to check connection status.
- **CLI: HTTPS & Basic Auth**: Added console flags (`--tls-cert`, `--tls-key`, `--basic-auth-user`, `--basic-auth-pass`) to easily serve the dashboard API over secure TLS and basic authentication.
- **Test Helpers Package**: Exposed mock and stub structures (`FakeClientStorage`, `FakeServerStorage`, `FakeWorkerPoolStorage`, `FakeLogger`, `FakeTracer`) inside the new `test/queuetest/` package to allow easy unit/integration testing for external consumers.

## [2.4.0] - 2026-05-21

### Added
- **Job Details Modal**: Interactive UI overlay displaying raw JSON-formatted job arguments and detailed error messages, with single-click clipboard copying.
- **Bulk DLQ Actions**: Support for retrying or purging all failed/dead letter queue jobs at once via `POST /api/jobs/failed/retry` and `POST /api/jobs/failed/purge` endpoints and corresponding dashboard buttons.
- **Cron Jobs Inspector**: A dedicated Dashboard tab that parses and displays all active registered cron schedules, target queues, and execution payloads.

## [2.3.0] - 2026-05-21

### Added
- **SQLite Storage Backend**: Implemented local file-based persistence support out-of-the-box (pure Go/CGO-free via `github.com/glebarez/go-sqlite`).
- **Command Line Interface (CLI)**: Added a compiled standalone binary (`cmd/runiq`) to easily launch worker pools and dashboard servers directly from the console.
- **Built-in Generic Job Handlers**: Integrated generic `ShellJob` (for executing command-line scripts) and `WebhookJob` (for outgoing HTTP POST/method payloads) directly inside the CLI.
- **SQLite Integration Tests**: Built `test/sqlite_test.go` covering full lifecycle parity (scheduling, retries, unique job locks, batches, rate limits, and pause/resume states).

## [2.2.0] - 2026-05-21

### Added
- **Worker Shutdown**: Added a WaitGroup-based shutdown mechanism to `WorkerPool` that halts polling and awaits completion of executing jobs upon context cancellation.
- **Shutdown Timeout Option**: Introduced `WithShutdownTimeout(timeout time.Duration)` worker option to configure maximum wait time (defaults to 10s).
- **Dynamic Queue Pause & Resume**: Added capability to pause and resume individual job queues dynamically at runtime without restarting workers.
- **Persistent Pause/Resume Storage**: Implemented storage of paused queue names using the `runiq_paused_queues` table in PostgreSQL and the `runiq:paused_queues` Set in Redis.
- **Queue Pause & Resume API**: Added `/api/queues/pause` and `/api/queues/resume` endpoints to the admin dashboard API.
- **Dashboard UI Actions**: Integrated interactive Pause/Resume buttons and status badges in the dashboard SPA's queues table.
- **Integration Tests**: Added `TestWorkerPoolShutdown`, `TestWorkerPoolQueuePause`, and `TestAdminPauseResumeEndpoints`.

## [2.1.0] - 2026-05-21

### Added
- **Pluggable Dashboard Middleware Support**: Added support for injecting arbitrary HTTP middlewares (e.g. Basic Auth, JWT, OAuth) into the dashboard `Server` using the `ServerOption` functional option pattern.
- **`WithMiddleware` functional option**: Added `WithMiddleware(mws ...func(http.Handler) http.Handler) ServerOption` to configure middlewares.
- **Middleware integration tests**: Added `TestDashboardWithMiddleware` to verify the execution, interception, and header-modifying behaviors of injected middlewares.

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
