# Changelog

All notable changes to the orkai-runiq project will be documented in this file.

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
