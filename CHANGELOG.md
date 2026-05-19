# Changelog

All notable changes to the orkai-runiq project will be documented in this file.

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
