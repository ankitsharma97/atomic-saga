# Changelog

## 2.0.0

Breaking release. See "Migrating from 1.x" in the README.

### Added
- Crash recovery for sagas: executions are owned by a worker, heartbeat while running, and are claimed and finished by `recover()` / `startRecovery()` when their worker disappears. Writes are fenced by owner (`LeaseLostError`).
- Per-saga recovery strategy (`resume` or `compensate`).
- `StepMeta` for actions and compensations: stable `idempotencyKey`, `AbortSignal` (timeout / lease loss), outputs of earlier steps.
- Compensation retries, `COMPENSATION_FAILED` status and `onCompensationFailed` hook.
- `NonRetryableError`, `retryable()` and `maxBackoffMs` in retry policies.
- Idempotency middleware stores and replays responses, rejects key reuse with a different request (422), returns 409 while in progress, releases the key on 5xx or dropped connections, supports scoping, and works with plain `http`.
- Transactional outbox writes events in the caller's transaction (`add(event, tx)`), claims batches safely across publishers, retries with backoff, dead-letters after `maxAttempts`, and never overlaps polls.
- PostgreSQL stores (`PostgresSagaStore`, `PostgresIdempotencyStore`, `PostgresOutboxStore`), `migrate`, `schemaSql`, `withTransaction`.
- In-memory stores (`InMemorySagaStore`, `InMemoryIdempotencyStore`, `InMemoryOutboxStore`).
- Test suite covering failure paths, recovery and concurrency, including Postgres integration tests.

### Fixed
- A failed compensation no longer reports the saga as `COMPENSATED`.
- A throwing `onSuccess` hook no longer marks a completed saga as `FAILED`.
- Timed-out attempts are aborted via `AbortSignal` instead of silently continuing.
- The package no longer depends on itself, and no longer ships the design PDF, sources and tests.

### Removed
- `OutboxTransaction`, `utils`, the `@Idempotent` decorator, and the `joi`, `winston`, `uuid` and `express` dependencies. Node.js 18+ is required.
