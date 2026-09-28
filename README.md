# atomic-saga

**All-or-nothing operations for distributed Node.js apps — on the Postgres you already have.**

One API request often has to change several systems: charge a card, reserve stock, write an order, emit an event. There is no database transaction spanning all of them, so a failure halfway leaves them disagreeing. `atomic-saga` gives you the three standard tools for this, built to survive the failure cases, with no extra infrastructure:

| Problem | Tool |
|---|---|
| A later step fails after earlier ones succeeded | **`SagaOrchestrator`** — runs steps in order; on failure runs each completed step's compensation in reverse. Persists every transition, and **recovers executions interrupted by crashes** on any worker. |
| A client retries and the work runs twice | **`IdempotencyMiddleware`** — Stripe-style `Idempotency-Key` handling: the stored response is **replayed**, concurrent duplicates get 409, reusing a key for a different request gets 422. |
| The DB write commits but the event is lost (or vice versa) | **`TransactionalOutbox`** — the event is written **in your database transaction**; a background publisher delivers it with retries. |

Stores for **PostgreSQL** (multi-worker safe, `FOR UPDATE SKIP LOCKED`) and **in-memory** (tests, single process) are included. Zero runtime dependencies.

```bash
npm install atomic-saga pg
```

Requires Node.js 18+. `pg` is only needed for the Postgres stores.

---

## Contents

- [Quick start](#quick-start)
- [Sagas](#sagas)
- [Idempotent endpoints](#idempotent-endpoints)
- [Transactional outbox](#transactional-outbox)
- [Stores](#stores)
- [Guarantees and limits](#guarantees-and-limits)
- [API reference](#api-reference)
- [Migrating from 1.x](#migrating-from-1x)

## Quick start

```ts
import { Pool } from 'pg';
import { SagaOrchestrator, PostgresSagaStore, migrate, NonRetryableError } from 'atomic-saga';

const pool = new Pool({ connectionString: process.env.DATABASE_URL });
await migrate(pool); // creates saga_executions, idempotency_keys, outbox_events (idempotent)

const checkout = {
  id: 'checkout',
  name: 'Checkout',
  steps: [
    {
      id: 'reserve-stock',
      name: 'Reserve stock',
      action: (ctx, meta) => inventory.reserve(ctx.sku, ctx.qty, { idempotencyKey: meta.idempotencyKey }),
      compensation: (ctx, reservation) => inventory.release(reservation.id)
    },
    {
      id: 'charge-card',
      name: 'Charge card',
      action: async (ctx, meta) => {
        const charge = await payments.charge(ctx.amount, { idempotencyKey: meta.idempotencyKey });
        if (charge.declined) throw new NonRetryableError('Card declined'); // fail now, don't retry
        return charge;
      },
      compensation: (ctx, charge) => payments.refund(charge.id)
    },
    {
      id: 'create-order',
      name: 'Create order',
      action: (ctx, meta) => orders.create(ctx.orderId, meta.results['charge-card'])
    }
  ]
};

const sagas = new SagaOrchestrator(new PostgresSagaStore(pool));
sagas.register(checkout);
sagas.startRecovery(); // finish executions left behind by crashed workers

const execution = await sagas.executeSaga(checkout, { orderId: 'o1', sku: 'sku-1', qty: 2, amount: 50 });
execution.status; // 'COMPLETED' | 'COMPENSATED' | 'COMPENSATION_FAILED'
```

A complete service — Express endpoint, idempotency, saga and outbox, all on Postgres — is in [`examples/checkout.ts`](examples/checkout.ts).

## Sagas

### Steps

```ts
interface TransactionStep<TContext, TOutput> {
  id: string;
  name: string;
  action(context: TContext, meta: StepMeta): Promise<TOutput>;
  compensation?(context: TContext, output: TOutput | undefined, meta: StepMeta): Promise<void>;
  retryPolicy?: RetryPolicy;             // default: 3 attempts, 1s backoff x2
  compensationRetryPolicy?: RetryPolicy; // default: retryPolicy
  timeout?: number;                      // per attempt, default 30s
}
```

Every action and compensation receives `meta`:

| Field | Use |
|---|---|
| `idempotencyKey` | Stable per execution and step (`<executionId>:<stepId>`, `…:compensate` for compensations). **Pass it to downstream APIs** so a retried or recovered step does not repeat its side effect. |
| `signal` | An `AbortSignal` aborted on timeout or when this worker loses ownership. Pass it to `fetch`, database drivers, etc. |
| `results` | Outputs of the steps completed so far, keyed by step id. |
| `attempt`, `executionId`, `sagaId`, `stepId` | For logging and tracing. |

### What happens on failure

1. The failing step is retried per its `retryPolicy`. Throw `NonRetryableError` (or use `retryPolicy.retryable`) to fail immediately.
2. Compensations of the **completed** steps run in reverse order, each with its own retries. The failed step itself is not compensated. If it may have had a partial effect, make it clean up after itself.
3. The saga ends as:
   - `COMPENSATED` — everything was undone; `onFailure` is called.
   - `COMPENSATION_FAILED` — at least one compensation kept failing. The other compensations still ran. The failing step is marked `COMPENSATION_FAILED` with its `compensationError`; `onCompensationFailed` is called. **This needs a human or a repair job** — it is never reported as success.

`executeSaga` resolves with the execution in all of these cases. It only rejects on infrastructure errors (store unavailable, ownership lost); the execution is then left active and `recover()` finishes it.

Hooks (`onSuccess`, `onFailure`, `onCompensationFailed`) run after the outcome is persisted; if a hook throws, it is logged and the outcome is unchanged.

### Crash recovery

Every state change is persisted before the next one begins, and a running execution heartbeats. If a process dies (deploy, OOM, crash), its executions stop heartbeating; after `staleAfterMs` (default 60s) any worker's `recover()` can claim them — exactly one worker wins each claim — and drive them to a terminal state:

- **Interrupted while running steps** — by default (`recovery: 'resume'`) the interrupted step is re-run and the saga continues. This is why steps should be idempotent (use `meta.idempotencyKey`). Set `recovery: 'compensate'` on the definition to undo the completed steps instead.
- **Interrupted while compensating** — the remaining compensations run; finished ones are not repeated.
- **Definition changed since the execution started** (step ids no longer match) — the execution is compensated.

A worker that was only paused (long GC, network partition) and finds its execution claimed by another stops at the next heartbeat: its step `signal` is aborted and `executeSaga` rejects with `LeaseLostError`. Writes are fenced by owner, so it cannot overwrite the new owner's progress.

```ts
const sagas = new SagaOrchestrator(store, {
  staleAfterMs: 60_000,      // silence before an execution is considered abandoned
  heartbeatIntervalMs: 20_000,
  workerId: process.env.HOSTNAME
});
sagas.register(checkout);    // a fresh process must register definitions before recovering them
sagas.startRecovery();       // or call sagas.recover() from your own scheduler
// on shutdown:
await sagas.stopRecovery();
```

Context and step outputs are stored as JSON, so they must be JSON-serializable.

## Idempotent endpoints

```ts
import express from 'express';
import { IdempotencyMiddleware, PostgresIdempotencyStore } from 'atomic-saga';

const app = express();
app.use(express.json()); // before the middleware: the body is part of the request fingerprint
app.use(
  new IdempotencyMiddleware(new PostgresIdempotencyStore(pool), {
    required: false,                 // true: 400 when the header is missing
    scope: req => req.user?.id,      // keys are per user
    lockMs: 60_000,                  // longer than your slowest request
    ttlMs: 24 * 60 * 60 * 1000
  }).middleware()
);
```

Clients send `Idempotency-Key: <uuid>` on POST/PATCH (configurable via `methods`). Then:

| Situation | Response |
|---|---|
| First request with the key | Runs normally. The response (status, headers, body) is stored **before** it is sent. |
| Retry, same request | The stored response, with `Idempotent-Replayed: true`. The handler does not run. |
| Retry while the first is still running | `409` with `Retry-After`. |
| Same key, different method, path or body | `422 idempotency_key_reused`. |
| Handler responded `5xx`, or the connection dropped | Key released: the client can retry. (Customize with `shouldStore`.) |
| Store unreachable | `503`, or pass-through with `failOpen: true`. |

`Set-Cookie` and hop-by-hop headers are not replayed. Responses larger than `maxBodyBytes` (1 MiB) are not stored. The middleware uses only Node's `http` types, so it works with Express, Connect, or plain `http.createServer`.

Clean up expired keys periodically with `store.deleteExpired()`.

## Transactional outbox

Write the event in the same transaction as the change it describes:

```ts
import { TransactionalOutbox, PostgresOutboxStore, withTransaction } from 'atomic-saga';

const outbox = new TransactionalOutbox(new PostgresOutboxStore(pool), {
  publish: (topic, message) => kafka.send({ topic, messages: [{ key: message.eventId, value: JSON.stringify(message) }] })
});

await withTransaction(pool, async tx => {
  await tx.query('UPDATE orders SET status = $1 WHERE id = $2', ['paid', orderId]);
  await outbox.add({ eventType: 'OrderPaid', payload: { orderId } }, tx); // commits or rolls back with the UPDATE
});

outbox.start(); // background publisher
```

- Topics default to the event type in dot case (`OrderPaid` → `order.paid`); override per event with `topic` or globally with `topicFor`.
- Several publishers can run at once; each event is claimed by one of them (`lockMs`, default 30s).
- Failed publishes are retried with exponential backoff (default 10 attempts, up to 5 min apart), then marked `FAILED`. `PostgresOutboxStore#requeueFailed()` puts them back.
- Pass your own `id` to make `add` idempotent.
- `outbox.getStats()` returns pending / published / failed counts.

Delivery is **at-least-once**: if the process dies after the broker accepted a message but before it was marked published, it is sent again. Consumers should dedupe on `eventId`.

## Stores

| | Saga | Idempotency | Outbox |
|---|---|---|---|
| PostgreSQL | `PostgresSagaStore` | `PostgresIdempotencyStore` | `PostgresOutboxStore` |
| In-memory | `InMemorySagaStore` | `InMemoryIdempotencyStore` | `InMemoryOutboxStore` |

**PostgreSQL** (9.5+): run `migrate(pool)` on startup, or get the DDL from `schemaSql()` for your migration tool. Table names are configurable (`migrate(pool, { outboxEvents: 'app.outbox' })` and `new PostgresOutboxStore(pool, { tableName: 'app.outbox' })`). Leases use the database clock, so app-server clock drift does not matter.

**In-memory**: for tests and single-process apps. State is lost on restart, so there is no cross-process recovery. Values are round-tripped through JSON to behave like the real stores.

**Your own database**: implement `SagaStore`, `IdempotencyStore` or `OutboxStore<TTx>` (see [`src/types`](src/types/index.ts)). The contracts that matter: `updateExecution`/`heartbeat` must be fenced by owner, and `claimStaleExecutions`, `begin` and `claimBatch` must be atomic across processes. The Postgres implementations are a reference.

## Guarantees and limits

What you get:

- A saga either completes, or all completed steps' compensations run — including after a crash, as long as some worker runs `recover()`.
- A failed compensation is surfaced as `COMPENSATION_FAILED`, never hidden.
- An idempotency key runs its handler at most once while the stored response is retained (`ttlMs`), provided the request finishes within `lockMs`.
- An outbox event is published if and only if its transaction committed (at least once).

What you need to do:

- **Make steps and compensations idempotent**, using `meta.idempotencyKey` with downstream APIs. Retries and recovery re-run them; that's what makes the guarantees possible.
- **Treat compensations as business operations** (a refund, a release), not database rollbacks. Other systems may see intermediate states — sagas give eventual consistency, not isolation.
- **Honor `meta.signal`** in long steps. A timed-out attempt that ignores it keeps running in the background while the retry starts.
- **Alert on `COMPENSATION_FAILED`** and on outbox `FAILED` counts.

When to use something else: if you need long-running workflows (days, human approvals, timers), fan-out/fan-in, or versioned workflow code, use a durable-execution engine such as Temporal, Restate or Inngest. `atomic-saga` is for request-scoped, multi-step operations where adding infrastructure isn't worth it.

## API reference

### `new SagaOrchestrator(store, options?)`

| Option | Default | |
|---|---|---|
| `logger` | silent | `{ info, warn, error, debug }`; `ConsoleLogger` is included |
| `defaultRetryPolicy` | `{ maxAttempts: 3, backoffMs: 1000, backoffMultiplier: 2 }` | also `maxBackoffMs`, `retryable(error)` |
| `defaultTimeout` | `30000` | per attempt, ms |
| `staleAfterMs` | `60000` | |
| `heartbeatIntervalMs` | `staleAfterMs / 3` | |
| `workerId` | `<hostname>:<pid>:<random>` | |

Methods: `register(definition)`, `executeSaga(definition, context, { executionId? })`, `recover({ limit? })`, `startRecovery(intervalMs?)`, `stopRecovery()`, `getExecution(id)`, `listExecutions({ sagaId?, status?, limit? })`.

### `new IdempotencyMiddleware(store, options?)`

Options: `header` (`Idempotency-Key`), `methods` (`['POST', 'PATCH']`), `required` (`false`), `ttlMs` (24h), `lockMs` (60s), `scope(req)`, `shouldStore(status)` (`status < 500`), `maxBodyBytes` (1 MiB), `failOpen` (`false`), `logger`. Methods: `middleware()`, `generateKey()`.

### `new TransactionalOutbox(store, broker, options?)`

Options: `pollIntervalMs` (1000), `batchSize` (100), `lockMs` (30000), `retryPolicy` (10 attempts, 1s ×2, max 5 min), `topicFor(eventType)`, `logger`. Methods: `add(event, tx?)`, `start()`, `stop()`, `processNow()`, `getStats()`.

### `AtomicApiOperations`

A small facade that wires the three together:

```ts
const atomic = new AtomicApiOperations({ sagaStore, idempotencyStore, outboxStore, messageBroker });
app.use(atomic.idempotencyMiddleware());
atomic.register(checkout).start();   // recovery + outbox publisher
await atomic.executeSaga(checkout, ctx);
await atomic.stop();
```

### Errors

`NonRetryableError` (throw from a step to skip retries), `StepTimeoutError`, `LeaseLostError`.

## Migrating from 1.x

2.0 is a breaking release. The 1.x idempotency middleware and outbox did not provide the guarantees they described (duplicates got a 409 instead of the original response; the outbox did not write inside the caller's transaction), and their store interfaces could not express the fix.

- **Constructors take an options object**: `new SagaOrchestrator(store, { logger, defaultRetryPolicy, defaultTimeout })`, `new IdempotencyMiddleware(store, { logger, header })`, `new TransactionalOutbox(store, broker, { logger, pollIntervalMs })`.
- **Step signatures** gained a `meta` argument; compensations receive the step's **output** as the second argument (as before) and the saga context as the first. Existing `action(context)` functions keep working.
- **New statuses**: `COMPENSATION_FAILED` for sagas and steps. `execution.error` / `stepResult.error` are plain `{ name, message, stack }` objects.
- **`listExecutions(sagaId, status)`** → `listExecutions({ sagaId, status })`.
- **Store interfaces changed.** Use the bundled stores, or see [Stores](#stores) for the new contracts: `SagaStore` gained `heartbeat` and `claimStaleExecutions`, and `updateExecution` returns whether the write happened; `IdempotencyStore` is now `begin` / `complete` / `release`; `OutboxStore` is now `saveEvent(event, tx)` / `claimBatch` / `markPublished` / `markRetry` / `markFailed` / `getStats`.
- **Idempotency header** defaults to `Idempotency-Key` (was `X-Idempotency-Key`); pass `{ header: 'X-Idempotency-Key' }` to keep the old one.
- **Outbox**: `storeEvent(sagaId, stepId, type, payload)` → `add({ eventType, payload, metadata }, tx)`. `OutboxTransaction` was removed; use `withTransaction` (or your own transaction) and pass `tx`. `MessageBroker` only needs `publish`.
- **Removed**: `utils`, the no-op `@Idempotent` decorator, and the `joi`, `winston`, `uuid` and `express` dependencies. Node 18+ is required.

## Development

```bash
npm install
npm test                                  # unit tests (in-memory)
createdb atomic_saga_test && npm run test:pg   # plus Postgres integration tests
npm run lint && npm run typecheck
```

## License

MIT
