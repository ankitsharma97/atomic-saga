/**
 * Core types and interfaces for atomic-saga.
 *
 * Everything that is persisted (saga context, step outputs, outbox payloads,
 * idempotent responses) must be JSON-serializable: stores are free to round-trip
 * values through JSON, and the bundled stores do exactly that.
 */

// ---------------------------------------------------------------------------
// Shared
// ---------------------------------------------------------------------------

export interface Logger {
  info(message: string, meta?: any): void;
  error(message: string, error?: Error, meta?: any): void;
  warn(message: string, meta?: any): void;
  debug(message: string, meta?: any): void;
}

/** Plain-object form of an Error, safe to store as JSON. */
export interface SerializedError {
  name: string;
  message: string;
  stack?: string;
}

export interface RetryPolicy {
  maxAttempts: number;
  backoffMs: number;
  backoffMultiplier: number;
  /** Upper bound for a single backoff delay. */
  maxBackoffMs?: number;
  /** Return false to stop retrying for this error. Defaults to retrying everything except NonRetryableError. */
  retryable?: (error: Error) => boolean;
}

// ---------------------------------------------------------------------------
// Sagas
// ---------------------------------------------------------------------------

/** Extra information passed to every step action and compensation. */
export interface StepMeta {
  executionId: string;
  sagaId: string;
  stepId: string;
  /** 1-based attempt number. */
  attempt: number;
  /**
   * Stable key for this step of this execution (`<executionId>:<stepId>` for actions,
   * `<executionId>:<stepId>:compensate` for compensations). Pass it to downstream APIs
   * so a retried or recovered step does not repeat its side effect.
   */
  idempotencyKey: string;
  /** Aborted when the attempt times out or this worker loses ownership of the execution. */
  signal: AbortSignal;
  /** Outputs of the steps that have completed so far, keyed by step id. */
  results: Record<string, unknown>;
}

export interface TransactionStep<TContext = any, TOutput = any> {
  id: string;
  name: string;
  action: (context: TContext, meta: StepMeta) => Promise<TOutput>;
  /** Undo the action. Receives the action's output. Should be idempotent. */
  compensation?: (context: TContext, output: TOutput | undefined, meta: StepMeta) => Promise<void>;
  retryPolicy?: RetryPolicy;
  /** Retry policy for the compensation. Defaults to the step's (or the orchestrator's) retry policy. */
  compensationRetryPolicy?: RetryPolicy;
  /** Per-attempt timeout in ms. */
  timeout?: number;
}

/**
 * What to do with an execution that was interrupted (process crash, deploy, lost lease)
 * while it was still running its steps.
 * - `resume`: re-run the interrupted step and continue. Steps must be idempotent (use `meta.idempotencyKey`).
 * - `compensate`: undo the steps that completed and stop.
 */
export type RecoveryStrategy = 'resume' | 'compensate';

export interface SagaDefinition<TContext = any> {
  id: string;
  name: string;
  steps: TransactionStep<TContext>[];
  /** Defaults to `resume`. */
  recovery?: RecoveryStrategy;
  onSuccess?: (context: TContext, execution: SagaExecution) => Promise<void>;
  /** Called after all compensations succeeded. */
  onFailure?: (context: TContext, error: SerializedError, execution: SagaExecution) => Promise<void>;
  /** Called when at least one compensation failed; the system needs manual attention. */
  onCompensationFailed?: (context: TContext, execution: SagaExecution) => Promise<void>;
}

export type SagaStatus =
  | 'PENDING'
  | 'RUNNING'
  | 'COMPLETED'
  | 'COMPENSATING'
  | 'COMPENSATED'
  /** A step failed and at least one compensation could not be completed. Needs manual attention. */
  | 'COMPENSATION_FAILED';

export const TERMINAL_SAGA_STATUSES: readonly SagaStatus[] = ['COMPLETED', 'COMPENSATED', 'COMPENSATION_FAILED'];
export const ACTIVE_SAGA_STATUSES: readonly SagaStatus[] = ['PENDING', 'RUNNING', 'COMPENSATING'];

export type StepStatus = 'SUCCESS' | 'FAILED' | 'COMPENSATED' | 'COMPENSATION_FAILED';

export interface StepResult {
  stepId: string;
  stepName: string;
  status: StepStatus;
  output?: any;
  error?: SerializedError;
  compensationError?: SerializedError;
  startedAt: Date;
  completedAt?: Date;
  attempts: number;
  compensationAttempts?: number;
}

export interface SagaExecution {
  id: string;
  sagaId: string;
  status: SagaStatus;
  context: any;
  /** Index of the step currently (or last) being executed. */
  currentStep?: number;
  stepResults: StepResult[];
  /** The error that made the saga fail. */
  error?: SerializedError;
  /** Worker that currently owns (is running) this execution. */
  owner?: string;
  startedAt: Date;
  updatedAt: Date;
  completedAt?: Date;
}

export interface ListExecutionsFilter {
  sagaId?: string;
  status?: SagaStatus;
  limit?: number;
}

export interface SagaStore {
  /** Insert a new execution. */
  saveExecution(execution: SagaExecution): Promise<void>;
  /**
   * Persist the execution, but only if `execution.owner` still owns it. Must refresh `updatedAt`.
   * Returns false if ownership was lost (another worker claimed it).
   */
  updateExecution(execution: SagaExecution): Promise<boolean>;
  /** Refresh `updatedAt` without other changes. Returns false if ownership was lost. */
  heartbeat(id: string, owner: string): Promise<boolean>;
  getExecution(id: string): Promise<SagaExecution | null>;
  listExecutions(filter?: ListExecutionsFilter): Promise<SagaExecution[]>;
  /**
   * Atomically take ownership of up to `limit` active executions (PENDING, RUNNING, COMPENSATING)
   * whose `updatedAt` is older than `staleAfterMs`, setting their owner to `owner`.
   */
  claimStaleExecutions(options: { owner: string; staleAfterMs: number; limit: number }): Promise<SagaExecution[]>;
}

// ---------------------------------------------------------------------------
// Idempotency
// ---------------------------------------------------------------------------

export interface StoredResponse {
  statusCode: number;
  headers: Record<string, string>;
  /** Response body, base64-encoded. */
  body: string;
}

export interface IdempotencyRecord {
  key: string;
  /** Hash of the request (method, path, body). A different request with the same key is rejected. */
  fingerprint: string;
  status: 'IN_PROGRESS' | 'COMPLETED';
  response?: StoredResponse;
  /** While IN_PROGRESS, other requests with this key are rejected until this time. */
  lockedUntil: Date;
  expiresAt: Date;
}

export type BeginResult = { acquired: true } | { acquired: false; record: IdempotencyRecord };

export interface IdempotencyStore {
  /**
   * Atomically start processing `key`. Succeeds if no record exists, the record expired,
   * or it is IN_PROGRESS with an expired lock. Otherwise returns the existing record.
   */
  begin(key: string, fingerprint: string, options: { lockMs: number; ttlMs: number }): Promise<BeginResult>;
  /** Mark `key` as completed and store the response for replay. */
  complete(key: string, response: StoredResponse): Promise<void>;
  /** Forget `key` so the request can be retried (used when processing failed). */
  release(key: string): Promise<void>;
  /** Remove expired records. Returns the number removed. */
  deleteExpired?(): Promise<number>;
}

// ---------------------------------------------------------------------------
// Outbox
// ---------------------------------------------------------------------------

export type OutboxEventStatus = 'PENDING' | 'PUBLISHED' | 'FAILED';

export interface OutboxEvent {
  id: string;
  topic: string;
  eventType: string;
  payload: any;
  /** Free-form metadata (e.g. sagaId, aggregate id, trace id). */
  metadata?: Record<string, unknown>;
  status: OutboxEventStatus;
  attempts: number;
  lastError?: string;
  nextAttemptAt: Date;
  createdAt: Date;
  publishedAt?: Date;
}

export interface NewOutboxEvent {
  eventType: string;
  payload: any;
  /** Defaults to the topic derived from `eventType` (e.g. `PaymentProcessed` -> `payment.processed`). */
  topic?: string;
  metadata?: Record<string, unknown>;
  /** Provide your own id to make `add` idempotent. */
  id?: string;
}

export interface OutboxStats {
  pending: number;
  published: number;
  failed: number;
}

/**
 * `TTx` is the store's transaction handle (e.g. a `pg` PoolClient). Passing it to `saveEvent`
 * writes the event in the same database transaction as your business data.
 */
export interface OutboxStore<TTx = unknown> {
  saveEvent(event: OutboxEvent, tx?: TTx): Promise<void>;
  /**
   * Atomically claim up to `limit` PENDING events that are due, hiding them from other
   * claimers for `lockMs`.
   */
  claimBatch(options: { limit: number; lockMs: number }): Promise<OutboxEvent[]>;
  markPublished(id: string): Promise<void>;
  /** Record a failed attempt and schedule the next one. */
  markRetry(id: string, error: string, nextAttemptAt: Date): Promise<void>;
  /** Give up on the event (dead letter). */
  markFailed(id: string, error: string): Promise<void>;
  getStats(): Promise<OutboxStats>;
}

export interface OutboxMessage {
  eventId: string;
  eventType: string;
  payload: any;
  metadata?: Record<string, unknown>;
  createdAt: string;
}

export interface MessageBroker {
  /** Must resolve only once the broker has durably accepted the message. */
  publish(topic: string, message: OutboxMessage): Promise<void>;
}
