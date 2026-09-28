/**
 * atomic-saga — "all or nothing" operations for distributed Node.js apps.
 *
 * - SagaOrchestrator: run steps in order, compensate completed steps on failure, recover after crashes
 * - IdempotencyMiddleware: make HTTP retries safe (stored responses are replayed)
 * - TransactionalOutbox: commit an event together with your data, publish it reliably afterwards
 *
 * @example
 * ```ts
 * import { SagaOrchestrator, PostgresSagaStore, migrate } from 'atomic-saga';
 *
 * await migrate(pool);
 * const sagas = new SagaOrchestrator(new PostgresSagaStore(pool));
 * sagas.register(checkoutSaga);
 * sagas.startRecovery();
 *
 * const execution = await sagas.executeSaga(checkoutSaga, { orderId, amount });
 * ```
 */
import { SagaOrchestrator, SagaOrchestratorOptions } from './core/SagaOrchestrator';
import { IdempotencyMiddleware, IdempotencyOptions } from './middleware/IdempotencyMiddleware';
import { TransactionalOutbox, TransactionalOutboxOptions } from './patterns/TransactionalOutbox';
import {
  IdempotencyStore,
  ListExecutionsFilter,
  Logger,
  MessageBroker,
  NewOutboxEvent,
  OutboxEvent,
  OutboxStats,
  OutboxStore,
  SagaDefinition,
  SagaExecution,
  SagaStore
} from './types';
import { ConsoleLogger } from './utils/helpers';

export { SagaOrchestrator, validateDefinition } from './core/SagaOrchestrator';
export type { SagaOrchestratorOptions } from './core/SagaOrchestrator';
export { IdempotencyMiddleware } from './middleware/IdempotencyMiddleware';
export type { IdempotencyOptions, IdempotencyRequest, IdempotencyHandler } from './middleware/IdempotencyMiddleware';
export { TransactionalOutbox, defaultTopicFor } from './patterns/TransactionalOutbox';
export type { TransactionalOutboxOptions } from './patterns/TransactionalOutbox';

export { InMemorySagaStore, InMemoryIdempotencyStore, InMemoryOutboxStore } from './stores/memory';
export {
  PostgresSagaStore,
  PostgresIdempotencyStore,
  PostgresOutboxStore,
  migrate,
  schemaSql,
  withTransaction
} from './stores/postgres';
export type { PgPool, PgPoolClient, PgQueryable, PgQueryResult, PostgresTableNames } from './stores/postgres';

export { NonRetryableError, StepTimeoutError, LeaseLostError, serializeError } from './utils/errors';
export { ConsoleLogger, NoopLogger } from './utils/helpers';
export { ACTIVE_SAGA_STATUSES, TERMINAL_SAGA_STATUSES } from './types';
export type * from './types';

export interface AtomicApiConfig<TTx = unknown> {
  sagaStore: SagaStore;
  idempotencyStore?: IdempotencyStore;
  outboxStore?: OutboxStore<TTx>;
  messageBroker?: MessageBroker;
  /** Defaults to a console logger. */
  logger?: Logger;
  saga?: Omit<SagaOrchestratorOptions, 'logger'>;
  idempotency?: Omit<IdempotencyOptions, 'logger'>;
  outbox?: Omit<TransactionalOutboxOptions, 'logger'>;
}

/** Convenience facade wiring the orchestrator, idempotency middleware and outbox together. */
export class AtomicApiOperations<TTx = unknown> {
  readonly sagas: SagaOrchestrator;
  readonly idempotency: IdempotencyMiddleware | undefined;
  readonly outbox: TransactionalOutbox<TTx> | undefined;

  constructor(config: AtomicApiConfig<TTx>) {
    const logger = config.logger ?? new ConsoleLogger();
    this.sagas = new SagaOrchestrator(config.sagaStore, { ...config.saga, logger });
    this.idempotency = config.idempotencyStore
      ? new IdempotencyMiddleware(config.idempotencyStore, { ...config.idempotency, logger })
      : undefined;
    if (config.outboxStore && config.messageBroker) {
      this.outbox = new TransactionalOutbox(config.outboxStore, config.messageBroker, { ...config.outbox, logger });
    } else if (config.outboxStore || config.messageBroker) {
      throw new Error('The outbox needs both outboxStore and messageBroker');
    }
  }

  register(definition: SagaDefinition<any>): this {
    this.sagas.register(definition);
    return this;
  }

  executeSaga<TContext = any>(
    definition: SagaDefinition<TContext>,
    context: TContext,
    options?: { executionId?: string }
  ): Promise<SagaExecution> {
    return this.sagas.executeSaga(definition, context, options);
  }

  getExecution(id: string): Promise<SagaExecution | null> {
    return this.sagas.getExecution(id);
  }

  listExecutions(filter?: ListExecutionsFilter): Promise<SagaExecution[]> {
    return this.sagas.listExecutions(filter);
  }

  /** Express/Connect middleware. Requires `idempotencyStore`. */
  idempotencyMiddleware() {
    return this.requireIdempotency().middleware();
  }

  generateIdempotencyKey(): string {
    return this.requireIdempotency().generateKey();
  }

  /** Add an event to the outbox, inside your transaction `tx`. Requires the outbox to be configured. */
  addOutboxEvent(event: NewOutboxEvent, tx?: TTx): Promise<OutboxEvent> {
    return this.requireOutbox().add(event, tx);
  }

  getOutboxStats(): Promise<OutboxStats> {
    return this.requireOutbox().getStats();
  }

  /** Start background work: saga recovery, and outbox publishing if configured. */
  start(): void {
    this.sagas.startRecovery();
    this.outbox?.start();
  }

  /** Stop background work and wait for in-flight batches. */
  async stop(): Promise<void> {
    await Promise.all([this.sagas.stopRecovery(), this.outbox?.stop()]);
  }

  private requireIdempotency(): IdempotencyMiddleware {
    if (!this.idempotency) throw new Error('Idempotency is not configured: pass idempotencyStore');
    return this.idempotency;
  }

  private requireOutbox(): TransactionalOutbox<TTx> {
    if (!this.outbox) throw new Error('The outbox is not configured: pass outboxStore and messageBroker');
    return this.outbox;
  }
}

export default AtomicApiOperations;
