import { randomUUID } from 'crypto';
import { Logger, MessageBroker, NewOutboxEvent, OutboxEvent, OutboxStats, OutboxStore, RetryPolicy } from '../types';
import { toError } from '../utils/errors';
import { backoffDelay, NoopLogger } from '../utils/helpers';

export interface TransactionalOutboxOptions {
  logger?: Logger;
  /** Delay between polls when there is nothing to publish. Default 1000ms. */
  pollIntervalMs?: number;
  /** Events claimed per poll. Default 100. */
  batchSize?: number;
  /**
   * How long claimed events stay hidden from other publishers. If this process dies mid-batch,
   * the events become visible again after this. Default 30s.
   */
  lockMs?: number;
  /** Retry schedule for failed publishes; `maxAttempts` is when the event is marked FAILED. */
  retryPolicy?: RetryPolicy;
  /** Map an event type to a topic. Default: `PaymentProcessed` -> `payment.processed`. */
  topicFor?: (eventType: string) => string;
}

const DEFAULT_OUTBOX_RETRY: RetryPolicy = { maxAttempts: 10, backoffMs: 1000, backoffMultiplier: 2, maxBackoffMs: 5 * 60 * 1000 };

/**
 * Transactional outbox: write events to an outbox table in the same database transaction as the
 * business change (`add(event, tx)`), and let a background publisher deliver them to the broker.
 * Either both the change and the event are committed, or neither is.
 *
 * Delivery is at-least-once: a message can be published twice (e.g. the process dies between
 * publishing and marking it published). Consumers should dedupe on `eventId`.
 */
export class TransactionalOutbox<TTx = unknown> {
  private readonly store: OutboxStore<TTx>;
  private readonly broker: MessageBroker;
  private readonly logger: Logger;
  private readonly pollIntervalMs: number;
  private readonly batchSize: number;
  private readonly lockMs: number;
  private readonly retryPolicy: RetryPolicy;
  private readonly topicFor: (eventType: string) => string;
  private running = false;
  private timer: NodeJS.Timeout | undefined;
  private inFlight: Promise<unknown> | undefined;

  constructor(store: OutboxStore<TTx>, broker: MessageBroker, options: TransactionalOutboxOptions = {}) {
    this.store = store;
    this.broker = broker;
    this.logger = options.logger ?? new NoopLogger();
    this.pollIntervalMs = options.pollIntervalMs ?? 1000;
    this.batchSize = options.batchSize ?? 100;
    this.lockMs = options.lockMs ?? 30000;
    this.retryPolicy = options.retryPolicy ?? DEFAULT_OUTBOX_RETRY;
    this.topicFor = options.topicFor ?? defaultTopicFor;
  }

  /**
   * Add an event to the outbox. Pass your open transaction as `tx` so the event commits
   * (or rolls back) together with your business data.
   */
  async add(event: NewOutboxEvent, tx?: TTx): Promise<OutboxEvent> {
    const now = new Date();
    const stored: OutboxEvent = {
      id: event.id ?? randomUUID(),
      topic: event.topic ?? this.topicFor(event.eventType),
      eventType: event.eventType,
      payload: event.payload,
      status: 'PENDING',
      attempts: 0,
      nextAttemptAt: now,
      createdAt: now
    };
    if (event.metadata !== undefined) stored.metadata = event.metadata;

    await this.store.saveEvent(stored, tx);
    this.logger.debug('Event added to outbox', { eventId: stored.id, eventType: stored.eventType });
    return stored;
  }

  /** Start publishing in the background. Polls never overlap. */
  start(): void {
    if (this.running) return;
    this.running = true;
    this.logger.info('Outbox publisher started', { pollIntervalMs: this.pollIntervalMs });
    this.schedule(0);
  }

  /** Stop publishing and wait for the batch in progress to finish. */
  async stop(): Promise<void> {
    if (!this.running) return;
    this.running = false;
    if (this.timer) clearTimeout(this.timer);
    this.timer = undefined;
    await this.inFlight;
    this.logger.info('Outbox publisher stopped');
  }

  /** Publish one batch of due events now. Returns how many were published. */
  async processNow(): Promise<number> {
    const events = await this.store.claimBatch({ limit: this.batchSize, lockMs: this.lockMs });
    let published = 0;
    for (const event of events) {
      if (await this.publish(event)) published++;
    }
    return published;
  }

  async getStats(): Promise<OutboxStats> {
    return this.store.getStats();
  }

  private schedule(delayMs: number): void {
    if (!this.running) return;
    this.timer = setTimeout(() => {
      let claimedFullBatch = false;
      this.inFlight = this.store
        .claimBatch({ limit: this.batchSize, lockMs: this.lockMs })
        .then(async events => {
          claimedFullBatch = events.length >= this.batchSize;
          for (const event of events) await this.publish(event);
        })
        .catch(error => this.logger.error('Outbox poll failed', toError(error)))
        .finally(() => {
          this.inFlight = undefined;
          // Drain a backlog without waiting; otherwise poll at the normal pace.
          this.schedule(claimedFullBatch ? 0 : this.pollIntervalMs);
        });
    }, delayMs);
    this.timer.unref?.();
  }

  private async publish(event: OutboxEvent): Promise<boolean> {
    try {
      await this.broker.publish(event.topic, {
        eventId: event.id,
        eventType: event.eventType,
        payload: event.payload,
        ...(event.metadata !== undefined ? { metadata: event.metadata } : {}),
        createdAt: event.createdAt.toISOString()
      });
    } catch (raw) {
      const error = toError(raw);
      const attempts = event.attempts + 1;
      try {
        if (attempts >= this.retryPolicy.maxAttempts) {
          await this.store.markFailed(event.id, error.message);
          this.logger.error('Outbox event failed permanently', error, { eventId: event.id, attempts });
        } else {
          const nextAttemptAt = new Date(Date.now() + backoffDelay(this.retryPolicy, attempts));
          await this.store.markRetry(event.id, error.message, nextAttemptAt);
          this.logger.warn('Outbox publish failed, will retry', { eventId: event.id, attempts, nextAttemptAt });
        }
      } catch (storeError) {
        // The claim lock expires and the event is retried.
        this.logger.error('Failed to record outbox publish failure', toError(storeError), { eventId: event.id });
      }
      return false;
    }

    try {
      await this.store.markPublished(event.id);
    } catch (error) {
      // The event will be re-published after its lock expires (at-least-once delivery).
      this.logger.error('Published event but failed to mark it published', toError(error), { eventId: event.id });
    }
    return true;
  }
}

export function defaultTopicFor(eventType: string): string {
  return eventType
    .replace(/([a-z0-9])([A-Z])/g, '$1.$2')
    .replace(/[\s_-]+/g, '.')
    .toLowerCase();
}
