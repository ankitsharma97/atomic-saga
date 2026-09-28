/**
 * In-memory stores: for tests, local development and single-process apps.
 * State is lost on restart, so they cannot provide crash recovery across processes.
 * Values are round-tripped through JSON to behave like a real database.
 */
import {
  ACTIVE_SAGA_STATUSES,
  BeginResult,
  IdempotencyRecord,
  IdempotencyStore,
  ListExecutionsFilter,
  OutboxEvent,
  OutboxStats,
  OutboxStore,
  SagaExecution,
  SagaStore,
  StoredResponse
} from '../../types';
import { jsonClone, reviveExecution, reviveIdempotencyRecord, reviveOutboxEvent } from '../../utils/helpers';

export class InMemorySagaStore implements SagaStore {
  private readonly executions = new Map<string, SagaExecution>();

  async saveExecution(execution: SagaExecution): Promise<void> {
    if (this.executions.has(execution.id)) {
      throw new Error(`Saga execution ${execution.id} already exists`);
    }
    this.executions.set(execution.id, this.copy(execution));
  }

  async updateExecution(execution: SagaExecution): Promise<boolean> {
    const current = this.executions.get(execution.id);
    if (!current || current.owner !== execution.owner) return false;
    this.executions.set(execution.id, this.copy({ ...execution, updatedAt: new Date() }));
    return true;
  }

  async heartbeat(id: string, owner: string): Promise<boolean> {
    const current = this.executions.get(id);
    if (!current || current.owner !== owner) return false;
    current.updatedAt = new Date();
    return true;
  }

  async getExecution(id: string): Promise<SagaExecution | null> {
    const execution = this.executions.get(id);
    return execution ? this.copy(execution) : null;
  }

  async listExecutions(filter: ListExecutionsFilter = {}): Promise<SagaExecution[]> {
    const matches = [...this.executions.values()]
      .filter(e => (!filter.sagaId || e.sagaId === filter.sagaId) && (!filter.status || e.status === filter.status))
      .sort((a, b) => b.startedAt.getTime() - a.startedAt.getTime());
    return matches.slice(0, filter.limit ?? matches.length).map(e => this.copy(e));
  }

  async claimStaleExecutions(options: { owner: string; staleAfterMs: number; limit: number }): Promise<SagaExecution[]> {
    const cutoff = Date.now() - options.staleAfterMs;
    const stale = [...this.executions.values()]
      .filter(e => ACTIVE_SAGA_STATUSES.includes(e.status) && e.updatedAt.getTime() < cutoff)
      .sort((a, b) => a.updatedAt.getTime() - b.updatedAt.getTime())
      .slice(0, options.limit);
    return stale.map(e => {
      e.owner = options.owner;
      e.updatedAt = new Date();
      return this.copy(e);
    });
  }

  private copy(execution: SagaExecution): SagaExecution {
    return reviveExecution(jsonClone(execution));
  }
}

export class InMemoryIdempotencyStore implements IdempotencyStore {
  private readonly records = new Map<string, IdempotencyRecord>();

  async begin(key: string, fingerprint: string, options: { lockMs: number; ttlMs: number }): Promise<BeginResult> {
    const now = Date.now();
    const existing = this.records.get(key);
    const takeable =
      !existing ||
      existing.expiresAt.getTime() <= now ||
      (existing.status === 'IN_PROGRESS' && existing.lockedUntil.getTime() <= now);

    if (!takeable) {
      return { acquired: false, record: reviveIdempotencyRecord(jsonClone(existing)) };
    }
    this.records.set(key, {
      key,
      fingerprint,
      status: 'IN_PROGRESS',
      lockedUntil: new Date(now + options.lockMs),
      expiresAt: new Date(now + options.ttlMs)
    });
    return { acquired: true };
  }

  async complete(key: string, response: StoredResponse): Promise<void> {
    const record = this.records.get(key);
    if (!record) return;
    record.status = 'COMPLETED';
    record.response = jsonClone(response);
  }

  async release(key: string): Promise<void> {
    this.records.delete(key);
  }

  async deleteExpired(): Promise<number> {
    const now = Date.now();
    let removed = 0;
    for (const [key, record] of this.records) {
      if (record.expiresAt.getTime() <= now) {
        this.records.delete(key);
        removed++;
      }
    }
    return removed;
  }
}

export class InMemoryOutboxStore implements OutboxStore<unknown> {
  private readonly events = new Map<string, OutboxEvent & { lockedUntil?: number }>();

  /** `tx` is ignored: there is no database transaction to join in memory. */
  async saveEvent(event: OutboxEvent): Promise<void> {
    if (this.events.has(event.id)) return; // idempotent add
    this.events.set(event.id, reviveOutboxEvent(jsonClone(event)));
  }

  async claimBatch(options: { limit: number; lockMs: number }): Promise<OutboxEvent[]> {
    const now = Date.now();
    const due = [...this.events.values()]
      .filter(
        e => e.status === 'PENDING' && e.nextAttemptAt.getTime() <= now && (e.lockedUntil === undefined || e.lockedUntil <= now)
      )
      .sort((a, b) => a.createdAt.getTime() - b.createdAt.getTime())
      .slice(0, options.limit);
    return due.map(e => {
      e.lockedUntil = now + options.lockMs;
      const { lockedUntil: _lockedUntil, ...event } = e;
      return reviveOutboxEvent(jsonClone(event));
    });
  }

  async markPublished(id: string): Promise<void> {
    const event = this.events.get(id);
    if (!event) return;
    event.status = 'PUBLISHED';
    event.publishedAt = new Date();
    delete event.lockedUntil;
  }

  async markRetry(id: string, error: string, nextAttemptAt: Date): Promise<void> {
    const event = this.events.get(id);
    if (!event) return;
    event.attempts++;
    event.lastError = error;
    event.nextAttemptAt = nextAttemptAt;
    delete event.lockedUntil;
  }

  async markFailed(id: string, error: string): Promise<void> {
    const event = this.events.get(id);
    if (!event) return;
    event.attempts++;
    event.status = 'FAILED';
    event.lastError = error;
    delete event.lockedUntil;
  }

  async getStats(): Promise<OutboxStats> {
    const stats: OutboxStats = { pending: 0, published: 0, failed: 0 };
    for (const event of this.events.values()) {
      if (event.status === 'PENDING') stats.pending++;
      else if (event.status === 'PUBLISHED') stats.published++;
      else stats.failed++;
    }
    return stats;
  }

  /** Test helper: all events, oldest first. */
  all(): OutboxEvent[] {
    return [...this.events.values()]
      .sort((a, b) => a.createdAt.getTime() - b.createdAt.getTime())
      .map(({ lockedUntil: _lockedUntil, ...event }) => reviveOutboxEvent(jsonClone(event)));
  }
}
