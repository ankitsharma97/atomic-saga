import { IdempotencyRecord, Logger, OutboxEvent, RetryPolicy, SagaExecution } from '../types';

export const DEFAULT_RETRY_POLICY: RetryPolicy = { maxAttempts: 3, backoffMs: 1000, backoffMultiplier: 2 };

/** Delay before retry number `attempt` (1-based: the delay after the first failed attempt is attempt 1). */
export function backoffDelay(policy: RetryPolicy, attempt: number): number {
  const delay = policy.backoffMs * Math.pow(policy.backoffMultiplier, attempt - 1);
  return policy.maxBackoffMs !== undefined ? Math.min(delay, policy.maxBackoffMs) : delay;
}

export function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/** Deep copy through JSON, so in-memory stores behave like real (serializing) ones. */
export function jsonClone<T>(value: T): T {
  return value === undefined ? value : JSON.parse(JSON.stringify(value));
}

function toDate(value: unknown): Date {
  return value instanceof Date ? value : new Date(value as string);
}

/** Restore Date fields of an execution that went through JSON. */
export function reviveExecution(raw: any): SagaExecution {
  const execution: SagaExecution = {
    ...raw,
    startedAt: toDate(raw.startedAt),
    updatedAt: toDate(raw.updatedAt),
    stepResults: (raw.stepResults ?? []).map((r: any) => {
      const result = { ...r, startedAt: toDate(r.startedAt) };
      if (r.completedAt != null) result.completedAt = toDate(r.completedAt);
      return result;
    })
  };
  if (raw.completedAt != null) execution.completedAt = toDate(raw.completedAt);
  return execution;
}

export function reviveOutboxEvent(raw: any): OutboxEvent {
  const event: OutboxEvent = {
    ...raw,
    nextAttemptAt: toDate(raw.nextAttemptAt),
    createdAt: toDate(raw.createdAt)
  };
  if (raw.publishedAt != null) event.publishedAt = toDate(raw.publishedAt);
  return event;
}

export function reviveIdempotencyRecord(raw: any): IdempotencyRecord {
  return { ...raw, lockedUntil: toDate(raw.lockedUntil), expiresAt: toDate(raw.expiresAt) };
}

export class ConsoleLogger implements Logger {
  info(message: string, meta?: any): void {
    console.log(`[INFO] ${message}`, meta ?? '');
  }

  error(message: string, error?: Error, meta?: any): void {
    console.error(`[ERROR] ${message}`, error?.message ?? '', meta ?? '');
  }

  warn(message: string, meta?: any): void {
    console.warn(`[WARN] ${message}`, meta ?? '');
  }

  debug(message: string, meta?: any): void {
    console.debug(`[DEBUG] ${message}`, meta ?? '');
  }
}

export class NoopLogger implements Logger {
  info(): void {}
  error(): void {}
  warn(): void {}
  debug(): void {}
}
