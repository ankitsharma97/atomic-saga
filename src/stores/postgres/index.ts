/**
 * PostgreSQL stores. Works with `pg` (node-postgres) or anything with the same `query` shape.
 * Requires PostgreSQL 9.5+ (ON CONFLICT, SKIP LOCKED). Run `migrate(pool)` once to create the tables.
 *
 * Leases and lock expiry use the database clock (`now()`), so app servers' clocks may drift.
 */
import {
  ACTIVE_SAGA_STATUSES,
  BeginResult,
  IdempotencyStore,
  ListExecutionsFilter,
  OutboxEvent,
  OutboxStats,
  OutboxStore,
  SagaExecution,
  SagaStore,
  StoredResponse
} from '../../types';
import { reviveExecution } from '../../utils/helpers';

export interface PgQueryResult {
  rows: any[];
  rowCount: number | null;
}

/** A `pg` Pool, PoolClient or Client. */
export interface PgQueryable {
  query(text: string, values?: unknown[]): Promise<PgQueryResult>;
}

export interface PgPoolClient extends PgQueryable {
  release(error?: Error | boolean): void;
}

export interface PgPool extends PgQueryable {
  connect(): Promise<PgPoolClient>;
}

export interface PostgresTableNames {
  sagaExecutions?: string;
  idempotencyKeys?: string;
  outboxEvents?: string;
}

const DEFAULT_TABLES: Required<PostgresTableNames> = {
  sagaExecutions: 'saga_executions',
  idempotencyKeys: 'idempotency_keys',
  outboxEvents: 'outbox_events'
};

function tableName(name: string): string {
  if (!/^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$/.test(name)) {
    throw new Error(`Invalid table name "${name}"`);
  }
  return name
    .split('.')
    .map(part => `"${part}"`)
    .join('.');
}

function indexName(table: string, suffix: string): string {
  return `"${table.replace(/\./g, '_')}_${suffix}"`;
}

const ms = (param: string) => `(${param}::double precision * interval '1 millisecond')`;

/** SQL that creates all tables and indexes (idempotent). */
export function schemaSql(tables: PostgresTableNames = {}): string {
  const t = { ...DEFAULT_TABLES, ...tables };
  const sagas = tableName(t.sagaExecutions);
  const keys = tableName(t.idempotencyKeys);
  const outbox = tableName(t.outboxEvents);
  const active = ACTIVE_SAGA_STATUSES.map(s => `'${s}'`).join(', ');

  return `
CREATE TABLE IF NOT EXISTS ${sagas} (
  id          text PRIMARY KEY,
  saga_id     text NOT NULL,
  status      text NOT NULL,
  owner       text,
  data        jsonb NOT NULL,
  started_at  timestamptz NOT NULL,
  updated_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ${indexName(t.sagaExecutions, 'active_idx')}
  ON ${sagas} (updated_at) WHERE status IN (${active});
CREATE INDEX IF NOT EXISTS ${indexName(t.sagaExecutions, 'saga_idx')}
  ON ${sagas} (saga_id, started_at DESC);

CREATE TABLE IF NOT EXISTS ${keys} (
  key          text PRIMARY KEY,
  fingerprint  text NOT NULL,
  status       text NOT NULL,
  response     jsonb,
  locked_until timestamptz NOT NULL,
  expires_at   timestamptz NOT NULL,
  created_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ${indexName(t.idempotencyKeys, 'expires_idx')} ON ${keys} (expires_at);

CREATE TABLE IF NOT EXISTS ${outbox} (
  id              text PRIMARY KEY,
  topic           text NOT NULL,
  event_type      text NOT NULL,
  payload         jsonb NOT NULL,
  metadata        jsonb,
  status          text NOT NULL DEFAULT 'PENDING',
  attempts        integer NOT NULL DEFAULT 0,
  last_error      text,
  next_attempt_at timestamptz NOT NULL DEFAULT now(),
  locked_until    timestamptz,
  created_at      timestamptz NOT NULL DEFAULT now(),
  published_at    timestamptz
);
CREATE INDEX IF NOT EXISTS ${indexName(t.outboxEvents, 'pending_idx')}
  ON ${outbox} (next_attempt_at, created_at) WHERE status = 'PENDING';
`;
}

/** Create the tables used by the Postgres stores. Safe to run on every startup. */
export async function migrate(db: PgQueryable, tables: PostgresTableNames = {}): Promise<void> {
  await db.query(schemaSql(tables));
}

/**
 * Run `fn` inside a transaction on a dedicated client. Commits if `fn` resolves, rolls back if it throws.
 *
 * ```ts
 * await withTransaction(pool, async tx => {
 *   await tx.query('UPDATE accounts SET balance = balance - $1 WHERE id = $2', [amount, id]);
 *   await outbox.add({ eventType: 'PaymentCaptured', payload: { id, amount } }, tx);
 * });
 * ```
 */
export async function withTransaction<T>(pool: PgPool, fn: (tx: PgPoolClient) => Promise<T>): Promise<T> {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const result = await fn(client);
    await client.query('COMMIT');
    client.release();
    return result;
  } catch (error) {
    try {
      await client.query('ROLLBACK');
      client.release();
    } catch (rollbackError) {
      client.release(rollbackError as Error); // discard the broken connection
    }
    throw error;
  }
}

// ---------------------------------------------------------------------------
// Sagas
// ---------------------------------------------------------------------------

export class PostgresSagaStore implements SagaStore {
  private readonly db: PgQueryable;
  private readonly table: string;

  constructor(db: PgQueryable, options: { tableName?: string } = {}) {
    this.db = db;
    this.table = tableName(options.tableName ?? DEFAULT_TABLES.sagaExecutions);
  }

  async saveExecution(execution: SagaExecution): Promise<void> {
    await this.db.query(
      `INSERT INTO ${this.table} (id, saga_id, status, owner, data, started_at, updated_at)
       VALUES ($1, $2, $3, $4, $5, $6, now())`,
      [execution.id, execution.sagaId, execution.status, execution.owner ?? null, JSON.stringify(execution), execution.startedAt]
    );
  }

  async updateExecution(execution: SagaExecution): Promise<boolean> {
    const result = await this.db.query(
      `UPDATE ${this.table} SET status = $3, data = $4, updated_at = now()
       WHERE id = $1 AND owner IS NOT DISTINCT FROM $2::text`,
      [execution.id, execution.owner ?? null, execution.status, JSON.stringify(execution)]
    );
    return (result.rowCount ?? 0) > 0;
  }

  async heartbeat(id: string, owner: string): Promise<boolean> {
    const result = await this.db.query(`UPDATE ${this.table} SET updated_at = now() WHERE id = $1 AND owner = $2`, [
      id,
      owner
    ]);
    return (result.rowCount ?? 0) > 0;
  }

  async getExecution(id: string): Promise<SagaExecution | null> {
    const result = await this.db.query(`SELECT data, owner, updated_at FROM ${this.table} WHERE id = $1`, [id]);
    return result.rows[0] ? this.fromRow(result.rows[0]) : null;
  }

  async listExecutions(filter: ListExecutionsFilter = {}): Promise<SagaExecution[]> {
    const where: string[] = [];
    const values: unknown[] = [];
    if (filter.sagaId) {
      values.push(filter.sagaId);
      where.push(`saga_id = $${values.length}`);
    }
    if (filter.status) {
      values.push(filter.status);
      where.push(`status = $${values.length}`);
    }
    values.push(filter.limit ?? 100);
    const result = await this.db.query(
      `SELECT data, owner, updated_at FROM ${this.table}
       ${where.length ? `WHERE ${where.join(' AND ')}` : ''}
       ORDER BY started_at DESC LIMIT $${values.length}`,
      values
    );
    return result.rows.map(row => this.fromRow(row));
  }

  async claimStaleExecutions(options: { owner: string; staleAfterMs: number; limit: number }): Promise<SagaExecution[]> {
    const result = await this.db.query(
      `UPDATE ${this.table} t SET owner = $1, updated_at = now()
       FROM (
         SELECT id FROM ${this.table}
         WHERE status = ANY($2::text[]) AND updated_at < now() - ${ms('$3')}
         ORDER BY updated_at
         LIMIT $4
         FOR UPDATE SKIP LOCKED
       ) stale
       WHERE t.id = stale.id
       RETURNING t.data, t.owner, t.updated_at`,
      [options.owner, ACTIVE_SAGA_STATUSES, options.staleAfterMs, options.limit]
    );
    return result.rows.map(row => this.fromRow(row));
  }

  private fromRow(row: any): SagaExecution {
    const data = typeof row.data === 'string' ? JSON.parse(row.data) : row.data;
    const execution = reviveExecution(data);
    // Columns are authoritative for ownership and liveness.
    if (row.owner != null) execution.owner = row.owner;
    else delete execution.owner;
    execution.updatedAt = new Date(row.updated_at);
    return execution;
  }
}

// ---------------------------------------------------------------------------
// Idempotency
// ---------------------------------------------------------------------------

export class PostgresIdempotencyStore implements IdempotencyStore {
  private readonly db: PgQueryable;
  private readonly table: string;

  constructor(db: PgQueryable, options: { tableName?: string } = {}) {
    this.db = db;
    this.table = tableName(options.tableName ?? DEFAULT_TABLES.idempotencyKeys);
  }

  async begin(key: string, fingerprint: string, options: { lockMs: number; ttlMs: number }): Promise<BeginResult> {
    // A concurrent release() can delete the row between the upsert and the select; retry then.
    for (let i = 0; i < 3; i++) {
      const inserted = await this.db.query(
        `INSERT INTO ${this.table} AS k (key, fingerprint, status, locked_until, expires_at)
         VALUES ($1, $2, 'IN_PROGRESS', now() + ${ms('$3')}, now() + ${ms('$4')})
         ON CONFLICT (key) DO UPDATE SET
           fingerprint = EXCLUDED.fingerprint,
           status = 'IN_PROGRESS',
           response = NULL,
           locked_until = EXCLUDED.locked_until,
           expires_at = EXCLUDED.expires_at,
           created_at = now()
         WHERE k.expires_at <= now() OR (k.status = 'IN_PROGRESS' AND k.locked_until <= now())
         RETURNING key`,
        [key, fingerprint, options.lockMs, options.ttlMs]
      );
      if ((inserted.rowCount ?? 0) > 0) return { acquired: true };

      const existing = await this.db.query(
        `SELECT key, fingerprint, status, response, locked_until, expires_at FROM ${this.table} WHERE key = $1`,
        [key]
      );
      const row = existing.rows[0];
      if (row) {
        const record = {
          key: row.key,
          fingerprint: row.fingerprint,
          status: row.status,
          lockedUntil: new Date(row.locked_until),
          expiresAt: new Date(row.expires_at),
          ...(row.response ? { response: row.response as StoredResponse } : {})
        };
        return { acquired: false, record };
      }
    }
    throw new Error(`Could not acquire idempotency key "${key}" due to contention`);
  }

  async complete(key: string, response: StoredResponse): Promise<void> {
    await this.db.query(`UPDATE ${this.table} SET status = 'COMPLETED', response = $2 WHERE key = $1`, [
      key,
      JSON.stringify(response)
    ]);
  }

  async release(key: string): Promise<void> {
    await this.db.query(`DELETE FROM ${this.table} WHERE key = $1 AND status = 'IN_PROGRESS'`, [key]);
  }

  async deleteExpired(): Promise<number> {
    const result = await this.db.query(`DELETE FROM ${this.table} WHERE expires_at <= now()`);
    return result.rowCount ?? 0;
  }
}

// ---------------------------------------------------------------------------
// Outbox
// ---------------------------------------------------------------------------

export class PostgresOutboxStore implements OutboxStore<PgQueryable> {
  private readonly db: PgQueryable;
  private readonly table: string;

  constructor(db: PgQueryable, options: { tableName?: string } = {}) {
    this.db = db;
    this.table = tableName(options.tableName ?? DEFAULT_TABLES.outboxEvents);
  }

  /** Pass the client of your open transaction as `tx` to commit the event atomically with your data. */
  async saveEvent(event: OutboxEvent, tx?: PgQueryable): Promise<void> {
    await (tx ?? this.db).query(
      `INSERT INTO ${this.table} (id, topic, event_type, payload, metadata, status, attempts, next_attempt_at, created_at)
       VALUES ($1, $2, $3, $4, $5, 'PENDING', 0, now(), now())
       ON CONFLICT (id) DO NOTHING`,
      [
        event.id,
        event.topic,
        event.eventType,
        JSON.stringify(event.payload ?? null),
        event.metadata === undefined ? null : JSON.stringify(event.metadata)
      ]
    );
  }

  async claimBatch(options: { limit: number; lockMs: number }): Promise<OutboxEvent[]> {
    const result = await this.db.query(
      `UPDATE ${this.table} t SET locked_until = now() + ${ms('$2')}
       FROM (
         SELECT id FROM ${this.table}
         WHERE status = 'PENDING' AND next_attempt_at <= now()
           AND (locked_until IS NULL OR locked_until <= now())
         ORDER BY created_at, id
         LIMIT $1
         FOR UPDATE SKIP LOCKED
       ) due
       WHERE t.id = due.id
       RETURNING t.*`,
      [options.limit, options.lockMs]
    );
    return result.rows
      .map(row => this.fromRow(row))
      .sort((a, b) => a.createdAt.getTime() - b.createdAt.getTime() || a.id.localeCompare(b.id));
  }

  async markPublished(id: string): Promise<void> {
    await this.db.query(
      `UPDATE ${this.table} SET status = 'PUBLISHED', published_at = now(), locked_until = NULL WHERE id = $1`,
      [id]
    );
  }

  async markRetry(id: string, error: string, nextAttemptAt: Date): Promise<void> {
    await this.db.query(
      `UPDATE ${this.table}
       SET attempts = attempts + 1, last_error = $2, next_attempt_at = $3, locked_until = NULL
       WHERE id = $1`,
      [id, error, nextAttemptAt]
    );
  }

  async markFailed(id: string, error: string): Promise<void> {
    await this.db.query(
      `UPDATE ${this.table}
       SET status = 'FAILED', attempts = attempts + 1, last_error = $2, locked_until = NULL
       WHERE id = $1`,
      [id, error]
    );
  }

  async getStats(): Promise<OutboxStats> {
    const result = await this.db.query(`SELECT status, count(*)::int AS count FROM ${this.table} GROUP BY status`);
    const stats: OutboxStats = { pending: 0, published: 0, failed: 0 };
    for (const row of result.rows) {
      if (row.status === 'PENDING') stats.pending = row.count;
      else if (row.status === 'PUBLISHED') stats.published = row.count;
      else if (row.status === 'FAILED') stats.failed = row.count;
    }
    return stats;
  }

  /**
   * Put FAILED events back in the queue (e.g. after fixing the broker). Returns how many were requeued.
   */
  async requeueFailed(ids?: string[]): Promise<number> {
    const result = await this.db.query(
      `UPDATE ${this.table} SET status = 'PENDING', attempts = 0, next_attempt_at = now(), locked_until = NULL
       WHERE status = 'FAILED' ${ids ? 'AND id = ANY($1::text[])' : ''}`,
      ids ? [ids] : []
    );
    return result.rowCount ?? 0;
  }

  private fromRow(row: any): OutboxEvent {
    const event: OutboxEvent = {
      id: row.id,
      topic: row.topic,
      eventType: row.event_type,
      payload: row.payload,
      status: row.status,
      attempts: row.attempts,
      nextAttemptAt: new Date(row.next_attempt_at),
      createdAt: new Date(row.created_at)
    };
    if (row.metadata != null) event.metadata = row.metadata;
    if (row.last_error != null) event.lastError = row.last_error;
    if (row.published_at != null) event.publishedAt = new Date(row.published_at);
    return event;
  }
}
