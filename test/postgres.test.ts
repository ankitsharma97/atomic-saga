/**
 * Integration tests against a real PostgreSQL. Skipped unless DATABASE_URL is set, e.g.
 *   DATABASE_URL=postgres://localhost/atomic_saga_test npm test
 */
import { Pool } from 'pg';
import {
  IdempotencyMiddleware,
  migrate,
  OutboxMessage,
  PostgresIdempotencyStore,
  PostgresOutboxStore,
  PostgresSagaStore,
  SagaDefinition,
  SagaExecution,
  SagaOrchestrator,
  TransactionalOutbox,
  withTransaction
} from '../src';

const url = process.env['DATABASE_URL'];
const describeDb = url ? describe : describe.skip;

describeDb('Postgres stores', () => {
  let pool: Pool;
  const schema = `atomic_saga_test_${process.pid}`;
  const tables = {
    sagaExecutions: `${schema}.saga_executions`,
    idempotencyKeys: `${schema}.idempotency_keys`,
    outboxEvents: `${schema}.outbox_events`
  };

  beforeAll(async () => {
    pool = new Pool({ connectionString: url, max: 10 });
    await pool.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE; CREATE SCHEMA "${schema}"`);
    await migrate(pool, tables);
    await migrate(pool, tables); // idempotent
  });

  afterAll(async () => {
    await pool.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`);
    await pool.end();
  });

  beforeEach(async () => {
    await pool.query(`TRUNCATE ${Object.values(tables).join(', ')}`);
  });

  describe('PostgresSagaStore', () => {
    const store = () => new PostgresSagaStore(pool, { tableName: tables.sagaExecutions });

    test('runs a saga end to end and round-trips dates and outputs', async () => {
      const sagas = new SagaOrchestrator(store(), { defaultRetryPolicy: { maxAttempts: 1, backoffMs: 0, backoffMultiplier: 1 } });
      const def: SagaDefinition = {
        id: 'checkout',
        name: 'Checkout',
        steps: [
          { id: 'a', name: 'a', action: async () => ({ n: 1 }), compensation: async () => {} },
          {
            id: 'b',
            name: 'b',
            action: async () => {
              throw new Error('nope');
            }
          }
        ]
      };

      const result = await sagas.executeSaga(def, { orderId: 'o1' });
      const stored = await sagas.getExecution(result.id);

      expect(stored?.status).toBe('COMPENSATED');
      expect(stored?.context).toEqual({ orderId: 'o1' });
      expect(stored?.stepResults[0]).toMatchObject({ stepId: 'a', status: 'COMPENSATED', output: { n: 1 } });
      expect(stored?.stepResults[0]!.startedAt).toBeInstanceOf(Date);
      expect(stored?.error?.message).toBe('nope');
      expect(await sagas.listExecutions({ sagaId: 'checkout', status: 'COMPENSATED' })).toHaveLength(1);
      expect(await sagas.listExecutions({ status: 'COMPLETED' })).toHaveLength(0);
    });

    test('updates are fenced by owner', async () => {
      const s = store();
      const execution: SagaExecution = {
        id: 'e1',
        sagaId: 's',
        status: 'RUNNING',
        context: {},
        stepResults: [],
        owner: 'A',
        startedAt: new Date(),
        updatedAt: new Date()
      };
      await s.saveExecution(execution);
      expect(await s.updateExecution({ ...execution, owner: 'B' })).toBe(false);
      expect(await s.heartbeat('e1', 'B')).toBe(false);
      expect(await s.updateExecution(execution)).toBe(true);
      expect(await s.heartbeat('e1', 'A')).toBe(true);
    });

    test('concurrent workers each claim a disjoint set of stale executions', async () => {
      const s = store();
      for (let i = 0; i < 20; i++) {
        await s.saveExecution({
          id: `e${i}`,
          sagaId: 's',
          status: i % 5 === 0 ? 'COMPLETED' : 'RUNNING',
          context: {},
          stepResults: [],
          owner: 'dead',
          startedAt: new Date(),
          updatedAt: new Date()
        });
      }
      await pool.query(`UPDATE ${tables.sagaExecutions} SET updated_at = now() - interval '1 hour'`);

      const claims = await Promise.all(
        [1, 2, 3, 4].map(w => s.claimStaleExecutions({ owner: `w${w}`, staleAfterMs: 60000, limit: 10 }))
      );
      const ids = claims.flat().map(e => e.id);
      expect(ids).toHaveLength(16); // 20 minus 4 COMPLETED
      expect(new Set(ids).size).toBe(16);
      claims.forEach((claimed, i) => claimed.forEach(e => expect(e.owner).toBe(`w${i + 1}`)));
      expect(await s.claimStaleExecutions({ owner: 'w5', staleAfterMs: 60000, limit: 10 })).toHaveLength(0);
    });

    test('recovers an execution abandoned by a crashed worker', async () => {
      const s = store();
      const calls: string[] = [];
      const def: SagaDefinition = {
        id: 'checkout',
        name: 'Checkout',
        steps: ['a', 'b'].map(id => ({ id, name: id, action: async () => void calls.push(id) }))
      };
      await s.saveExecution({
        id: 'crashed',
        sagaId: 'checkout',
        status: 'RUNNING',
        context: {},
        currentStep: 1,
        stepResults: [{ stepId: 'a', stepName: 'a', status: 'SUCCESS', startedAt: new Date(), attempts: 1 }],
        owner: 'dead',
        startedAt: new Date(),
        updatedAt: new Date()
      });
      await pool.query(`UPDATE ${tables.sagaExecutions} SET updated_at = now() - interval '1 hour'`);

      const [recovered] = await new SagaOrchestrator(s, { workerId: 'w' }).register(def).recover();

      expect(recovered?.status).toBe('COMPLETED');
      expect(calls).toEqual(['b']);
    });
  });

  describe('PostgresIdempotencyStore', () => {
    const store = () => new PostgresIdempotencyStore(pool, { tableName: tables.idempotencyKeys });
    const opts = { lockMs: 60000, ttlMs: 60000 };

    test('begin / complete / replay / release lifecycle', async () => {
      const s = store();
      expect(await s.begin('k', 'fp', opts)).toEqual({ acquired: true });

      const busy = await s.begin('k', 'fp', opts);
      expect(busy.acquired).toBe(false);
      if (!busy.acquired) expect(busy.record.status).toBe('IN_PROGRESS');

      await s.complete('k', { statusCode: 201, headers: { a: 'b' }, body: 'e30=' });
      const done = await s.begin('k', 'fp', opts);
      if (done.acquired) throw new Error('expected existing record');
      expect(done.record).toMatchObject({
        status: 'COMPLETED',
        fingerprint: 'fp',
        response: { statusCode: 201, headers: { a: 'b' }, body: 'e30=' }
      });
      expect(done.record.expiresAt).toBeInstanceOf(Date);

      await s.release('k'); // completed keys are not released
      expect((await s.begin('k', 'fp', opts)).acquired).toBe(false);

      expect(await s.begin('k2', 'fp', opts)).toEqual({ acquired: true });
      await s.release('k2');
      expect(await s.begin('k2', 'fp', opts)).toEqual({ acquired: true });
    });

    test('exactly one of many concurrent requests acquires a key', async () => {
      const s = store();
      const results = await Promise.all(Array.from({ length: 20 }, () => s.begin('race', 'fp', opts)));
      expect(results.filter(r => r.acquired)).toHaveLength(1);
    });

    test('expired locks and expired records can be taken over; deleteExpired cleans up', async () => {
      const s = store();
      await s.begin('stale-lock', 'fp', { lockMs: 1, ttlMs: 60000 });
      await s.begin('expired', 'fp', { lockMs: 1, ttlMs: 1 });
      await s.complete('expired', { statusCode: 200, headers: {}, body: '' });
      await new Promise(r => setTimeout(r, 20));

      expect((await s.begin('stale-lock', 'fp', opts)).acquired).toBe(true);
      expect(await s.deleteExpired()).toBe(1);
      expect((await s.begin('expired', 'fp', opts)).acquired).toBe(true);
    });

    test('works as the middleware store over HTTP', async () => {
      const express = (await import('express')).default;
      let calls = 0;
      const app = express();
      app.use(express.json());
      app.use(new IdempotencyMiddleware(store()).middleware());
      app.post('/pay', (_req, res) => void res.status(201).json({ n: ++calls }));
      const server = app.listen(0);
      await new Promise(r => server.once('listening', r));
      const { port } = server.address() as { port: number };

      const send = () =>
        fetch(`http://127.0.0.1:${port}/pay`, {
          method: 'POST',
          headers: { 'content-type': 'application/json', 'idempotency-key': 'pay-1' },
          body: '{"amount":1}'
        });
      const bodies = [await (await send()).json(), await (await send()).json()];
      server.closeAllConnections();
      await new Promise(r => server.close(r));

      expect(bodies).toEqual([{ n: 1 }, { n: 1 }]);
      expect(calls).toBe(1);
    });
  });

  describe('PostgresOutboxStore', () => {
    const store = () => new PostgresOutboxStore(pool, { tableName: tables.outboxEvents });

    function broker() {
      const published: OutboxMessage[] = [];
      return { published, publish: async (_topic: string, message: OutboxMessage) => void published.push(message) };
    }

    test('an event added in a rolled-back transaction is never published', async () => {
      const b = broker();
      const outbox = new TransactionalOutbox(store(), b);

      await withTransaction(pool, async tx => {
        await outbox.add({ id: 'committed', eventType: 'OrderPlaced', payload: { id: 1 } }, tx);
      });
      await expect(
        withTransaction(pool, async tx => {
          await outbox.add({ id: 'rolled-back', eventType: 'OrderPlaced', payload: { id: 2 } }, tx);
          throw new Error('business logic failed');
        })
      ).rejects.toThrow('business logic failed');

      expect(await outbox.processNow()).toBe(1);
      expect(b.published.map(m => m.eventId)).toEqual(['committed']);
      expect(await outbox.getStats()).toEqual({ pending: 0, published: 1, failed: 0 });
    });

    test('concurrent publishers never publish the same event twice', async () => {
      const b = broker();
      const s = store();
      const seed = new TransactionalOutbox(s, b);
      for (let i = 0; i < 50; i++) await seed.add({ eventType: 'Tick', payload: i });

      const publishers = [1, 2, 3, 4].map(() => new TransactionalOutbox(s, b, { batchSize: 7 }));
      for (let round = 0; round < 10; round++) {
        await Promise.all(publishers.map(p => p.processNow()));
      }

      const ids = b.published.map(m => m.eventId);
      expect(ids).toHaveLength(50);
      expect(new Set(ids).size).toBe(50);
    });

    test('retry scheduling, dead-lettering and requeue', async () => {
      const s = store();
      let fail = true;
      const published: string[] = [];
      const outbox = new TransactionalOutbox(
        s,
        {
          publish: async (_t, m) => {
            if (fail) throw new Error('broker down');
            published.push(m.eventId);
          }
        },
        { retryPolicy: { maxAttempts: 2, backoffMs: 0, backoffMultiplier: 1 } }
      );
      await outbox.add({ id: 'e1', eventType: 'A', payload: null });

      await outbox.processNow();
      const { rows } = await pool.query(`SELECT attempts, last_error, status FROM ${tables.outboxEvents}`);
      expect(rows[0]).toEqual({ attempts: 1, last_error: 'broker down', status: 'PENDING' });

      await outbox.processNow();
      expect(await outbox.getStats()).toEqual({ pending: 0, published: 0, failed: 1 });

      fail = false;
      expect(await s.requeueFailed()).toBe(1);
      await outbox.processNow();
      expect(published).toEqual(['e1']);
    });
  });
});
