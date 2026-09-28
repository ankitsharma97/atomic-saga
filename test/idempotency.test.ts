import express from 'express';
import type { AddressInfo } from 'net';
import type { Server } from 'http';
import { IdempotencyMiddleware, IdempotencyOptions, IdempotencyStore, InMemoryIdempotencyStore } from '../src';

interface TestApp {
  url: string;
  calls: Record<string, number>;
  release: () => void;
  close: () => Promise<void>;
}

async function startApp(store: IdempotencyStore = new InMemoryIdempotencyStore(), options: IdempotencyOptions = {}): Promise<TestApp> {
  const calls: Record<string, number> = {};
  const count = (name: string) => (calls[name] = (calls[name] ?? 0) + 1);
  const waiting: Array<() => void> = [];
  const release = () => waiting.splice(0).forEach(resolve => resolve());

  const app = express();
  app.use(express.json());
  app.use(new IdempotencyMiddleware(store, options).middleware());

  app.post('/charges', (req, res) => {
    const n = count('charges');
    res.setHeader('X-Charge', String(n));
    res.cookie('session', 'secret');
    res.status(201).json({ id: `ch_${n}`, amount: req.body.amount });
  });
  app.get('/charges', (_req, res) => {
    count('get');
    res.json({ ok: true });
  });
  app.post('/flaky', (_req, res) => {
    const n = count('flaky');
    if (n === 1) res.status(503).json({ error: 'downstream unavailable' });
    else res.status(200).json({ attempt: n });
  });
  app.post('/empty', (_req, res) => {
    count('empty');
    res.status(204).end();
  });
  app.post('/slow', async (_req, res) => {
    count('slow');
    await new Promise<void>(resolve => waiting.push(resolve));
    res.json({ done: true });
  });

  const server: Server = await new Promise(resolve => {
    const s = app.listen(0, () => resolve(s));
  });
  const { port } = server.address() as AddressInfo;
  return {
    url: `http://127.0.0.1:${port}`,
    calls,
    release,
    close: () =>
      new Promise(resolve => {
        release();
        server.close(() => resolve());
        server.closeAllConnections();
      })
  };
}

function post(app: TestApp, path: string, body: unknown, key?: string, headers: Record<string, string> = {}) {
  return fetch(`${app.url}${path}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', ...(key ? { 'idempotency-key': key } : {}), ...headers },
    body: JSON.stringify(body)
  });
}

describe('IdempotencyMiddleware', () => {
  let app: TestApp;
  afterEach(async () => app?.close());

  test('without a key, requests pass through', async () => {
    app = await startApp();
    await post(app, '/charges', { amount: 1 });
    await post(app, '/charges', { amount: 1 });
    expect(app.calls['charges']).toBe(2);
  });

  test('a retry with the same key replays the stored response without re-running the handler', async () => {
    app = await startApp();
    const first = await post(app, '/charges', { amount: 100 }, 'k1');
    const second = await post(app, '/charges', { amount: 100 }, 'k1');

    expect(app.calls['charges']).toBe(1);
    expect(second.status).toBe(201);
    expect(await second.json()).toEqual(await first.json());
    expect(second.headers.get('x-charge')).toBe('1');
    expect(second.headers.get('content-type')).toMatch(/application\/json/);
    expect(second.headers.get('idempotent-replayed')).toBe('true');
    expect(first.headers.get('idempotent-replayed')).toBeNull();
    expect(second.headers.get('set-cookie')).toBeNull();
  });

  test('replays empty responses', async () => {
    app = await startApp();
    await post(app, '/empty', {}, 'k');
    const replay = await post(app, '/empty', {}, 'k');
    expect(replay.status).toBe(204);
    expect(app.calls['empty']).toBe(1);
  });

  test('the same key with a different body is rejected with 422', async () => {
    app = await startApp();
    await post(app, '/charges', { amount: 100 }, 'k1');
    const reused = await post(app, '/charges', { amount: 999 }, 'k1');
    expect(reused.status).toBe(422);
    expect(((await reused.json()) as { error: string }).error).toBe('idempotency_key_reused');
    expect(app.calls['charges']).toBe(1);
  });

  test('body key order does not change the fingerprint', async () => {
    app = await startApp();
    await fetch(`${app.url}/charges`, {
      method: 'POST',
      headers: { 'content-type': 'application/json', 'idempotency-key': 'k' },
      body: '{"amount":1,"currency":"usd"}'
    });
    const replay = await fetch(`${app.url}/charges`, {
      method: 'POST',
      headers: { 'content-type': 'application/json', 'idempotency-key': 'k' },
      body: '{"currency":"usd","amount":1}'
    });
    expect(replay.status).toBe(201);
    expect(app.calls['charges']).toBe(1);
  });

  test('a concurrent request with the same key gets 409 while the first is in progress', async () => {
    app = await startApp();
    const first = post(app, '/slow', {}, 'k');
    await new Promise(r => setTimeout(r, 30));

    const concurrent = await post(app, '/slow', {}, 'k');
    expect(concurrent.status).toBe(409);
    expect(Number(concurrent.headers.get('retry-after'))).toBeGreaterThan(0);

    app.release();
    expect((await first).status).toBe(200);
    const after = await post(app, '/slow', {}, 'k');
    expect(after.headers.get('idempotent-replayed')).toBe('true');
    expect(app.calls['slow']).toBe(1);
  });

  test('a 5xx response releases the key so the client can retry', async () => {
    app = await startApp();
    expect((await post(app, '/flaky', {}, 'k')).status).toBe(503);
    const retry = await post(app, '/flaky', {}, 'k');
    expect(retry.status).toBe(200);
    expect(await retry.json()).toEqual({ attempt: 2 });

    const replay = await post(app, '/flaky', {}, 'k');
    expect(await replay.json()).toEqual({ attempt: 2 });
    expect(app.calls['flaky']).toBe(2);
  });

  test('an expired lock can be taken over (the first request died)', async () => {
    app = await startApp(new InMemoryIdempotencyStore(), { lockMs: 20 });
    void post(app, '/slow', {}, 'k').catch(() => {});
    await new Promise(r => setTimeout(r, 40));
    const takeover = post(app, '/slow', {}, 'k');
    await new Promise(r => setTimeout(r, 20));
    app.release();
    expect((await takeover).status).toBe(200);
    expect(app.calls['slow']).toBe(2);
  });

  test('only configured methods are affected', async () => {
    app = await startApp();
    const get = () => fetch(`${app.url}/charges`, { headers: { 'idempotency-key': 'k' } });
    await get();
    await get();
    expect(app.calls['get']).toBe(2);
  });

  test('required: requests without a key get 400; oversized keys get 400', async () => {
    app = await startApp(new InMemoryIdempotencyStore(), { required: true });
    expect((await post(app, '/charges', {})).status).toBe(400);
    expect((await post(app, '/charges', {}, 'x'.repeat(256))).status).toBe(400);
    expect(app.calls['charges']).toBeUndefined();
  });

  test('scope separates keys between tenants', async () => {
    app = await startApp(new InMemoryIdempotencyStore(), { scope: req => String(req.headers['x-tenant']) });
    await post(app, '/charges', { amount: 1 }, 'k', { 'x-tenant': 'a' });
    await post(app, '/charges', { amount: 1 }, 'k', { 'x-tenant': 'b' });
    expect(app.calls['charges']).toBe(2);
  });

  test('store outage: 503 by default, pass-through with failOpen', async () => {
    const broken: IdempotencyStore = {
      begin: async () => {
        throw new Error('connection refused');
      },
      complete: async () => {},
      release: async () => {}
    };
    app = await startApp(broken);
    expect((await post(app, '/charges', {}, 'k')).status).toBe(503);
    expect(app.calls['charges']).toBeUndefined();
    await app.close();

    app = await startApp(broken, { failOpen: true });
    expect((await post(app, '/charges', {}, 'k')).status).toBe(201);
  });

  test('responses over maxBodyBytes are not stored', async () => {
    app = await startApp(new InMemoryIdempotencyStore(), { maxBodyBytes: 5 });
    await post(app, '/charges', { amount: 1 }, 'k');
    await post(app, '/charges', { amount: 1 }, 'k');
    expect(app.calls['charges']).toBe(2);
  });
});
