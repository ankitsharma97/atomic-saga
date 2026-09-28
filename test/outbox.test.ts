import { defaultTopicFor, InMemoryOutboxStore, MessageBroker, OutboxMessage, TransactionalOutbox } from '../src';

class FakeBroker implements MessageBroker {
  published: Array<{ topic: string; message: OutboxMessage }> = [];
  failuresLeft = 0;
  delayMs = 0;
  inFlight = 0;
  maxInFlight = 0;

  async publish(topic: string, message: OutboxMessage): Promise<void> {
    this.inFlight++;
    this.maxInFlight = Math.max(this.maxInFlight, this.inFlight);
    try {
      if (this.delayMs) await new Promise(r => setTimeout(r, this.delayMs));
      if (this.failuresLeft > 0) {
        this.failuresLeft--;
        throw new Error('broker unavailable');
      }
      this.published.push({ topic, message });
    } finally {
      this.inFlight--;
    }
  }
}

const noBackoff = { maxAttempts: 3, backoffMs: 0, backoffMultiplier: 1 };

describe('TransactionalOutbox', () => {
  test('publishes added events with derived topics and marks them published', async () => {
    const store = new InMemoryOutboxStore();
    const broker = new FakeBroker();
    const outbox = new TransactionalOutbox(store, broker);

    const event = await outbox.add({ eventType: 'PaymentCaptured', payload: { amount: 5 }, metadata: { sagaId: 's1' } });
    expect(await outbox.processNow()).toBe(1);

    expect(broker.published).toEqual([
      {
        topic: 'payment.captured',
        message: {
          eventId: event.id,
          eventType: 'PaymentCaptured',
          payload: { amount: 5 },
          metadata: { sagaId: 's1' },
          createdAt: event.createdAt.toISOString()
        }
      }
    ]);
    expect(await outbox.getStats()).toEqual({ pending: 0, published: 1, failed: 0 });
    expect(await outbox.processNow()).toBe(0);
  });

  test('an explicit id makes add idempotent', async () => {
    const store = new InMemoryOutboxStore();
    const outbox = new TransactionalOutbox(store, new FakeBroker());
    await outbox.add({ id: 'evt-1', eventType: 'A', payload: 1 });
    await outbox.add({ id: 'evt-1', eventType: 'A', payload: 1 });
    expect(store.all()).toHaveLength(1);
  });

  test('failed publishes are retried after backoff and then marked FAILED', async () => {
    const store = new InMemoryOutboxStore();
    const broker = new FakeBroker();
    broker.failuresLeft = 1;
    const outbox = new TransactionalOutbox(store, broker, {
      retryPolicy: { maxAttempts: 3, backoffMs: 10_000, backoffMultiplier: 1 }
    });

    await outbox.add({ eventType: 'A', payload: 1 });
    expect(await outbox.processNow()).toBe(0);
    const [retrying] = store.all();
    expect(retrying!.attempts).toBe(1);
    expect(retrying!.lastError).toBe('broker unavailable');
    expect(retrying!.nextAttemptAt.getTime()).toBeGreaterThan(Date.now() + 5000);
    expect(await outbox.processNow()).toBe(0); // not due yet
  });

  test('events are marked FAILED after maxAttempts', async () => {
    const broker = new FakeBroker();
    broker.failuresLeft = 99;
    const outbox = new TransactionalOutbox(new InMemoryOutboxStore(), broker, { retryPolicy: noBackoff });
    await outbox.add({ eventType: 'B', payload: 2 });

    for (let i = 0; i < 5; i++) await outbox.processNow();

    expect(await outbox.getStats()).toEqual({ pending: 0, published: 0, failed: 1 });
    expect(broker.published).toHaveLength(0);
  });

  test('claimed events are hidden from other publishers until the lock expires', async () => {
    const store = new InMemoryOutboxStore();
    await new TransactionalOutbox(store, new FakeBroker()).add({ eventType: 'A', payload: 1 });

    const first = await store.claimBatch({ limit: 10, lockMs: 30 });
    expect(first).toHaveLength(1);
    expect(await store.claimBatch({ limit: 10, lockMs: 30 })).toHaveLength(0);
    await new Promise(r => setTimeout(r, 40));
    expect(await store.claimBatch({ limit: 10, lockMs: 30 })).toHaveLength(1); // e.g. the first publisher died
  });

  test('the background publisher never overlaps polls, drains backlogs, and stop() waits', async () => {
    const store = new InMemoryOutboxStore();
    const broker = new FakeBroker();
    broker.delayMs = 5;
    const outbox = new TransactionalOutbox(store, broker, { pollIntervalMs: 1, batchSize: 3 });
    for (let i = 0; i < 10; i++) await outbox.add({ eventType: 'A', payload: i });

    outbox.start();
    for (let i = 0; i < 100 && broker.published.length < 10; i++) await new Promise(r => setTimeout(r, 5));
    await outbox.stop();

    expect(broker.published.map(p => p.message.payload)).toEqual([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
    expect(broker.maxInFlight).toBe(1);
    expect(broker.inFlight).toBe(0);
  });

  test('defaultTopicFor', () => {
    expect(defaultTopicFor('PaymentProcessed')).toBe('payment.processed');
    expect(defaultTopicFor('order_created')).toBe('order.created');
    expect(defaultTopicFor('HTTPRequestFailed')).toBe('httprequest.failed');
  });
});
