/**
 * Checkout service: an HTTP endpoint that reserves stock, charges a card and creates an order —
 * all or nothing — using Postgres for every piece of state.
 *
 * Run it (needs a Postgres database):
 *   DATABASE_URL=postgres://localhost/atomic_saga_test npx tsc -p examples && node examples/dist/examples/checkout.js
 *
 * What it demonstrates:
 * - IdempotencyMiddleware: a client retrying POST /orders with the same Idempotency-Key never double-charges
 * - SagaOrchestrator: a declined card releases the reserved stock; a crash mid-saga is finished by recover()
 * - TransactionalOutbox: the OrderPlaced event is committed in the same transaction as the order row
 * - meta.idempotencyKey: downstream calls (payment, inventory) dedupe retried steps
 */
import express from 'express';
import type { AddressInfo } from 'net';
import { Pool } from 'pg';
import {
  ConsoleLogger,
  IdempotencyMiddleware,
  Logger,
  migrate,
  NonRetryableError,
  PgQueryable,
  PostgresIdempotencyStore,
  PostgresOutboxStore,
  PostgresSagaStore,
  SagaDefinition,
  SagaOrchestrator,
  TransactionalOutbox,
  withTransaction
} from '../src';

// ---------------------------------------------------------------------------
// Fake downstream services. Real ones (Stripe, an inventory API) accept an idempotency key the same way.
// ---------------------------------------------------------------------------

class PaymentService {
  private readonly charges = new Map<string, { id: string; amount: number; refunded: boolean }>();

  async charge(card: string, amount: number, idempotencyKey: string) {
    const existing = this.charges.get(idempotencyKey);
    if (existing) return existing;
    if (card === 'declined') throw new NonRetryableError('Card declined'); // retrying won't help
    const charge = { id: `ch_${this.charges.size + 1}`, amount, refunded: false };
    this.charges.set(idempotencyKey, charge);
    return charge;
  }

  async refund(chargeId: string) {
    for (const charge of this.charges.values()) if (charge.id === chargeId) charge.refunded = true;
  }

  get count() {
    return this.charges.size;
  }
}

class InventoryService {
  stock = new Map([['sku-1', 10]]);
  private readonly reservations = new Map<string, { sku: string; qty: number }>();

  async reserve(sku: string, qty: number, idempotencyKey: string) {
    if (this.reservations.has(idempotencyKey)) return { reservationId: idempotencyKey };
    const available = this.stock.get(sku) ?? 0;
    if (available < qty) throw new NonRetryableError(`Only ${available} of ${sku} left`);
    this.stock.set(sku, available - qty);
    this.reservations.set(idempotencyKey, { sku, qty });
    return { reservationId: idempotencyKey };
  }

  async release(reservationId: string) {
    const reservation = this.reservations.get(reservationId);
    if (!reservation) return; // already released: compensations must be idempotent
    this.stock.set(reservation.sku, (this.stock.get(reservation.sku) ?? 0) + reservation.qty);
    this.reservations.delete(reservationId);
  }
}

// ---------------------------------------------------------------------------
// The saga
// ---------------------------------------------------------------------------

interface CheckoutContext {
  orderId: string;
  sku: string;
  qty: number;
  amount: number;
  card: string;
}

function checkoutSaga(pool: Pool, outbox: TransactionalOutbox<PgQueryable>, payments: PaymentService, inventory: InventoryService) {
  const saga: SagaDefinition<CheckoutContext> = {
    id: 'checkout',
    name: 'Checkout',
    recovery: 'resume', // every step is idempotent, so an interrupted checkout can safely continue
    steps: [
      {
        id: 'reserve-stock',
        name: 'Reserve stock',
        action: (ctx, meta) => inventory.reserve(ctx.sku, ctx.qty, meta.idempotencyKey),
        compensation: async (_ctx, out) => {
          if (out) await inventory.release(out.reservationId);
        }
      },
      {
        id: 'charge-card',
        name: 'Charge card',
        timeout: 10_000,
        retryPolicy: { maxAttempts: 3, backoffMs: 200, backoffMultiplier: 2 },
        action: (ctx, meta) => payments.charge(ctx.card, ctx.amount, meta.idempotencyKey),
        compensation: async (_ctx, out) => {
          if (out) await payments.refund(out.id);
        }
      },
      {
        id: 'create-order',
        name: 'Create order',
        // The order row and its event commit together, or not at all.
        action: (ctx, meta) =>
          withTransaction(pool, async tx => {
            const charge = meta.results['charge-card'] as { id: string };
            await tx.query(
              `INSERT INTO example_orders (id, sku, qty, charge_id) VALUES ($1, $2, $3, $4) ON CONFLICT (id) DO NOTHING`,
              [ctx.orderId, ctx.sku, ctx.qty, charge.id]
            );
            await outbox.add(
              { id: `order-placed:${ctx.orderId}`, eventType: 'OrderPlaced', payload: { orderId: ctx.orderId, chargeId: charge.id } },
              tx
            );
            return { orderId: ctx.orderId };
          })
      }
    ]
  };
  return saga;
}

// ---------------------------------------------------------------------------
// Wiring
// ---------------------------------------------------------------------------

export async function createApp(pool: Pool) {
  await migrate(pool);
  await pool.query(
    `CREATE TABLE IF NOT EXISTS example_orders (id text PRIMARY KEY, sku text NOT NULL, qty int NOT NULL, charge_id text NOT NULL)`
  );

  const logger = new ConsoleLogger();
  const payments = new PaymentService();
  const inventory = new InventoryService();
  const published: string[] = [];

  const sagas = new SagaOrchestrator(new PostgresSagaStore(pool), { logger: quiet(logger) });
  const outbox = new TransactionalOutbox(
    new PostgresOutboxStore(pool),
    { publish: async (topic, message) => void published.push(`${topic}:${message.eventId}`) }, // e.g. Kafka, SNS, RabbitMQ
    { logger: quiet(logger), pollIntervalMs: 100 }
  );
  const saga = checkoutSaga(pool, outbox, payments, inventory);
  sagas.register(saga);

  const app = express();
  app.use(express.json());
  app.use(new IdempotencyMiddleware(new PostgresIdempotencyStore(pool), { required: true, logger }).middleware());

  app.post('/orders', async (req, res, next) => {
    try {
      const { sku, qty, card } = req.body as { sku: string; qty: number; card: string };
      const orderId = `ord_${req.header('idempotency-key')}`;
      const execution = await sagas.executeSaga(saga, { orderId, sku, qty, card, amount: qty * 25 });

      if (execution.status === 'COMPLETED') {
        res.status(201).json({ orderId, status: 'placed' });
      } else if (execution.status === 'COMPENSATED') {
        res.status(402).json({ error: execution.error?.message, status: 'rolled_back' });
      } else {
        // COMPENSATION_FAILED: something could not be undone. Alert a human.
        res.status(500).json({ error: 'Order failed and could not be fully rolled back', executionId: execution.id });
      }
    } catch (error) {
      next(error); // store failure or lost lease: 5xx releases the idempotency key; recover() finishes the saga
    }
  });

  outbox.start();
  sagas.startRecovery();

  return {
    app,
    payments,
    inventory,
    published,
    stop: () => Promise.all([outbox.stop(), sagas.stopRecovery()])
  };
}

/** Only warnings and errors, to keep the demo output readable. */
function quiet(logger: ConsoleLogger): Logger {
  return {
    info: () => {},
    debug: () => {},
    warn: (message, meta) => logger.warn(message, meta),
    error: (message, error, meta) => logger.error(message, error, meta)
  };
}

// ---------------------------------------------------------------------------
// Demo
// ---------------------------------------------------------------------------

async function demo() {
  const pool = new Pool({ connectionString: process.env['DATABASE_URL'] ?? 'postgres://localhost/atomic_saga_test' });
  await pool.query(
    'TRUNCATE saga_executions, idempotency_keys, outbox_events; DROP TABLE IF EXISTS example_orders'
  ).catch(() => {}); // fresh demo state (tables may not exist yet)

  const service = await createApp(pool);
  const server = service.app.listen(0);
  await new Promise(resolve => server.once('listening', resolve));
  const base = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;

  const order = (key: string, body: object) =>
    fetch(`${base}/orders`, {
      method: 'POST',
      headers: { 'content-type': 'application/json', 'idempotency-key': key },
      body: JSON.stringify(body)
    }).then(async r => ({ status: r.status, replayed: r.headers.get('idempotent-replayed') === 'true', body: await r.json() }));

  console.log('1. Place an order:            ', await order('a1', { sku: 'sku-1', qty: 2, card: 'ok' }));
  console.log('2. Client retries (same key): ', await order('a1', { sku: 'sku-1', qty: 2, card: 'ok' }));
  console.log('3. Same key, different body:  ', await order('a1', { sku: 'sku-1', qty: 9, card: 'ok' }));
  console.log('4. Declined card:             ', await order('b1', { sku: 'sku-1', qty: 3, card: 'declined' }));

  await new Promise(r => setTimeout(r, 300)); // let the outbox publish
  console.log('\nCharges made:   ', service.payments.count, '(the retry did not charge again)');
  console.log('Stock left:     ', service.inventory.stock.get('sku-1'), '(10 - 2; the declined order released its 3)');
  console.log('Events published:', service.published);

  server.closeAllConnections();
  server.close();
  await service.stop();
  await pool.end();
}

if (require.main === module) {
  demo().catch(error => {
    console.error(error);
    process.exit(1);
  });
}
