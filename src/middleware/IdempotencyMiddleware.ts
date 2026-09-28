import { createHash, randomUUID } from 'crypto';
import type { IncomingMessage, ServerResponse } from 'http';
import { IdempotencyStore, Logger, StoredResponse } from '../types';
import { toError } from '../utils/errors';
import { NoopLogger } from '../utils/helpers';

/** The parts of an Express/Connect request the middleware uses. A plain `http` request works too. */
export interface IdempotencyRequest extends IncomingMessage {
  body?: unknown;
  path?: string;
  baseUrl?: string;
}

export type IdempotencyHandler = (req: IdempotencyRequest, res: ServerResponse, next: (error?: unknown) => void) => void;

export interface IdempotencyOptions {
  logger?: Logger;
  /** Request header carrying the key. Default `Idempotency-Key`. */
  header?: string;
  /** Methods the middleware applies to. Default POST and PATCH. */
  methods?: string[];
  /** Reject requests without a key (400) instead of passing them through. Default false. */
  required?: boolean;
  /** How long a completed response is kept for replay. Default 24h. */
  ttlMs?: number;
  /**
   * How long a key stays locked while its request is processing. Concurrent requests with the key
   * get 409 until then; after it, the key can be taken over (e.g. the first request's process died).
   * Set it above your slowest request. Default 60s.
   */
  lockMs?: number;
  /** Namespace keys, e.g. per user or tenant: `req => req.user.id`. */
  scope?: (req: IdempotencyRequest) => string | undefined;
  /** Which responses are stored for replay. Others release the key so the client can retry. Default: status < 500. */
  shouldStore?: (statusCode: number) => boolean;
  /** Responses larger than this are not stored (the key is released). Default 1 MiB. */
  maxBodyBytes?: number;
  /** When the store is unreachable: true passes the request through unprotected, false answers 503. Default false. */
  failOpen?: boolean;
}

const NOT_REPLAYED_HEADERS = new Set([
  'connection',
  'content-length',
  'date',
  'keep-alive',
  'set-cookie',
  'transfer-encoding'
]);

/**
 * Idempotency middleware (Stripe-style semantics):
 * - first request with a key runs normally; its response is stored
 * - a retry with the same key and same request gets the stored response replayed (`Idempotent-Replayed: true`)
 * - a retry while the first is still processing gets 409
 * - the same key with a different request (method, path or body) gets 422
 * - if the request fails (5xx, or the connection dropped) the key is released so the client can retry
 *
 * Mount it after your body parser so the body is part of the request fingerprint.
 */
export class IdempotencyMiddleware {
  private readonly store: IdempotencyStore;
  private readonly logger: Logger;
  private readonly header: string;
  private readonly methods: Set<string>;
  private readonly required: boolean;
  private readonly ttlMs: number;
  private readonly lockMs: number;
  private readonly scope: ((req: IdempotencyRequest) => string | undefined) | undefined;
  private readonly shouldStore: (statusCode: number) => boolean;
  private readonly maxBodyBytes: number;
  private readonly failOpen: boolean;

  constructor(store: IdempotencyStore, options: IdempotencyOptions = {}) {
    this.store = store;
    this.logger = options.logger ?? new NoopLogger();
    this.header = (options.header ?? 'Idempotency-Key').toLowerCase();
    this.methods = new Set((options.methods ?? ['POST', 'PATCH']).map(m => m.toUpperCase()));
    this.required = options.required ?? false;
    this.ttlMs = options.ttlMs ?? 24 * 60 * 60 * 1000;
    this.lockMs = options.lockMs ?? 60 * 1000;
    this.scope = options.scope;
    this.shouldStore = options.shouldStore ?? (status => status < 500);
    this.maxBodyBytes = options.maxBodyBytes ?? 1024 * 1024;
    this.failOpen = options.failOpen ?? false;
  }

  middleware(): IdempotencyHandler {
    return (req, res, next) => {
      this.handle(req, res, next).catch(next);
    };
  }

  /** Generate a random idempotency key (for clients). */
  generateKey(): string {
    return randomUUID();
  }

  private async handle(req: IdempotencyRequest, res: ServerResponse, next: (error?: unknown) => void): Promise<void> {
    const method = (req.method ?? 'GET').toUpperCase();
    if (!this.methods.has(method)) return next();

    const rawKey = req.headers[this.header];
    const key = Array.isArray(rawKey) ? rawKey[0] : rawKey;
    if (!key) {
      if (this.required) return sendJson(res, 400, { error: 'idempotency_key_required', header: this.header });
      return next();
    }
    if (key.length > 255) {
      return sendJson(res, 400, { error: 'idempotency_key_invalid', message: 'Key must be at most 255 characters' });
    }

    const scope = this.scope?.(req);
    const storeKey = scope ? `${scope}:${key}` : key;
    const fingerprint = fingerprintRequest(method, req);

    let begin;
    try {
      begin = await this.store.begin(storeKey, fingerprint, { lockMs: this.lockMs, ttlMs: this.ttlMs });
    } catch (error) {
      this.logger.error('Idempotency store unavailable', toError(error), { key: storeKey });
      if (this.failOpen) return next();
      return sendJson(res, 503, { error: 'idempotency_unavailable' });
    }

    if (!begin.acquired) {
      const { record } = begin;
      if (record.fingerprint !== fingerprint) {
        return sendJson(res, 422, {
          error: 'idempotency_key_reused',
          message: 'This idempotency key was already used for a different request'
        });
      }
      if (record.status === 'COMPLETED' && record.response) {
        this.logger.debug('Replaying stored response', { key: storeKey });
        return replay(res, record.response);
      }
      const retryAfter = Math.max(1, Math.ceil((record.lockedUntil.getTime() - Date.now()) / 1000));
      res.setHeader('Retry-After', String(retryAfter));
      return sendJson(res, 409, {
        error: 'idempotency_request_in_progress',
        message: 'A request with this idempotency key is still being processed'
      });
    }

    this.captureResponse(res, storeKey);
    next();
  }

  /**
   * Buffer what the handler writes. When it ends the response, store the response (or release the key)
   * *before* the last bytes go out, so a client that retries as soon as it has the response gets a replay.
   */
  private captureResponse(res: ServerResponse, key: string): void {
    const chunks: Buffer[] = [];
    let size = 0;
    let overflow = false;

    const collect = (chunk: unknown, encoding?: unknown) => {
      if (chunk == null || typeof chunk === 'function' || overflow) return;
      const buffer = typeof chunk === 'string'
        ? Buffer.from(chunk, typeof encoding === 'string' ? (encoding as BufferEncoding) : 'utf8')
        : Buffer.from(chunk as Uint8Array);
      size += buffer.length;
      if (size > this.maxBodyBytes) {
        overflow = true;
        chunks.length = 0;
      } else {
        chunks.push(buffer);
      }
    };

    const release = () =>
      this.store.release(key).catch(error => this.logger.error('Failed to release idempotency key', toError(error), { key }));

    let settled = false;
    const originalWrite = res.write;
    const originalEnd = res.end;

    res.write = function (this: ServerResponse, chunk: unknown, ...rest: unknown[]) {
      collect(chunk, rest[0]);
      return (originalWrite as Function).call(this, chunk, ...rest);
    } as typeof res.write;

    res.end = ((chunk?: unknown, ...rest: unknown[]) => {
      const finish = () => (originalEnd as Function).call(res, chunk, ...rest);
      if (settled) return finish();
      settled = true;
      collect(chunk, rest[0]);

      if (overflow || !this.shouldStore(res.statusCode)) {
        void release();
        return finish();
      }

      const response: StoredResponse = {
        statusCode: res.statusCode,
        headers: replayableHeaders(res),
        body: Buffer.concat(chunks).toString('base64')
      };
      this.store
        .complete(key, response)
        .catch(error => {
          // The key stays locked until lockMs passes; retries get 409 meanwhile.
          this.logger.error('Failed to store idempotent response', toError(error), { key });
        })
        .finally(finish);
      return res;
    }) as typeof res.end;

    // Connection dropped before the handler responded: let the client retry.
    res.once('close', () => {
      if (settled) return;
      settled = true;
      void release();
    });
  }
}

function fingerprintRequest(method: string, req: IdempotencyRequest): string {
  const path = req.path !== undefined ? `${req.baseUrl ?? ''}${req.path}` : (req.url ?? '').split('?')[0];
  const body = req.body === undefined ? '' : stableStringify(req.body);
  return createHash('sha256').update(`${method} ${path}\n${body}`).digest('hex');
}

/** JSON.stringify with sorted object keys, so clients reordering keys still match. */
function stableStringify(value: unknown): string {
  if (Buffer.isBuffer(value)) return value.toString('base64');
  if (value === null || typeof value !== 'object') return JSON.stringify(value) ?? 'null';
  if (Array.isArray(value)) return `[${value.map(stableStringify).join(',')}]`;
  const entries = Object.keys(value as object)
    .sort()
    .filter(k => (value as Record<string, unknown>)[k] !== undefined)
    .map(k => `${JSON.stringify(k)}:${stableStringify((value as Record<string, unknown>)[k])}`);
  return `{${entries.join(',')}}`;
}

function replayableHeaders(res: ServerResponse): Record<string, string> {
  const headers: Record<string, string> = {};
  for (const [name, value] of Object.entries(res.getHeaders())) {
    if (value === undefined || NOT_REPLAYED_HEADERS.has(name.toLowerCase())) continue;
    headers[name] = Array.isArray(value) ? value.join(', ') : String(value);
  }
  return headers;
}

function replay(res: ServerResponse, response: StoredResponse): void {
  res.statusCode = response.statusCode;
  for (const [name, value] of Object.entries(response.headers)) {
    res.setHeader(name, value);
  }
  res.setHeader('Idempotent-Replayed', 'true');
  res.end(Buffer.from(response.body, 'base64'));
}

function sendJson(res: ServerResponse, statusCode: number, body: unknown): void {
  res.statusCode = statusCode;
  res.setHeader('Content-Type', 'application/json; charset=utf-8');
  res.end(JSON.stringify(body));
}
