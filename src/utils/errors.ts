import { SerializedError } from '../types';

/** Throw from a step to fail it immediately, without further retries. */
export class NonRetryableError extends Error {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message);
    this.name = 'NonRetryableError';
    if (options?.cause !== undefined) {
      (this as { cause?: unknown }).cause = options.cause;
    }
  }
}

/** Raised when a step attempt exceeds its timeout. */
export class StepTimeoutError extends Error {
  constructor(timeoutMs: number) {
    super(`Operation timed out after ${timeoutMs}ms`);
    this.name = 'StepTimeoutError';
  }
}

/** Raised when another worker took over an execution this worker was running. */
export class LeaseLostError extends Error {
  constructor(executionId: string) {
    super(`Lost ownership of saga execution ${executionId}`);
    this.name = 'LeaseLostError';
  }
}

export function toError(value: unknown): Error {
  if (value instanceof Error) return value;
  return new Error(typeof value === 'string' ? value : JSON.stringify(value));
}

export function serializeError(value: unknown): SerializedError {
  const error = toError(value);
  const serialized: SerializedError = { name: error.name, message: error.message };
  if (error.stack !== undefined) serialized.stack = error.stack;
  return serialized;
}
