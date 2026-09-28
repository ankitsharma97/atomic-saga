import { randomUUID } from 'crypto';
import { hostname } from 'os';
import {
  ListExecutionsFilter,
  Logger,
  RetryPolicy,
  SagaDefinition,
  SagaExecution,
  SagaStore,
  SerializedError,
  StepMeta,
  StepResult,
  TERMINAL_SAGA_STATUSES,
  TransactionStep
} from '../types';
import { LeaseLostError, NonRetryableError, serializeError, StepTimeoutError, toError } from '../utils/errors';
import { backoffDelay, DEFAULT_RETRY_POLICY, NoopLogger, sleep } from '../utils/helpers';

export interface SagaOrchestratorOptions {
  logger?: Logger;
  defaultRetryPolicy?: RetryPolicy;
  /** Default per-attempt timeout for steps and compensations, in ms. Default 30000. */
  defaultTimeout?: number;
  /** Identifies this process as the owner of the executions it runs. Default `<hostname>:<pid>:<random>`. */
  workerId?: string;
  /**
   * An active execution whose owner has not written or heartbeated for this long is considered
   * abandoned and can be claimed by `recover()`. Default 60000.
   */
  staleAfterMs?: number;
  /** How often a running execution heartbeats. Default `staleAfterMs / 3`. */
  heartbeatIntervalMs?: number;
}

type StepOutcome = { ok: true } | { ok: false; error: SerializedError };

/**
 * Saga orchestrator: runs steps in order and, if one fails, runs the compensations of the
 * completed steps in reverse order. Every state change is persisted, so an execution
 * interrupted by a crash can be picked up by `recover()` on any worker.
 */
export class SagaOrchestrator {
  readonly workerId: string;
  private readonly store: SagaStore;
  private readonly logger: Logger;
  private readonly defaultRetryPolicy: RetryPolicy;
  private readonly defaultTimeout: number;
  private readonly staleAfterMs: number;
  private readonly heartbeatIntervalMs: number;
  private readonly definitions = new Map<string, SagaDefinition>();
  private recoveryTimer: NodeJS.Timeout | undefined;
  private recoveryRun: Promise<unknown> | undefined;
  private recoveryActive = false;

  constructor(store: SagaStore, options: SagaOrchestratorOptions = {}) {
    this.store = store;
    this.logger = options.logger ?? new NoopLogger();
    this.defaultRetryPolicy = options.defaultRetryPolicy ?? DEFAULT_RETRY_POLICY;
    this.defaultTimeout = options.defaultTimeout ?? 30000;
    this.workerId = options.workerId ?? `${hostname()}:${process.pid}:${randomUUID().slice(0, 8)}`;
    this.staleAfterMs = options.staleAfterMs ?? 60000;
    this.heartbeatIntervalMs = options.heartbeatIntervalMs ?? Math.max(1, Math.floor(this.staleAfterMs / 3));
  }

  /**
   * Register a saga definition so interrupted executions of it can be recovered.
   * `executeSaga` registers automatically, but a freshly started process must register
   * its definitions before calling `recover()`.
   */
  register(definition: SagaDefinition<any>): this {
    validateDefinition(definition);
    this.definitions.set(definition.id, definition);
    return this;
  }

  /**
   * Run a saga to completion. Resolves with the final execution, whose status is COMPLETED,
   * COMPENSATED or COMPENSATION_FAILED. Rejects only on infrastructure errors (store failures,
   * lost ownership); in that case the execution stays active and `recover()` will finish it.
   */
  async executeSaga<TContext = any>(
    definition: SagaDefinition<TContext>,
    context: TContext,
    options: { executionId?: string } = {}
  ): Promise<SagaExecution> {
    this.register(definition);

    const now = new Date();
    const execution: SagaExecution = {
      id: options.executionId ?? randomUUID(),
      sagaId: definition.id,
      status: 'PENDING',
      context,
      stepResults: [],
      owner: this.workerId,
      startedAt: now,
      updatedAt: now
    };

    this.logger.info('Starting saga execution', {
      executionId: execution.id,
      sagaId: definition.id,
      stepCount: definition.steps.length
    });

    await this.store.saveExecution(execution);
    return this.run(definition, execution);
  }

  /**
   * Claim executions abandoned by crashed or stuck workers and drive them to a terminal state.
   * Safe to call concurrently from many workers: each execution is claimed by exactly one.
   */
  async recover(options: { limit?: number } = {}): Promise<SagaExecution[]> {
    const claimed = await this.store.claimStaleExecutions({
      owner: this.workerId,
      staleAfterMs: this.staleAfterMs,
      limit: options.limit ?? 10
    });

    const finished: SagaExecution[] = [];
    for (const execution of claimed) {
      const definition = this.definitions.get(execution.sagaId);
      if (!definition) {
        this.logger.warn('Cannot recover execution: saga definition not registered', {
          executionId: execution.id,
          sagaId: execution.sagaId
        });
        continue;
      }

      this.logger.info('Recovering saga execution', {
        executionId: execution.id,
        sagaId: execution.sagaId,
        status: execution.status,
        completedSteps: execution.stepResults.length
      });

      if (execution.status !== 'COMPENSATING') {
        const mismatch = execution.stepResults.findIndex((r, i) => definition.steps[i]?.id !== r.stepId);
        if (mismatch !== -1) {
          this.markForCompensation(execution, 'Saga definition changed since the execution started');
        } else if ((definition.recovery ?? 'resume') === 'compensate') {
          this.markForCompensation(execution, 'Saga was interrupted and its recovery strategy is "compensate"');
        }
      }

      try {
        finished.push(await this.run(definition, execution));
      } catch (error) {
        this.logger.error('Failed to recover saga execution', toError(error), { executionId: execution.id });
      }
    }
    return finished;
  }

  /** Call `recover()` periodically until `stopRecovery()`. */
  startRecovery(intervalMs: number = Math.max(1, Math.floor(this.staleAfterMs / 2))): void {
    if (this.recoveryActive) return;
    this.recoveryActive = true;

    const tick = async () => {
      try {
        await this.recover();
      } catch (error) {
        this.logger.error('Saga recovery pass failed', toError(error));
      }
    };
    const schedule = () => {
      if (!this.recoveryActive) return;
      this.recoveryTimer = setTimeout(() => {
        this.recoveryRun = tick().finally(() => {
          this.recoveryRun = undefined;
          schedule();
        });
      }, intervalMs);
      this.recoveryTimer.unref?.();
    };
    schedule();
  }

  /** Stop the recovery loop and wait for an in-flight pass to finish. */
  async stopRecovery(): Promise<void> {
    this.recoveryActive = false;
    if (this.recoveryTimer) clearTimeout(this.recoveryTimer);
    this.recoveryTimer = undefined;
    await this.recoveryRun;
  }

  async getExecution(id: string): Promise<SagaExecution | null> {
    return this.store.getExecution(id);
  }

  async listExecutions(filter?: ListExecutionsFilter): Promise<SagaExecution[]> {
    return this.store.listExecutions(filter);
  }

  // -------------------------------------------------------------------------
  // Execution
  // -------------------------------------------------------------------------

  private async run(definition: SagaDefinition, execution: SagaExecution): Promise<SagaExecution> {
    const lease = this.startHeartbeat(execution);
    try {
      if (execution.status === 'PENDING' || execution.status === 'RUNNING') {
        const outcome = await this.runSteps(definition, execution, lease.signal);
        if (outcome.ok) {
          execution.status = 'COMPLETED';
          execution.completedAt = new Date();
          delete execution.currentStep;
          await this.persist(execution);
          this.logger.info('Saga completed', {
            executionId: execution.id,
            sagaId: definition.id,
            durationMs: execution.completedAt.getTime() - execution.startedAt.getTime()
          });
          await this.runHook('onSuccess', execution, () => definition.onSuccess?.(execution.context, execution));
          return execution;
        }
        execution.error = outcome.error;
      }

      if (TERMINAL_SAGA_STATUSES.includes(execution.status)) {
        return execution;
      }

      const compensated = await this.compensate(definition, execution, lease.signal);

      if (compensated) {
        const error = execution.error ?? { name: 'Error', message: 'Saga failed' };
        await this.runHook('onFailure', execution, () => definition.onFailure?.(execution.context, error, execution));
      } else {
        await this.runHook('onCompensationFailed', execution, () =>
          definition.onCompensationFailed?.(execution.context, execution)
        );
      }
      return execution;
    } finally {
      lease.stop();
    }
  }

  private async runSteps(definition: SagaDefinition, execution: SagaExecution, lease: AbortSignal): Promise<StepOutcome> {
    const last = execution.stepResults[execution.stepResults.length - 1];
    if (last?.status === 'FAILED') {
      // Crashed after recording the failure but before compensating.
      return { ok: false, error: last.error ?? { name: 'Error', message: `Step ${last.stepId} failed` } };
    }

    execution.status = 'RUNNING';

    for (let i = execution.stepResults.length; i < definition.steps.length; i++) {
      const step = definition.steps[i]!;
      execution.currentStep = i;
      await this.persist(execution);

      this.logger.info(`Executing step ${i + 1}/${definition.steps.length}`, {
        executionId: execution.id,
        stepId: step.id
      });

      const result = await this.executeStep(step, execution, lease);
      execution.stepResults.push(result);
      await this.persist(execution);

      if (result.status === 'FAILED') {
        return { ok: false, error: result.error! };
      }
    }
    return { ok: true };
  }

  private async executeStep(step: TransactionStep, execution: SagaExecution, lease: AbortSignal): Promise<StepResult> {
    const result: StepResult = {
      stepId: step.id,
      stepName: step.name,
      status: 'SUCCESS',
      startedAt: new Date(),
      attempts: 0
    };

    const outcome = await this.withRetries(
      step.retryPolicy ?? this.defaultRetryPolicy,
      step.timeout ?? this.defaultTimeout,
      lease,
      (attempt, signal) =>
        step.action(execution.context, this.meta(execution, step.id, attempt, signal, `${execution.id}:${step.id}`)),
      { executionId: execution.id, stepId: step.id, phase: 'action' }
    );

    result.attempts = outcome.attempts;
    result.completedAt = new Date();
    if (outcome.ok) {
      if (outcome.value !== undefined) result.output = outcome.value;
    } else {
      result.status = 'FAILED';
      result.error = serializeError(outcome.error);
      this.logger.error(`Step failed after ${outcome.attempts} attempt(s)`, outcome.error, {
        executionId: execution.id,
        stepId: step.id
      });
    }
    return result;
  }

  /** Returns true if every compensation succeeded. */
  private async compensate(definition: SagaDefinition, execution: SagaExecution, lease: AbortSignal): Promise<boolean> {
    execution.status = 'COMPENSATING';
    await this.persist(execution);

    this.logger.info('Compensating saga', { executionId: execution.id, sagaId: definition.id });

    for (let i = execution.stepResults.length - 1; i >= 0; i--) {
      const result = execution.stepResults[i]!;
      if (result.status !== 'SUCCESS' && result.status !== 'COMPENSATION_FAILED') continue;

      const step = definition.steps.find(s => s.id === result.stepId);
      if (!step?.compensation) {
        this.logger.debug('Step has no compensation, skipping', { executionId: execution.id, stepId: result.stepId });
        continue;
      }
      const compensation = step.compensation;

      const outcome = await this.withRetries(
        step.compensationRetryPolicy ?? step.retryPolicy ?? this.defaultRetryPolicy,
        step.timeout ?? this.defaultTimeout,
        lease,
        (attempt, signal) =>
          compensation(
            execution.context,
            result.output,
            this.meta(execution, step.id, attempt, signal, `${execution.id}:${step.id}:compensate`)
          ),
        { executionId: execution.id, stepId: step.id, phase: 'compensation' }
      );

      result.compensationAttempts = (result.compensationAttempts ?? 0) + outcome.attempts;
      if (outcome.ok) {
        result.status = 'COMPENSATED';
        delete result.compensationError;
      } else {
        result.status = 'COMPENSATION_FAILED';
        result.compensationError = serializeError(outcome.error);
        this.logger.error('Compensation failed; manual intervention required', outcome.error, {
          executionId: execution.id,
          stepId: step.id
        });
      }
      await this.persist(execution);
    }

    const failed = execution.stepResults.some(r => r.status === 'COMPENSATION_FAILED');
    execution.status = failed ? 'COMPENSATION_FAILED' : 'COMPENSATED';
    execution.completedAt = new Date();
    delete execution.currentStep;
    await this.persist(execution);

    this.logger.info(`Saga ${failed ? 'compensation failed' : 'compensated'}`, {
      executionId: execution.id,
      sagaId: definition.id
    });
    return !failed;
  }

  /** Run `fn` with timeout and retries. Lease loss is rethrown instead of being treated as a step failure. */
  private async withRetries<T>(
    policy: RetryPolicy,
    timeoutMs: number,
    lease: AbortSignal,
    fn: (attempt: number, signal: AbortSignal) => Promise<T>,
    logMeta: Record<string, unknown>
  ): Promise<{ ok: true; value: T; attempts: number } | { ok: false; error: Error; attempts: number }> {
    const maxAttempts = Math.max(1, policy.maxAttempts);
    const retryable = policy.retryable ?? ((error: Error) => !(error instanceof NonRetryableError));

    for (let attempt = 1; ; attempt++) {
      if (lease.aborted) throw lease.reason;
      try {
        const value = await runWithTimeout(signal => fn(attempt, signal), timeoutMs, lease);
        return { ok: true, value, attempts: attempt };
      } catch (raw) {
        if (lease.aborted) throw lease.reason;
        const error = toError(raw);
        this.logger.warn(`Attempt ${attempt}/${maxAttempts} failed`, { ...logMeta, attempt, error: error.message });
        if (attempt >= maxAttempts || !retryable(error)) {
          return { ok: false, error, attempts: attempt };
        }
        await sleep(backoffDelay(policy, attempt));
      }
    }
  }

  private meta(execution: SagaExecution, stepId: string, attempt: number, signal: AbortSignal, idempotencyKey: string): StepMeta {
    const results: Record<string, unknown> = {};
    for (const r of execution.stepResults) {
      if (r.status !== 'FAILED') results[r.stepId] = r.output;
    }
    return { executionId: execution.id, sagaId: execution.sagaId, stepId, attempt, idempotencyKey, signal, results };
  }

  private markForCompensation(execution: SagaExecution, reason: string): void {
    execution.status = 'COMPENSATING';
    execution.error ??= { name: 'SagaInterruptedError', message: reason };
  }

  private async persist(execution: SagaExecution): Promise<void> {
    execution.owner = this.workerId;
    execution.updatedAt = new Date();
    if (!(await this.store.updateExecution(execution))) {
      throw new LeaseLostError(execution.id);
    }
  }

  private startHeartbeat(execution: SagaExecution): { signal: AbortSignal; stop: () => void } {
    const controller = new AbortController();
    const timer = setInterval(() => {
      this.store.heartbeat(execution.id, this.workerId).then(
        owned => {
          if (!owned && !controller.signal.aborted) {
            this.logger.warn('Lost ownership of saga execution', { executionId: execution.id });
            controller.abort(new LeaseLostError(execution.id));
          }
        },
        error => this.logger.warn('Saga heartbeat failed', { executionId: execution.id, error: toError(error).message })
      );
    }, this.heartbeatIntervalMs);
    timer.unref?.();
    return { signal: controller.signal, stop: () => clearInterval(timer) };
  }

  private async runHook(name: string, execution: SagaExecution, hook: () => Promise<void> | undefined): Promise<void> {
    try {
      await hook();
    } catch (error) {
      // The saga's outcome is already persisted; a failing hook must not change it.
      this.logger.error(`Saga ${name} hook threw`, toError(error), { executionId: execution.id });
    }
  }
}

/** Run `fn`, aborting its signal and rejecting on timeout or when `parent` aborts. */
function runWithTimeout<T>(fn: (signal: AbortSignal) => Promise<T>, timeoutMs: number, parent: AbortSignal): Promise<T> {
  const controller = new AbortController();
  return new Promise<T>((resolve, reject) => {
    const onParentAbort = () => fail(parent.reason);
    const timer = setTimeout(() => fail(new StepTimeoutError(timeoutMs)), timeoutMs);
    parent.addEventListener('abort', onParentAbort, { once: true });

    let settled = false;
    function cleanup() {
      settled = true;
      clearTimeout(timer);
      parent.removeEventListener('abort', onParentAbort);
    }
    function fail(error: unknown) {
      if (settled) return;
      cleanup();
      controller.abort(error);
      reject(error);
    }

    Promise.resolve()
      .then(() => fn(controller.signal))
      .then(
        value => {
          if (settled) return;
          cleanup();
          resolve(value);
        },
        error => fail(error)
      );
  });
}

export function validateDefinition(definition: SagaDefinition<any>): void {
  if (!definition.id || !definition.name) {
    throw new Error('Saga definition requires an id and a name');
  }
  if (!Array.isArray(definition.steps) || definition.steps.length === 0) {
    throw new Error(`Saga "${definition.id}" must have at least one step`);
  }
  const ids = new Set<string>();
  for (const step of definition.steps) {
    if (!step.id || !step.name || typeof step.action !== 'function') {
      throw new Error(`Saga "${definition.id}" has an invalid step: each step needs an id, a name and an action`);
    }
    if (ids.has(step.id)) {
      throw new Error(`Saga "${definition.id}" has duplicate step id "${step.id}"`);
    }
    ids.add(step.id);
  }
}
