import {
  InMemorySagaStore,
  LeaseLostError,
  NonRetryableError,
  SagaDefinition,
  SagaExecution,
  SagaOrchestrator,
  StepMeta,
  TransactionStep
} from '../src';

const fast = { maxAttempts: 3, backoffMs: 1, backoffMultiplier: 1 };

function step(id: string, overrides: Partial<TransactionStep> = {}): TransactionStep {
  return { id, name: id, action: async () => ({ [id]: true }), ...overrides };
}

function orchestrator(store = new InMemorySagaStore(), options: ConstructorParameters<typeof SagaOrchestrator>[1] = {}) {
  return new SagaOrchestrator(store, { defaultRetryPolicy: fast, ...options });
}

describe('SagaOrchestrator', () => {
  test('runs steps in order, passing earlier outputs, and persists COMPLETED', async () => {
    const store = new InMemorySagaStore();
    const seen: Array<Record<string, unknown>> = [];
    const saga: SagaDefinition<{ amount: number }> = {
      id: 'checkout',
      name: 'Checkout',
      steps: [
        step('charge', { action: async ctx => ({ chargeId: `ch_${ctx.amount}` }) }),
        step('reserve', {
          action: async (_ctx, meta: StepMeta) => {
            seen.push(meta.results);
            return { reservationId: 'r1' };
          }
        })
      ]
    };

    const result = await orchestrator(store).executeSaga(saga, { amount: 100 });

    expect(result.status).toBe('COMPLETED');
    expect(result.stepResults.map(r => r.status)).toEqual(['SUCCESS', 'SUCCESS']);
    expect(seen).toEqual([{ charge: { chargeId: 'ch_100' } }]);
    const stored = await store.getExecution(result.id);
    expect(stored?.status).toBe('COMPLETED');
    expect(stored?.completedAt).toBeInstanceOf(Date);
  });

  test('passes a stable idempotency key per step', async () => {
    const keys: string[] = [];
    let calls = 0;
    const saga: SagaDefinition = {
      id: 's',
      name: 's',
      steps: [
        step('a', {
          action: async (_ctx, meta) => {
            keys.push(meta.idempotencyKey);
            if (++calls < 3) throw new Error('flaky');
          }
        })
      ]
    };
    const result = await orchestrator().executeSaga(saga, {});
    expect(result.status).toBe('COMPLETED');
    expect(result.stepResults[0]!.attempts).toBe(3);
    expect(new Set(keys)).toEqual(new Set([`${result.id}:a`]));
  });

  test('on failure, compensates completed steps in reverse order with their outputs', async () => {
    const calls: string[] = [];
    const onFailure = jest.fn(async () => {});
    const saga: SagaDefinition = {
      id: 's',
      name: 's',
      onFailure,
      steps: [
        step('a', { action: async () => 'out-a', compensation: async (_c, out) => void calls.push(`undo-a:${out}`) }),
        step('b', { action: async () => 'out-b', compensation: async (_c, out) => void calls.push(`undo-b:${out}`) }),
        step('c', {
          action: async () => {
            throw new Error('card declined');
          },
          compensation: async () => void calls.push('undo-c')
        })
      ]
    };

    const result = await orchestrator().executeSaga(saga, {});

    expect(result.status).toBe('COMPENSATED');
    expect(calls).toEqual(['undo-b:out-b', 'undo-a:out-a']); // the failed step is not compensated
    expect(result.error?.message).toBe('card declined');
    expect(result.stepResults.map(r => r.status)).toEqual(['COMPENSATED', 'COMPENSATED', 'FAILED']);
    expect(onFailure).toHaveBeenCalledWith({}, expect.objectContaining({ message: 'card declined' }), expect.anything());
  });

  test('a compensation that keeps failing marks the saga COMPENSATION_FAILED but still undoes the rest', async () => {
    const undone: string[] = [];
    let undoBAttempts = 0;
    const onCompensationFailed = jest.fn(async () => {});
    const onFailure = jest.fn(async () => {});
    const saga: SagaDefinition = {
      id: 's',
      name: 's',
      onFailure,
      onCompensationFailed,
      steps: [
        step('a', { compensation: async () => void undone.push('a') }),
        step('b', {
          compensation: async () => {
            undoBAttempts++;
            throw new Error('refund API down');
          }
        }),
        step('c', {
          action: async () => {
            throw new NonRetryableError('boom');
          }
        })
      ]
    };

    const result = await orchestrator().executeSaga(saga, {});

    expect(result.status).toBe('COMPENSATION_FAILED');
    expect(undoBAttempts).toBe(3);
    expect(undone).toEqual(['a']);
    const b = result.stepResults[1]!;
    expect(b.status).toBe('COMPENSATION_FAILED');
    expect(b.compensationError?.message).toBe('refund API down');
    expect(onCompensationFailed).toHaveBeenCalledTimes(1);
    expect(onFailure).not.toHaveBeenCalled();
  });

  test('NonRetryableError and retryable() stop retries immediately', async () => {
    let attempts = 0;
    const saga: SagaDefinition = {
      id: 's',
      name: 's',
      steps: [
        step('a', {
          action: async () => {
            attempts++;
            throw new NonRetryableError('invalid card');
          }
        })
      ]
    };
    const result = await orchestrator().executeSaga(saga, {});
    expect(attempts).toBe(1);
    expect(result.stepResults[0]!.attempts).toBe(1);

    attempts = 0;
    const custom: SagaDefinition = {
      id: 's2',
      name: 's2',
      steps: [
        step('a', {
          retryPolicy: { ...fast, maxAttempts: 5, retryable: e => e.message !== 'fatal' },
          action: async () => {
            attempts++;
            throw new Error('fatal');
          }
        })
      ]
    };
    await orchestrator().executeSaga(custom, {});
    expect(attempts).toBe(1);
  });

  test('timeouts abort the attempt signal and are retried', async () => {
    const aborted: boolean[] = [];
    let attempt = 0;
    const saga: SagaDefinition = {
      id: 's',
      name: 's',
      steps: [
        step('a', {
          timeout: 20,
          action: (_ctx, meta) => {
            attempt++;
            if (attempt === 1) {
              return new Promise(resolve => {
                meta.signal.addEventListener('abort', () => {
                  aborted.push(true);
                  resolve('too late');
                });
              });
            }
            return Promise.resolve('ok');
          }
        })
      ]
    };

    const result = await orchestrator().executeSaga(saga, {});
    expect(result.status).toBe('COMPLETED');
    expect(aborted).toEqual([true]);
    expect(result.stepResults[0]!.output).toBe('ok');
    expect(result.stepResults[0]!.attempts).toBe(2);
  });

  test('a throwing onSuccess hook does not change the outcome', async () => {
    const saga: SagaDefinition = {
      id: 's',
      name: 's',
      steps: [step('a', { compensation: jest.fn() })],
      onSuccess: async () => {
        throw new Error('email failed');
      }
    };
    const result = await orchestrator().executeSaga(saga, {});
    expect(result.status).toBe('COMPLETED');
    expect(result.stepResults[0]!.status).toBe('SUCCESS');
  });

  test('rejects invalid definitions', async () => {
    await expect(orchestrator().executeSaga({ id: 's', name: 's', steps: [] }, {})).rejects.toThrow('at least one step');
    await expect(orchestrator().executeSaga({ id: 's', name: 's', steps: [step('a'), step('a')] }, {})).rejects.toThrow(
      'duplicate step id'
    );
  });

  describe('recovery', () => {
    /** Put an execution in the store as if a worker crashed mid-way, long ago. */
    async function seedCrashed(store: InMemorySagaStore, partial: Partial<SagaExecution>): Promise<string> {
      const execution: SagaExecution = {
        id: 'exec-1',
        sagaId: 'checkout',
        status: 'RUNNING',
        context: { orderId: 'o1' },
        stepResults: [],
        owner: 'dead-worker',
        startedAt: new Date(0),
        updatedAt: new Date(0),
        ...partial
      };
      await store.saveExecution(execution);
      return execution.id;
    }

    function recorded(ids: string[]) {
      const calls: string[] = [];
      const steps = ids.map(id =>
        step(id, {
          action: async () => {
            calls.push(id);
            return `out-${id}`;
          },
          compensation: async (_c, out) => void calls.push(`undo-${id}:${out}`)
        })
      );
      return { calls, steps };
    }

    const done = (id: string) => ({
      stepId: id,
      stepName: id,
      status: 'SUCCESS' as const,
      output: `out-${id}`,
      startedAt: new Date(0),
      attempts: 1
    });

    test('resumes an interrupted execution from the step it was on', async () => {
      const store = new InMemorySagaStore();
      await seedCrashed(store, { currentStep: 1, stepResults: [done('a')] });
      const { calls, steps } = recorded(['a', 'b', 'c']);

      const worker = orchestrator(store, { workerId: 'worker-2', staleAfterMs: 1000 });
      worker.register({ id: 'checkout', name: 'Checkout', steps });
      const recovered = await worker.recover();

      expect(recovered).toHaveLength(1);
      expect(recovered[0]!.status).toBe('COMPLETED');
      expect(calls).toEqual(['b', 'c']);
      expect((await store.getExecution('exec-1'))?.owner).toBe('worker-2');
    });

    test('compensates instead when the saga recovery strategy is "compensate"', async () => {
      const store = new InMemorySagaStore();
      await seedCrashed(store, { stepResults: [done('a'), done('b')] });
      const { calls, steps } = recorded(['a', 'b', 'c']);

      const worker = orchestrator(store, { staleAfterMs: 1000 });
      worker.register({ id: 'checkout', name: 'Checkout', recovery: 'compensate', steps });
      const [result] = await worker.recover();

      expect(result!.status).toBe('COMPENSATED');
      expect(result!.error?.name).toBe('SagaInterruptedError');
      expect(calls).toEqual(['undo-b:out-b', 'undo-a:out-a']);
    });

    test('finishes an interrupted compensation without repeating finished undos', async () => {
      const store = new InMemorySagaStore();
      await seedCrashed(store, {
        status: 'COMPENSATING',
        stepResults: [done('a'), { ...done('b'), status: 'COMPENSATED' }, { ...done('c'), status: 'FAILED' }]
      });
      const { calls, steps } = recorded(['a', 'b', 'c']);

      const worker = orchestrator(store, { staleAfterMs: 1000 });
      worker.register({ id: 'checkout', name: 'Checkout', steps });
      const [result] = await worker.recover();

      expect(result!.status).toBe('COMPENSATED');
      expect(calls).toEqual(['undo-a:out-a']);
    });

    test('compensates when the definition changed since the execution started', async () => {
      const store = new InMemorySagaStore();
      await seedCrashed(store, { stepResults: [done('a')] });
      const { calls, steps } = recorded(['x', 'a']);

      const worker = orchestrator(store, { staleAfterMs: 1000 });
      worker.register({ id: 'checkout', name: 'Checkout', steps });
      const [result] = await worker.recover();

      expect(result!.status).toBe('COMPENSATED');
      expect(calls).toEqual(['undo-a:out-a']);
    });

    test('leaves fresh executions and unregistered sagas alone', async () => {
      const store = new InMemorySagaStore();
      await seedCrashed(store, { id: 'fresh', updatedAt: new Date() });
      await seedCrashed(store, { id: 'other', sagaId: 'unknown' });

      const worker = orchestrator(store, { staleAfterMs: 60000 });
      worker.register({ id: 'checkout', name: 'Checkout', steps: [step('a')] });

      expect(await worker.recover()).toEqual([]);
      expect((await store.getExecution('fresh'))?.status).toBe('RUNNING');
    });

    test('only one worker claims a stale execution', async () => {
      const store = new InMemorySagaStore();
      await seedCrashed(store, {});
      const def = { id: 'checkout', name: 'Checkout', steps: [step('a')] };
      const workers = [1, 2, 3].map(i => orchestrator(store, { workerId: `w${i}`, staleAfterMs: 1000 }).register(def));

      const results = await Promise.all(workers.map(w => w.recover()));
      expect(results.flat()).toHaveLength(1);
    });

    test('a worker that loses its lease stops, and the new owner finishes the saga', async () => {
      const store = new InMemorySagaStore();
      const aborts: unknown[] = [];
      let calls = 0;
      const def: SagaDefinition = {
        id: 'checkout',
        name: 'Checkout',
        steps: [
          step('slow', {
            timeout: 5000,
            action: (_ctx, meta) => {
              calls++;
              if (calls > 1) return Promise.resolve('done by B');
              // Worker A hangs (e.g. GC pause, network partition) until its lease is revoked.
              return new Promise((_, reject) =>
                meta.signal.addEventListener('abort', () => {
                  aborts.push(meta.signal.reason);
                  reject(meta.signal.reason);
                })
              );
            }
          })
        ]
      };

      const a = orchestrator(store, { workerId: 'A', staleAfterMs: 30, heartbeatIntervalMs: 80 });
      const b = orchestrator(store, { workerId: 'B', staleAfterMs: 30 }).register(def);

      const running = a.executeSaga(def, {});
      const aOutcome = running.catch(e => e);
      await new Promise(r => setTimeout(r, 50)); // A has gone quiet past staleAfterMs
      const [recovered] = await b.recover();

      expect(recovered!.status).toBe('COMPLETED');
      expect(await aOutcome).toBeInstanceOf(LeaseLostError);
      expect(aborts[0]).toBeInstanceOf(LeaseLostError);
      const stored = await store.getExecution(recovered!.id);
      expect(stored?.owner).toBe('B');
      expect(stored?.status).toBe('COMPLETED');
    });

    test('startRecovery polls until stopped', async () => {
      const store = new InMemorySagaStore();
      await seedCrashed(store, {});
      const worker = orchestrator(store, { staleAfterMs: 1000 }).register({
        id: 'checkout',
        name: 'Checkout',
        steps: [step('a')]
      });

      worker.startRecovery(5);
      for (let i = 0; i < 50 && (await store.getExecution('exec-1'))?.status !== 'COMPLETED'; i++) {
        await new Promise(r => setTimeout(r, 10));
      }
      await worker.stopRecovery();
      expect((await store.getExecution('exec-1'))?.status).toBe('COMPLETED');
    });
  });
});
