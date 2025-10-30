import { ChildProcessWithoutNullStreams, spawn, spawnSync, fork } from 'node:child_process';
import type { ChildProcess } from 'node:child_process';
import { EventEmitter, once } from 'node:events';
import { randomInt } from 'node:crypto';
import { setTimeout as sleep } from 'node:timers/promises';
import path from 'node:path';

import IORedis from 'ioredis';
import { describe, expect, it, beforeAll, afterAll } from 'vitest';

import { UniqueMutexManager } from '../src/UniqueMutexManager';
import type { MutexRunContext } from '../src/UniqueMutexManager';
import {
  MutexAbortedError,
  MutexDeadlockError,
  MutexLockedError,
  MutexTimeoutError,
} from '../src/errors';

const projectRoot = path.resolve(__dirname, '..');

const redisAvailable = (() => {
  try {
    const result = spawnSync('redis-server', ['--version'], { stdio: 'ignore' });
    return result.status === 0;
  } catch {
    return false;
  }
})();

async function waitForRedisConnection(url: string, timeoutMs = 5000): Promise<void> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    const client = new IORedis(url, { lazyConnect: true });
    client.on('error', () => {});
    try {
      await client.connect();
      await client.ping();
      await client.quit();
      return;
    } catch (error) {
      await client.disconnect();
      await sleep(50);
    }
  }
  throw new Error(`Redis at ${url} did not become ready within ${timeoutMs}ms`);
}

async function terminateProcess(
  proc: ChildProcessWithoutNullStreams | ChildProcess | undefined
): Promise<void> {
  if (!proc) {
    return;
  }

  if (proc.exitCode !== null) {
    return;
  }

  proc.kill('SIGTERM');
  try {
    await once(proc, 'exit');
  } catch {
    proc.kill('SIGKILL');
  }
}

function createMessageQueue(child: ChildProcess) {
  const messages: unknown[] = [];
  const waiting: { resolve: (value: unknown) => void; reject: (error: Error) => void }[] = [];

  child.on('message', (message) => {
    if (waiting.length > 0) {
      const waiter = waiting.shift()!;
      waiter.resolve(message);
    } else {
      messages.push(message);
    }
  });

  child.on('exit', (code, signal) => {
    const error = new Error(`Child exited unexpectedly (code: ${code}, signal: ${signal})`);
    while (waiting.length > 0) {
      const waiter = waiting.shift()!;
      waiter.reject(error);
    }
  });

  return {
    async waitFor<T = unknown>(
      predicate: (message: unknown) => message is T,
      timeoutMs = 5000
    ): Promise<T> {
      const start = Date.now();

      while (true) {
        const matchIndex = messages.findIndex((message) => predicate(message));
        if (matchIndex !== -1) {
          const [matched] = messages.splice(matchIndex, 1);
          return matched as T;
        }

        const elapsed = Date.now() - start;
        if (elapsed >= timeoutMs) {
          throw new Error(`Timed out waiting for message after ${timeoutMs}ms`);
        }

        const remaining = timeoutMs - elapsed;

        const nextMessage = await new Promise<unknown>((resolve, reject) => {
          let settled = false;
          let timer: NodeJS.Timeout;

          const entry = {
            resolve: (value: unknown) => {
              if (settled) {
                return;
              }
              settled = true;
              clearTimeout(timer);
              const index = waiting.indexOf(entry);
              if (index !== -1) {
                waiting.splice(index, 1);
              }
              resolve(value);
            },
            reject: (error: Error) => {
              if (settled) {
                return;
              }
              settled = true;
              clearTimeout(timer);
              const index = waiting.indexOf(entry);
              if (index !== -1) {
                waiting.splice(index, 1);
              }
              reject(error);
            },
          };

          waiting.push(entry);
          timer = setTimeout(() => {
            entry.reject(new Error(`Timed out waiting for message after ${timeoutMs}ms`));
          }, remaining);
        });

        if (predicate(nextMessage)) {
          return nextMessage;
        }

        messages.push(nextMessage);
      }
    },
  };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null;
}

function isTypedMessage<T extends string>(
  message: unknown,
  type: T
): message is { type: T } & Record<string, unknown> {
  return isRecord(message) && message.type === type;
}

describe('UniqueMutexManager (single process)', () => {
  it('serializes operations per id with accurate timings', async () => {
    const manager = new UniqueMutexManager();
    const startOrder: number[] = [];
    const completionOrder: number[] = [];
    const startTimes: number[] = [];

    const tasks = Array.from({ length: 5 }, (_, idx) =>
      manager.runOperation('alpha', async ({ startTime, requestTime }) => {
        expect(startTime).toBeGreaterThanOrEqual(requestTime);
        if (startTimes.length > 0) {
          expect(startTime).toBeGreaterThanOrEqual(startTimes[startTimes.length - 1]);
        }
        startOrder.push(idx);
        startTimes.push(startTime);
        await sleep(10 + idx * 5);
        completionOrder.push(idx);
        return idx;
      })
    );

    const results = await Promise.all(tasks);

    expect(results).toEqual([0, 1, 2, 3, 4]);
    expect(startOrder).toEqual([0, 1, 2, 3, 4]);
    expect(completionOrder).toEqual([0, 1, 2, 3, 4]);
  });

  it('supports high concurrency without overlapping execution', async () => {
    const manager = new UniqueMutexManager();
    const operations = 40;
    const observedQueueSizes: number[] = [];
    let concurrent = 0;
    let peakConcurrent = 0;

    const tasks = Array.from({ length: operations }, (_, index) =>
      manager.runOperation('stress', async ({ currentMutex }) => {
        concurrent += 1;
        peakConcurrent = Math.max(peakConcurrent, concurrent);
        observedQueueSizes.push(currentMutex.queueSize);
        expect(concurrent).toBe(1);
        await sleep(randomInt(1, 15));
        concurrent -= 1;
        return index;
      })
    );

    const results = await Promise.all(tasks);
    expect(results).toEqual([...Array(operations).keys()]);
    expect(peakConcurrent).toBe(1);
    expect(Math.max(...observedQueueSizes)).toBeLessThanOrEqual(operations);
  });

  it('propagates errors and allows subsequent operations', async () => {
    const manager = new UniqueMutexManager();
    await expect(
      manager.runOperation('beta', async () => {
        await sleep(5);
        throw new Error('boom');
      })
    ).rejects.toThrowError('boom');

    const value = await manager.runOperation('beta', async () => 'recovered');
    expect(value).toBe('recovered');
  });

  it('can skip queueing when waitIfLocked is false', async () => {
    const manager = new UniqueMutexManager();
    const first = manager.runOperation('gamma', async () => {
      await sleep(50);
      return 'first';
    });

    await expect(
      manager.runOperation('gamma', async () => 'second', { waitIfLocked: false })
    ).rejects.toBeInstanceOf(MutexLockedError);

    await expect(first).resolves.toBe('first');
  });

  it('waits indefinitely for locks when no timeout is provided', async () => {
    const manager = new UniqueMutexManager();

    let blockerStarted!: () => void;
    const blockerReady = new Promise<void>((resolve) => {
      blockerStarted = resolve;
    });

    const blocker = manager.runOperation('omega', async () => {
      blockerStarted();
      await sleep(90);
      return 'blocker';
    });

    await blockerReady;

    const waitStart = Date.now();
    let executed = false;

    const follower = manager.runOperation('omega', async () => {
      executed = true;
      const waited = Date.now() - waitStart;
      expect(waited).toBeGreaterThanOrEqual(75);
      await sleep(5);
      return 'follower';
    });

    await expect(Promise.all([blocker, follower])).resolves.toEqual(['blocker', 'follower']);
    expect(executed).toBe(true);
  });

  it('respects timeout boundaries while waiting for a lock', async () => {
    const manager = new UniqueMutexManager();

    let blockerStarted!: () => void;
    const blockerReady = new Promise<void>((resolve) => {
      blockerStarted = resolve;
    });

    const blocker = manager.runOperation('theta', async () => {
      blockerStarted();
      await sleep(120);
      return 'slow';
    });

    await blockerReady;

    const attempted: { ran: boolean } = { ran: false };
    const start = Date.now();

    await expect(
      manager.runOperation(
        'theta',
        async () => {
          attempted.ran = true;
          return 'should-not-run';
        },
        { timeoutMs: 50 }
      )
    ).rejects.toBeInstanceOf(MutexTimeoutError);

    const elapsed = Date.now() - start;
    expect(attempted.ran).toBe(false);
    expect(elapsed).toBeGreaterThanOrEqual(45);
    expect(elapsed).toBeLessThan(200);

    await blocker;
  });

  it('aborts queued operations when an external signal fires', async () => {
    const manager = new UniqueMutexManager();

    let releaseBlocker!: () => void;
    let blockerStarted!: () => void;
    const blockerReady = new Promise<void>((resolve) => {
      blockerStarted = resolve;
    });

    const blocker = manager.runOperation('abort', async () => {
      blockerStarted();
      await new Promise<void>((resolve) => {
        releaseBlocker = resolve;
      });
      return 'blocker';
    });

    await blockerReady;

    const controller = new AbortController();
    let executed = false;

    const queued = manager.runOperation(
      'abort',
      async () => {
        executed = true;
        return 'should-not-run';
      },
      { signal: controller.signal }
    );

    await sleep(25);
    const reason = new Error('cancel-waiting');
    controller.abort(reason);

    const error = await queued.catch((err) => err as MutexAbortedError);
    expect(error).toBeInstanceOf(MutexAbortedError);
    expect(error.id).toBe('abort');
    expect(error.reason).toBe(reason);
    expect(executed).toBe(false);

    releaseBlocker();
    await blocker;
  });

  it('exposes an abort hook that cancels queued operations', async () => {
    const manager = new UniqueMutexManager();

    let releaseBlocker!: () => void;
    let blockerStarted!: () => void;
    const blockerReady = new Promise<void>((resolve) => {
      blockerStarted = resolve;
    });

    const blocker = manager.runOperation('abort-hook', async () => {
      blockerStarted();
      await new Promise<void>((resolve) => {
        releaseBlocker = resolve;
      });
      return 'blocker';
    });

    await blockerReady;

    let abortFn: ((reason?: unknown) => void) | undefined;
    let executed = false;

    const queued = manager.runOperation(
      'abort-hook',
      async () => {
        executed = true;
        return 'should-not-run';
      },
      {
        onAbort: (abort) => {
          abortFn = abort;
        },
      }
    );

    await sleep(30);
    expect(abortFn).toBeTypeOf('function');
    const reason = { tag: 'external-abort' };
    abortFn?.(reason);

    const error = await queued.catch((err) => err as MutexAbortedError);
    expect(error).toBeInstanceOf(MutexAbortedError);
    expect(error.id).toBe('abort-hook');
    expect(error.reason).toBe(reason);
    expect(executed).toBe(false);

    releaseBlocker();
    await blocker;
  });

  it('propagates abort signals to running operations for cooperative cancellation', async () => {
    const manager = new UniqueMutexManager();

    const controller = new AbortController();
    let abortEvents = 0;

    const runner = manager.runOperation('cooperative', async ({ abortSignal }) => {
      return new Promise<string>((resolve, reject) => {
        const onAbort = () => {
          abortEvents += 1;
          resolve('aborted');
        };
        if (abortSignal.aborted) {
          onAbort();
          return;
        }
        abortSignal.addEventListener('abort', onAbort, { once: true });
        setTimeout(() => resolve('completed'), 200);
      });
    }, { signal: controller.signal });

    await sleep(30);
    controller.abort('stop-now');

    const error = await runner.catch((err) => err as MutexAbortedError);
    expect(error).toBeInstanceOf(MutexAbortedError);
    expect(error.id).toBe('cooperative');
    expect(error.reason).toBe('stop-now');
    expect(abortEvents).toBeGreaterThanOrEqual(1);
  });

  it('handles bursts of aborts without leaking locks', async () => {
    const manager = new UniqueMutexManager();
    const total = 45;

    const tasks = Array.from({ length: total }, (_, index) => {
      const controller = new AbortController();
      const shouldAbort = index % 3 === 0;
      const promise = manager.runOperation(
        'burst-abort',
        async ({ abortSignal }) => {
          for (let step = 0; step < 5; step += 1) {
            if (abortSignal.aborted) {
              break;
            }
            await sleep(randomInt(1, 6));
          }
          return `task-${index}`;
        },
        { signal: controller.signal }
      );

      if (shouldAbort) {
        setTimeout(() => controller.abort(`abort-${index}`), randomInt(5, 30));
      }

      return promise;
    });

    const results = await Promise.allSettled(tasks);
    const rejections = results.filter(
      (entry): entry is PromiseRejectedResult => entry.status === 'rejected'
    );

    expect(rejections.length).toBeGreaterThan(0);
    for (const rejection of rejections) {
      expect(rejection.reason).toBeInstanceOf(MutexAbortedError);
    }

    const final = await manager.runOperation('burst-abort', async () => 'final-run');
    expect(final).toBe('final-run');
  });

  it('supports reentrant locking and exposes held mutex metadata', async () => {
    const manager = new UniqueMutexManager();
    const sequence: string[] = [];

    const result = await manager.runOperation('delta', async ({ heldMutexIds }) => {
      sequence.push(`outer:${heldMutexIds.join(',')}`);

      const inner = await manager.runOperation('delta', async ({ heldMutexIds: innerHeld }) => {
        sequence.push(`inner:${innerHeld.join(',')}`);
        expect(innerHeld).toEqual(['delta']);
        return 'inner';
      });

      expect(inner).toBe('inner');
      return 'outer';
    });

    expect(result).toBe('outer');
    expect(sequence).toEqual(['outer:delta', 'inner:delta']);
  });

  it('detects potential deadlocks and prevents soft locking', async () => {
    const manager = new UniqueMutexManager();

    let allowBetaToRequestAlpha!: () => void;
    const betaReady = new Promise<void>((resolve) => {
      allowBetaToRequestAlpha = resolve;
    });

    let nestedAlphaStarted!: () => void;
    const nestedAlphaReady = new Promise<void>((resolve) => {
      nestedAlphaStarted = resolve;
    });

    type Outcome =
      | { status: 'resolved'; value: string }
      | { status: 'rejected'; error: MutexDeadlockError };

    let nestedAlphaPromise: Promise<string> | undefined;
    let alphaWaitOutcome: Outcome | undefined;
    let betaWaitOutcome: Outcome | undefined;

    const betaPromise = manager.runOperation('beta', async () => {
      await betaReady;

      const nestedPromise = manager.runOperation('alpha', async () => 'beta-alpha');
      nestedAlphaPromise = nestedPromise;
      nestedAlphaStarted();

      try {
        const value = await nestedPromise;
        expect(value).toBe('beta-alpha');
        betaWaitOutcome = { status: 'resolved', value };
      } catch (error) {
        expect(error).toBeInstanceOf(MutexDeadlockError);
        if (error instanceof MutexDeadlockError) {
          betaWaitOutcome = { status: 'rejected', error };
        }
      }

      return 'beta';
    });

    const alphaPromise = manager.runOperation('alpha', async () => {
      allowBetaToRequestAlpha();

      await nestedAlphaReady;

      let sawDeadlock = false;
      try {
        const value = await manager.runOperation('beta', async () => 'should not run');
        alphaWaitOutcome = { status: 'resolved', value };
      } catch (error) {
        expect(error).toBeInstanceOf(MutexDeadlockError);
        if (error instanceof MutexDeadlockError) {
          expect(new Set(error.cycle)).toEqual(new Set(['alpha', 'beta']));
          alphaWaitOutcome = { status: 'rejected', error };
          sawDeadlock = true;
        } else {
          throw error;
        }
      }

      expect(sawDeadlock).toBe(true);
      return 'alpha';
    });

    await expect(Promise.all([alphaPromise, betaPromise])).resolves.toEqual(['alpha', 'beta']);

    expect(alphaWaitOutcome).toBeDefined();
    expect(betaWaitOutcome).toBeDefined();

    const outcomes = [alphaWaitOutcome!, betaWaitOutcome!];
    const failures = outcomes.filter((outcome) => outcome.status === 'rejected');
    expect(failures).toHaveLength(1);
    const failure = failures[0] as { status: 'rejected'; error: MutexDeadlockError };
    expect(new Set(failure.error.cycle)).toEqual(new Set(['alpha', 'beta']));
    expect(['alpha', 'beta']).toContain(failure.error.requestedId);
  });

  it('detects deadlocks without relying on timeout durations', async () => {
    const manager = new UniqueMutexManager();
    const contextA = manager.createContext();
    const contextB = manager.createContext();

    let alphaReady!: () => void;
    const alphaReadyPromise = new Promise<void>((resolve) => {
      alphaReady = resolve;
    });

    let betaWaiting!: () => void;
    const betaWaitingPromise = new Promise<void>((resolve) => {
      betaWaiting = resolve;
    });

    let deadlockStart = 0;

    const alphaResult = manager
      .runOperation(
        'alpha',
        async ({ contextToken }) => {
          alphaReady();
          await betaWaitingPromise;
          await manager.runOperation(
            'beta',
            async () => {
              await sleep(5);
            },
            { context: contextToken }
          );
          return 'alpha';
        },
        { context: contextA }
      )
      .then(() => undefined)
      .catch((error) => error);

    await alphaReadyPromise;

    const betaResult = manager
      .runOperation(
        'beta',
        async ({ contextToken }) => {
          deadlockStart = Date.now();
          betaWaiting();
          await manager.runOperation(
            'alpha',
            async () => {
              await sleep(5);
            },
            { context: contextToken }
          );
          return 'beta';
        },
        { context: contextB }
      )
      .then(() => undefined)
      .catch((error) => error);

    const outcomes = await Promise.all([alphaResult, betaResult]);
    const errors = outcomes.filter((value): value is MutexDeadlockError => value instanceof MutexDeadlockError);

    expect(errors.length).toBeGreaterThanOrEqual(1);
    for (const error of errors) {
      expect(new Set(error.cycle)).toEqual(new Set(['alpha', 'beta']));
    }

    expect(deadlockStart).toBeGreaterThan(0);
    expect(Date.now() - deadlockStart).toBeLessThan(100);
  });

  it('detects deadlocks triggered via event-driven cross dependencies', async () => {
    const manager = new UniqueMutexManager();
    const bus = new EventEmitter();

    const alphaStartedPromise = once(bus, 'alpha:start');
    const betaReadyPromise = once(bus, 'beta:ready');
    const gammaReadyPromise = once(bus, 'gamma:ready');
    const betaWaitingGammaPromise = once(bus, 'beta:waiting-gamma');

    type AttemptOutcome<T> =
      | { status: 'resolved'; value: T }
      | { status: 'rejected'; error: MutexDeadlockError };

    let alphaOutcome: AttemptOutcome<string> | undefined;
    let betaOutcome: AttemptOutcome<string> | undefined;
    let gammaOutcome: AttemptOutcome<string> | undefined;

    const alphaPromise = manager.runOperation('alpha', async () => {
      bus.emit('alpha:start');
      await betaReadyPromise;

      try {
        const result = await manager.runOperation('beta', async () => 'alpha->beta');
        alphaOutcome = { status: 'resolved', value: result };
      } catch (error) {
        expect(error).toBeInstanceOf(MutexDeadlockError);
        if (error instanceof MutexDeadlockError) {
          alphaOutcome = { status: 'rejected', error };
        } else {
          throw error;
        }
      }

      return 'alpha';
    });

    const betaPromise = manager.runOperation('beta', async () => {
      await alphaStartedPromise;
      bus.emit('beta:ready');
      await gammaReadyPromise;
      bus.emit('beta:waiting-gamma');

      try {
        const result = await manager.runOperation('gamma', async () => 'beta->gamma');
        betaOutcome = { status: 'resolved', value: result };
      } catch (error) {
        expect(error).toBeInstanceOf(MutexDeadlockError);
        if (error instanceof MutexDeadlockError) {
          betaOutcome = { status: 'rejected', error };
        } else {
          throw error;
        }
      }

      return 'beta';
    });

    const gammaPromise = manager.runOperation('gamma', async () => {
      await betaReadyPromise;
      bus.emit('gamma:ready');
      await betaWaitingGammaPromise;

      try {
        const result = await manager.runOperation('alpha', async () => 'gamma->alpha');
        gammaOutcome = { status: 'resolved', value: result };
      } catch (error) {
        expect(error).toBeInstanceOf(MutexDeadlockError);
        if (error instanceof MutexDeadlockError) {
          gammaOutcome = { status: 'rejected', error };
          expect(error.requestedId).toBe('alpha');
          expect(error.cycle).toEqual(['alpha', 'beta', 'gamma', 'alpha']);
        } else {
          throw error;
        }
      }

      return 'gamma';
    });

    await expect(Promise.all([alphaPromise, betaPromise, gammaPromise])).resolves.toEqual([
      'alpha',
      'beta',
      'gamma',
    ]);

    expect(alphaOutcome).toBeDefined();
    expect(betaOutcome).toBeDefined();
    expect(gammaOutcome).toBeDefined();

    expect(alphaOutcome?.status).toBe('resolved');
    expect(betaOutcome?.status).toBe('resolved');
    expect(gammaOutcome?.status).toBe('rejected');

    const rejected = gammaOutcome as { status: 'rejected'; error: MutexDeadlockError };
    expect(new Set(rejected.error.cycle)).toEqual(new Set(['alpha', 'beta', 'gamma']));
  });

  it('allows manual context propagation when async context is unavailable', async () => {
    const manager = new UniqueMutexManager();

    (manager as unknown as { supportsAsyncContext: boolean }).supportsAsyncContext = false;
    (manager as unknown as {
      context: { run: <R>(store: unknown, callback: () => R) => R; getStore: () => undefined };
    }).context = {
      run: (_store, callback) => callback(),
      getStore: () => undefined,
    };

    expect(manager.isAsyncContextTrackingSupported()).toBe(false);

    const emitter = new EventEmitter();
    const steps: string[] = [];

    let resolveBeta!: () => void;
    const betaFinished = new Promise<void>((resolve) => {
      resolveBeta = resolve;
    });

    emitter.on('request-beta', async (token: MutexRunContext) => {
      try {
        await manager.runOperation(
          'beta',
          async ({ heldMutexIds, contextToken }) => {
            steps.push('beta-start');
            expect(new Set(heldMutexIds)).toEqual(new Set(['alpha', 'beta']));
            expect(contextToken).toBe(token);

            await manager.runOperation(
              'alpha',
              async ({ heldMutexIds: reentrantHeld, contextToken: nestedToken }) => {
                steps.push('alpha-reenter');
                expect(new Set(reentrantHeld)).toEqual(new Set(['alpha', 'beta']));
                expect(nestedToken).toBe(token);
              },
              { context: contextToken }
            );

            steps.push('beta-end');
          },
          { context: token }
        );
      } finally {
        resolveBeta();
      }
    });

    await manager.runOperation('alpha', async ({ contextToken, heldMutexIds }) => {
      steps.push('alpha-start');
      expect(heldMutexIds).toEqual(['alpha']);
      setTimeout(() => emitter.emit('request-beta', contextToken), 0);
      await betaFinished;
      steps.push('alpha-end');
    });

    expect(steps).toEqual(['alpha-start', 'beta-start', 'alpha-reenter', 'beta-end', 'alpha-end']);
  });

  it('survives cascading workflows with manual contexts, reentrancy, and timeouts', async () => {
    const manager = new UniqueMutexManager();
    const emitter = new EventEmitter();
    const executing = new Set<Promise<void>>();
    const events: string[] = [];
    const inFlightById = new Map<string, number>();

    type WorkflowSpec = {
      id: string;
      label: string;
      context: MutexRunContext;
      work: number;
      reenter?: boolean;
      timeout?: number;
      delay?: number;
      chain?: WorkflowSpec[];
    };

    const schedule = (spec: WorkflowSpec) => {
      const delay = spec.delay ?? 0;
      const task = (async () => {
        if (delay > 0) {
          await sleep(delay);
        }

        try {
          await manager.runOperation(
            spec.id,
            async ({ contextToken }) => {
              const prior = inFlightById.get(spec.id) ?? 0;
              const next = prior + 1;
              inFlightById.set(spec.id, next);
              if (!spec.reenter) {
                expect(prior).toBe(0);
              }

              events.push(`start:${spec.label}`);
              try {
                if (spec.reenter) {
                  await manager.runOperation(
                    spec.id,
                    async () => {
                      events.push(`reenter:${spec.label}`);
                    },
                    { context: contextToken, timeoutMs: spec.timeout ?? 300 }
                  );
                }

                if (spec.chain) {
                  for (const next of spec.chain) {
                    emitter.emit('schedule', next);
                  }
                }

                await sleep(spec.work);
                events.push(`end:${spec.label}`);
              } finally {
                inFlightById.set(spec.id, next - 1);
              }
            },
            { context: spec.context, timeoutMs: spec.timeout ?? 300 }
          );
        } catch (error) {
          events.push(`error:${spec.label}:${(error as Error).name}`);
        }
      })();

      executing.add(task);
      task.finally(() => executing.delete(task));
    };

    emitter.on('schedule', (spec: WorkflowSpec) => {
      schedule(spec);
    });

    const workflowA = manager.createContext();
    const workflowB = manager.createContext();
    const workflowC = manager.createContext();

    emitter.emit('schedule', {
      id: 'alpha',
      label: 'workflow-a',
      context: workflowA,
      work: 30,
      reenter: true,
      chain: [
        {
          id: 'beta',
          label: 'workflow-b',
          context: workflowB,
          work: 25,
          chain: [
            {
              id: 'gamma',
              label: 'workflow-c',
              context: workflowC,
              work: 20,
              reenter: true,
              chain: [
                {
                  id: 'alpha',
                  label: 'follow-up-alpha',
                  context: workflowC,
                  work: 12,
                  timeout: 400,
                },
                {
                  id: 'beta',
                  label: 'follow-up-beta',
                  context: workflowA,
                  work: 10,
                  timeout: 350,
                  delay: 5,
                },
              ],
            },
          ],
        },
      ],
    });

    emitter.emit('schedule', {
      id: 'delta',
      label: 'isolated-delta',
      context: workflowB,
      work: 18,
      chain: [
        {
          id: 'alpha',
          label: 'delta-to-alpha',
          context: workflowB,
          work: 14,
          timeout: 400,
          reenter: true,
        },
      ],
    });

    const sharedContext = manager.createContext();
    emitter.emit('schedule', {
      id: 'gamma',
      label: 'shared-gamma',
      context: sharedContext,
      work: 16,
      reenter: true,
      timeout: 360,
      chain: [
        {
          id: 'gamma',
          label: 'shared-gamma-followup',
          context: sharedContext,
          work: 8,
          reenter: true,
          timeout: 320,
        },
      ],
    });

    while (executing.size > 0) {
      await Promise.all(Array.from(executing));
    }

    const errors = events.filter((entry) => entry.startsWith('error:'));
    expect(errors).toEqual([]);
    expect(events).toContain('reenter:workflow-a');
    expect(events).toContain('reenter:workflow-c');
    expect(events).toContain('reenter:delta-to-alpha');
    expect(events).toContain('reenter:shared-gamma');

    const labels = new Set([
      'workflow-a',
      'workflow-b',
      'workflow-c',
      'follow-up-alpha',
      'follow-up-beta',
      'isolated-delta',
      'delta-to-alpha',
      'shared-gamma',
      'shared-gamma-followup',
    ]);

    for (const label of labels) {
      expect(events).toContain(`start:${label}`);
      expect(events).toContain(`end:${label}`);
    }
  });

  it('handles bursty contention across many mutex ids', async () => {
    const manager = new UniqueMutexManager();
    const keys = ['alpha', 'beta', 'gamma', 'delta', 'epsilon'];
    const operationsPerKey = 40;
    const executed = new Map<string, number[]>();

    for (const key of keys) {
      executed.set(key, []);
    }

    const tasks: Promise<void>[] = [];

    for (const key of keys) {
      for (let i = 0; i < operationsPerKey; i += 1) {
        const ordinal = i;
        tasks.push(
          manager.runOperation(key, async () => {
            executed.get(key)!.push(ordinal);
            await sleep(randomInt(1, 20));
          })
        );
      }
    }

    await Promise.all(tasks);

    for (const key of keys) {
      const order = executed.get(key)!;
      expect(order).toEqual([...Array(operationsPerKey).keys()]);
    }
  });
});

describe('package loading', () => {
  beforeAll(() => {
    const buildResult = spawnSync('npm', ['run', 'build'], {
      cwd: projectRoot,
      stdio: 'pipe',
      env: { ...process.env, NODE_ENV: 'test' },
    });

    if (buildResult.status !== 0) {
      const stderr = buildResult.stderr?.toString() ?? 'unknown error';
      throw new Error(`Failed to build package before load checks: ${stderr}`);
    }
  });

  it('commonjs build loads without optional dependencies present', () => {
    const script = `
      const Module = require('module');
      const originalLoad = Module._load;
      Module._load = function patched(request, parent, isMain) {
        if (request === 'ioredis' || request === 'redlock') {
          const err = new Error('Cannot find module ' + request);
          err.code = 'MODULE_NOT_FOUND';
          throw err;
        }
        return originalLoad.apply(this, arguments);
      };

      try {
        require('./dist/cjs/index.js');
      } finally {
        Module._load = originalLoad;
      }
    `;

    const result = spawnSync(process.execPath, ['-e', script], {
      cwd: projectRoot,
      stdio: 'pipe',
      env: { ...process.env, NODE_ENV: 'test' },
    });

    const stdout = result.stdout?.toString() ?? '';
    const stderr = result.stderr?.toString() ?? '';

    if (result.error) {
      throw result.error;
    }

    if (result.status !== 0) {
      throw new Error(`Failed to require commonjs build.\nSTDOUT: ${stdout}\nSTDERR: ${stderr}`);
    }

    expect(stdout).toBe('');
    expect(stderr).toBe('');
  });
});

const describeDistributed = redisAvailable ? describe : describe.skip;

describeDistributed('UniqueMutexManager (distributed via redis)', () => {
  const redisPort = 6380;
  const redisUrl = `redis://127.0.0.1:${redisPort}`;
  let redisProcess: ChildProcessWithoutNullStreams | undefined;

  beforeAll(async () => {
    redisProcess = spawn('redis-server', ['--port', String(redisPort)]);

    redisProcess.stderr.setEncoding('utf8');
    redisProcess.stdout.setEncoding('utf8');

    await waitForRedisConnection(redisUrl, 10_000);
  }, 20_000);

  afterAll(async () => {
    await terminateProcess(redisProcess);
  });

  it('serializes operations across independent managers', async () => {
    const managerA = new UniqueMutexManager({ redis: { urls: [redisUrl] } });
    const managerB = new UniqueMutexManager({ redis: { urls: [redisUrl] } });

    const events: string[] = [];
    let concurrent = 0;

    const first = managerA.runOperation('shared', async () => {
      events.push('start-a');
      concurrent += 1;
      expect(concurrent).toBe(1);
      await sleep(60);
      concurrent -= 1;
      events.push('end-a');
      return 'A';
    });

    const second = managerB.runOperation('shared', async () => {
      events.push('start-b');
      concurrent += 1;
      expect(concurrent).toBe(1);
      await sleep(10);
      concurrent -= 1;
      events.push('end-b');
      return 'B';
    });

    await expect(Promise.all([first, second])).resolves.toEqual(['A', 'B']);
    expect(events).toHaveLength(4);
    expect(events.indexOf('end-a')).toBe(events.indexOf('start-a') + 1);
    expect(events.indexOf('end-b')).toBe(events.indexOf('start-b') + 1);

    await managerA.dispose();
    await managerB.dispose();
  });

  it('respects waitIfLocked=false across instances', async () => {
    const managerA = new UniqueMutexManager({ redis: { urls: [redisUrl] } });
    const managerB = new UniqueMutexManager({ redis: { urls: [redisUrl] } });

    let started!: () => void;
    const startedPromise = new Promise<void>((resolve) => {
      started = resolve;
    });

    const running = managerA.runOperation('rapid', async () => {
      started();
      await sleep(80);
      return 'done';
    });

    await startedPromise;

    await expect(
      managerB.runOperation('rapid', async () => 'should not run', { waitIfLocked: false })
    ).rejects.toBeInstanceOf(MutexLockedError);

    await running;
    await managerA.dispose();
    await managerB.dispose();
  });

  it('applies timeouts while waiting on distributed locks', async () => {
    const managerA = new UniqueMutexManager({ redis: { urls: [redisUrl] } });
    const managerB = new UniqueMutexManager({ redis: { urls: [redisUrl] } });

    let blockerStarted!: () => void;
    const blockerReady = new Promise<void>((resolve) => {
      blockerStarted = resolve;
    });

    const blocker = managerA.runOperation('timed', async () => {
      blockerStarted();
      await sleep(150);
      return 'primary';
    });

    await blockerReady;

    const attempted: { ran: boolean } = { ran: false };
    const start = Date.now();

    await expect(
      managerB.runOperation(
        'timed',
        async () => {
          attempted.ran = true;
          return 'should-not-run';
        },
        { timeoutMs: 60 }
      )
    ).rejects.toBeInstanceOf(MutexTimeoutError);

    const elapsed = Date.now() - start;
    expect(attempted.ran).toBe(false);
    expect(elapsed).toBeGreaterThanOrEqual(55);
    expect(elapsed).toBeLessThan(250);

    await blocker;
    await managerA.dispose();
    await managerB.dispose();
  });

  it('honors abort signals while waiting on distributed locks', async () => {
    const managerA = new UniqueMutexManager({ redis: { urls: [redisUrl] } });
    const managerB = new UniqueMutexManager({ redis: { urls: [redisUrl] } });

    let releaseBlocker!: () => void;
    let blockerStarted!: () => void;
    const blockerReady = new Promise<void>((resolve) => {
      blockerStarted = resolve;
    });

    const blocker = managerA.runOperation('distributed-abort', async () => {
      blockerStarted();
      await new Promise<void>((resolve) => {
        releaseBlocker = resolve;
      });
      return 'blocking';
    });

    await blockerReady;

    const controller = new AbortController();
    const queued = managerB.runOperation(
      'distributed-abort',
      async () => 'should-not-run',
      { signal: controller.signal }
    );

    await sleep(35);
    controller.abort('distributed-abort-signal');

    const error = await queued.catch((err) => err as MutexAbortedError);
    expect(error).toBeInstanceOf(MutexAbortedError);
    expect(error.id).toBe('distributed-abort');
    expect(error.reason).toBe('distributed-abort-signal');

    releaseBlocker();
    await blocker;

    await managerA.dispose();
    await managerB.dispose();
  });

  it('handles heavy contention across managers without overlap', async () => {
    const managers = Array.from({ length: 3 }, () => new UniqueMutexManager({ redis: { urls: [redisUrl] } }));
    const tasks: Promise<number>[] = [];
    let concurrent = 0;
    let maxConcurrent = 0;

    for (let i = 0; i < 30; i += 1) {
      const manager = managers[i % managers.length];
      tasks.push(
        manager.runOperation('distributed', async () => {
          concurrent += 1;
          maxConcurrent = Math.max(maxConcurrent, concurrent);
          if (concurrent > 1) {
            throw new Error(`Detected overlapping distributed execution (concurrent=${concurrent})`);
          }
          await sleep(randomInt(5, 25));
          concurrent -= 1;
          return i;
        })
      );
    }

    const results = await Promise.all(tasks);
    expect(results.sort((a, b) => a - b)).toEqual([...Array(30).keys()]);
    expect(maxConcurrent).toBe(1);

    await Promise.all(managers.map((manager) => manager.dispose()));
  });

  it('processes randomized bursts across managers and ids deterministically', async () => {
    const managers = Array.from({ length: 4 }, () => new UniqueMutexManager({ redis: { urls: [redisUrl] } }));
    const keys = ['alpha', 'beta', 'gamma'];
    const scheduled = new Map<string, number>();
    const executed = new Map<string, number[]>();
    const inFlight = new Map<string, number>();

    for (const key of keys) {
      scheduled.set(key, 0);
      executed.set(key, []);
      inFlight.set(key, 0);
    }

    const tasks: Promise<void>[] = [];
    const totalOperations = 90;

    for (let i = 0; i < totalOperations; i += 1) {
      const key = keys[i % keys.length];
      const ordinal = scheduled.get(key)!;
      scheduled.set(key, ordinal + 1);
      const manager = managers[i % managers.length];

      tasks.push(
        manager.runOperation(key, async () => {
          const current = inFlight.get(key)! + 1;
          inFlight.set(key, current);
          if (current !== 1) {
            throw new Error(`Concurrent execution detected for ${key}`);
          }

          await sleep(randomInt(5, 35));
          executed.get(key)!.push(ordinal);
          inFlight.set(key, inFlight.get(key)! - 1);
        })
      );
    }

    await Promise.all(tasks);

    for (const key of keys) {
      const order = executed.get(key)!;
      expect(order).toEqual([...Array(order.length).keys()]);
    }

    await Promise.all(managers.map((manager) => manager.dispose()));
  });

  it('detects deadlocks spanning multiple managers across processes', async () => {
    const registerPath = path.join(__dirname, 'helpers', 'register-ts.js');
    const workerScript = path.join(__dirname, 'helpers', 'distributed-deadlock-worker.ts');

    type ReadyMessage = { type: 'ready'; workerId: string };
    type OuterStartedMessage = { type: 'outer-started'; workerId: string; primaryId: string };
    type NestedResultMessage =
      | { type: 'nested-result'; workerId: string; status: 'resolved'; value: string }
      | {
          type: 'nested-result';
          workerId: string;
          status: 'deadlock';
          message: string;
          cycle: string[];
          requestedId: string;
        }
      | { type: 'nested-result'; workerId: string; status: 'error'; message: string; name?: string };
    type OuterCompleteMessage = { type: 'outer-complete'; workerId: string; value: string };
    type ShutdownCompleteMessage = { type: 'shutdown-complete'; workerId: string };

    const isReadyMessage = (message: unknown): message is ReadyMessage =>
      isTypedMessage(message, 'ready') && typeof message.workerId === 'string';

    const isOuterStartedMessage = (message: unknown): message is OuterStartedMessage =>
      isTypedMessage(message, 'outer-started') &&
      typeof message.primaryId === 'string' &&
      typeof message.workerId === 'string';

    const isNestedResultMessage = (message: unknown): message is NestedResultMessage => {
      if (!isTypedMessage(message, 'nested-result') || typeof message.workerId !== 'string') {
        return false;
      }

      if (message.status === 'resolved') {
        return typeof message.value === 'string';
      }

      if (message.status === 'deadlock') {
        return (
          Array.isArray(message.cycle) &&
          message.cycle.every((entry) => typeof entry === 'string') &&
          typeof message.requestedId === 'string' &&
          typeof message.message === 'string'
        );
      }

      if (message.status === 'error') {
        return typeof message.message === 'string';
      }

      return false;
    };

    const isOuterCompleteMessage = (message: unknown): message is OuterCompleteMessage =>
      isTypedMessage(message, 'outer-complete') &&
      typeof message.workerId === 'string' &&
      typeof message.value === 'string';

    const isShutdownCompleteMessage = (message: unknown): message is ShutdownCompleteMessage =>
      isTypedMessage(message, 'shutdown-complete') && typeof message.workerId === 'string';

    const workerA = fork(workerScript, {
      env: {
        ...process.env,
        REDIS_URL: redisUrl,
        PRIMARY_ID: 'alpha',
        NESTED_ID: 'beta',
        WORKER_LABEL: 'A',
      },
      stdio: ['inherit', 'inherit', 'inherit', 'ipc'],
      execArgv: [...process.execArgv, '-r', registerPath],
    });

    const workerB = fork(workerScript, {
      env: {
        ...process.env,
        REDIS_URL: redisUrl,
        PRIMARY_ID: 'beta',
        NESTED_ID: 'alpha',
        WORKER_LABEL: 'B',
      },
      stdio: ['inherit', 'inherit', 'inherit', 'ipc'],
      execArgv: [...process.execArgv, '-r', registerPath],
    });

    const queueA = createMessageQueue(workerA);
    const queueB = createMessageQueue(workerB);

    try {
      await Promise.all([queueA.waitFor(isReadyMessage), queueB.waitFor(isReadyMessage)]);

      workerA.send({ type: 'start-cycle' });
      workerB.send({ type: 'start-cycle' });

      await Promise.all([queueA.waitFor(isOuterStartedMessage), queueB.waitFor(isOuterStartedMessage)]);

      workerA.send({ type: 'proceed-nested' });
      workerB.send({ type: 'proceed-nested' });

      const nestedResults = await Promise.all([
        queueA.waitFor(isNestedResultMessage),
        queueB.waitFor(isNestedResultMessage),
      ]);

      const deadlock = nestedResults.find((result) => result.status === 'deadlock');
      expect(deadlock).toBeDefined();
      if (!deadlock) {
        throw new Error('Expected a deadlock result from one of the workers');
      }
      expect(new Set(deadlock.cycle)).toEqual(new Set(['alpha', 'beta']));
      expect(['alpha', 'beta']).toContain(deadlock.requestedId);

      const successful = nestedResults.find((result) => result.status === 'resolved');
      if (successful) {
        expect(successful.value === 'alpha->beta' || successful.value === 'beta->alpha').toBe(true);
      }

      const outerCompletions = await Promise.all([
        queueA.waitFor(isOuterCompleteMessage),
        queueB.waitFor(isOuterCompleteMessage),
      ]);

      expect(new Set(outerCompletions.map((item) => item.value))).toEqual(
        new Set(['alpha-outer', 'beta-outer'])
      );

      workerA.send({ type: 'shutdown' });
      workerB.send({ type: 'shutdown' });

      await Promise.all([queueA.waitFor(isShutdownCompleteMessage), queueB.waitFor(isShutdownCompleteMessage)]);

      await Promise.all([once(workerA, 'exit'), once(workerB, 'exit')]);
    } finally {
      await Promise.all([terminateProcess(workerA), terminateProcess(workerB)]);
    }
  });
});
