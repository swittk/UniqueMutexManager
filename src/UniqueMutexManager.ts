import type IORedisNamespace from 'ioredis';
import type { Cluster as IORedisCluster, Redis as IORedisClient } from 'ioredis';
import type Redlock from 'redlock';
import type { ExecutionError, Lock as RedlockLock, Settings as RedlockSettings } from 'redlock';

import {
  DistributedLockError,
  MutexAbortedError,
  MutexDeadlockError,
  MutexLockedError,
  MutexTimeoutError,
} from './errors';
import { Mutex, MutexState } from './mutex';
import {
  AsyncLocalStorageLike,
  createAsyncLocalStorage,
  createFallbackAsyncLocalStorage,
} from './asyncContext';
import { generateInstanceId } from './id';
import { importOptionalModule, loadOptionalModule } from './moduleLoader';

export type OperationType<T = unknown> = (args: {
  requestTime: number;
  startTime: number;
  currentMutex: Mutex;
  heldMutexIds: string[];
  contextToken: MutexRunContext;
  abortSignal: AbortSignal;
}) => Promise<T>;

interface LockState extends MutexState {
  tail: Promise<void>;
}

export interface RedisLockOptions {
  /**
   * Single URL or list of URLs for redis instances. Used only when a Redlock
   * instance is not provided directly.
   */
  urls: string | string[];
  /** Optional Redlock settings overrides when creating the internal instance. */
  settings?: Partial<RedlockSettings>;
  /**
   * Custom factory to create redis clients. The provided URL will be passed in.
   * This is useful when TLS or other configuration is required.
   */
  createClient?: (url: string) => IORedisClient | IORedisCluster;
}

export interface UniqueMutexManagerOptions {
  /**
   * A Redlock instance to use for distributed locking. When provided the
   * `redis` option is ignored.
   */
  redlock?: Redlock;
  /**
   * Configuration used to create a Redlock instance internally.
   */
  redis?: RedisLockOptions;
  /**
   * Duration in milliseconds that the distributed lock should live before it
   * needs to be extended. Defaults to 30 seconds.
   */
  lockTTL?: number;
  /**
   * Interval in milliseconds to extend the distributed lock. If omitted it
   * defaults to half of `lockTTL`. Set to `0` or a negative value to disable
   * automatic extensions.
   */
  lockExtendInterval?: number;
  /**
   * Optional redis client used to coordinate distributed deadlock detection
   * metadata. When omitted and `redis` options are provided, the first created
   * client will be reused. Providing a client is recommended when supplying a
   * custom Redlock instance.
   */
  coordinationClient?: IORedisClient | IORedisCluster;
}

interface DistributedLockHandle {
  release: () => Promise<void>;
}

const DEFAULT_LOCK_TTL = 30_000;

interface HeldLockState {
  count: number;
  distributedLock?: DistributedLockHandle;
}

interface OperationContext {
  id: string;
  held: Map<string, HeldLockState>;
  waitingFor?: string;
}

const CONTEXT_TOKEN_SYMBOL = Symbol('unique-mutex-context');

export interface MutexRunContext {
  readonly id: string;
}

type MutexRunContextInternal = MutexRunContext & {
  readonly managerId: string;
  readonly [CONTEXT_TOKEN_SYMBOL]: true;
};

function toArray(value: string | string[]): string[] {
  return Array.isArray(value) ? value : [value];
}

function isErrorWithName(error: unknown, name: string): error is Error {
  return error instanceof Error && error.name === name;
}

function isExecutionError(error: unknown): error is ExecutionError {
  return isErrorWithName(error, 'ExecutionError');
}

function isResourceLockedError(error: unknown): boolean {
  return isErrorWithName(error, 'ResourceLockedError');
}

function extractRedisString(value: unknown): string | undefined {
  if (typeof value === 'string') {
    return value;
  }
  if (typeof value === 'number' || typeof value === 'bigint') {
    return value.toString();
  }
  if (typeof Buffer !== 'undefined' && Buffer.isBuffer(value)) {
    return value.toString();
  }
  if (Array.isArray(value)) {
    for (let index = value.length - 1; index >= 0; index -= 1) {
      const entry = extractRedisString(value[index]);
      if (entry !== undefined) {
        return entry;
      }
    }
  }
  return undefined;
}

export class UniqueMutexManager {
  private static globalRequestCounter = 0;
  private static registrationLocks = new Map<string, Promise<void>>();

  private readonly locks = new Map<string, LockState>();
  private readonly createdRedisClients: Array<IORedisClient | IORedisCluster> = [];
  private readonly lockTTL: number;
  private readonly lockExtendInterval: number;
  private context: AsyncLocalStorageLike<OperationContext>;
  private supportsAsyncContext: boolean;
  private readonly lockOwners = new Map<string, OperationContext>();
  private readonly coordinationClient?: IORedisClient | IORedisCluster;
  private readonly instanceId: string;
  private redlock?: Redlock;
  private redlockPromise?: Promise<Redlock>;
  private contextCounter = 0;
  private readonly metadataTTL: number;
  private readonly contextTokens = new WeakMap<OperationContext, MutexRunContextInternal>();
  private readonly tokenContexts = new WeakMap<MutexRunContextInternal, OperationContext>();
  private readonly contextReady: Promise<void>;
  private queueTokenCounter = 0;
  private readonly requestQueues = new Map<string, number[]>();
  private readonly queueEntryTTL: number;

  private static readonly REDIS_PREFIX = 'uniquemutex';

  constructor(options: UniqueMutexManagerOptions = {}) {
    this.context = createFallbackAsyncLocalStorage<OperationContext>();
    this.supportsAsyncContext = false;
    this.contextReady = createAsyncLocalStorage<OperationContext>()
      .then(({ storage, supportsAsync }) => {
        this.context = storage;
        this.supportsAsyncContext = supportsAsync;
      })
      .catch(() => undefined);
    this.lockTTL = options.lockTTL ?? DEFAULT_LOCK_TTL;
    this.lockExtendInterval =
      options.lockExtendInterval ?? Math.max(Math.floor(this.lockTTL / 2), 0);
    this.metadataTTL = Math.max(this.lockTTL * 20, 10 * 60_000);
    this.queueEntryTTL = Math.max(this.lockTTL * 4, 2_000);
    this.instanceId = generateInstanceId();

    if (options.redlock) {
      this.redlock = options.redlock;
      this.coordinationClient = options.coordinationClient;
    } else if (options.redis) {
      const urls = toArray(options.redis.urls);
      if (urls.length === 0) {
        throw new Error('At least one redis url is required when using redis-based locking');
      }

      const clients = urls.map((url) => {
        if (options.redis?.createClient) {
          return options.redis.createClient(url);
        }

        const IORedisModule = loadOptionalModule<IORedisNamespace>('ioredis');
        if (!IORedisModule) {
          throw new Error(
            'Optional dependency "ioredis" is required to use the redis-based locking helpers'
          );
        }

        const IORedisCtor =
          (IORedisModule as { default?: new (...args: unknown[]) => IORedisClient }).default ??
          ((IORedisModule as unknown) as new (...args: unknown[]) => IORedisClient);
        const client = new IORedisCtor(url);
        this.createdRedisClients.push(client);
        return client;
      });

      this.redlockPromise = this.createRedlock(clients, options.redis.settings);
      this.coordinationClient = options.coordinationClient ?? clients[0];
    } else {
      this.coordinationClient = options.coordinationClient;
    }
  }

  /**
   * Creates a reusable context token that can be passed to future
   * `runOperation` calls when async context propagation is not available.
   */
  createContext(): MutexRunContext {
    const context = this.allocateContext();
    return this.ensureContextToken(context);
  }

  /**
   * Returns the current operation context token if one is active.
   */
  getCurrentContext(): MutexRunContext | undefined {
    const context = this.context.getStore();
    if (!context) {
      return undefined;
    }
    return this.ensureContextToken(context);
  }

  /**
   * Indicates whether async context propagation is natively supported.
   */
  isAsyncContextTrackingSupported(): boolean {
    return this.supportsAsyncContext;
  }

  /**
   * Gracefully closes any redis clients that were created internally.
   */
  async dispose(): Promise<void> {
    await Promise.all(
      this.createdRedisClients.map(async (client) => {
        const anyClient = client as unknown as {
          quit?: () => Promise<void>;
          disconnect?: () => void;
        };

        if (typeof anyClient.quit === 'function') {
          try {
            await anyClient.quit();
            return;
          } catch {
            // ignore and fall through to disconnect
          }
        }

        if (typeof anyClient.disconnect === 'function') {
          anyClient.disconnect();
        }
      })
    );
    this.createdRedisClients.length = 0;
  }

  private async ensureRedlock(): Promise<Redlock | undefined> {
    if (this.redlock) {
      return this.redlock;
    }

    if (!this.redlockPromise) {
      return undefined;
    }

    const instance = await this.redlockPromise;
    this.redlock = instance;
    return instance;
  }

  private async createRedlock(
    clients: Array<IORedisClient | IORedisCluster>,
    settings?: Partial<RedlockSettings>
  ): Promise<Redlock> {
    const redlockModule = await importOptionalModule<typeof import('redlock')>('redlock');
    if (!redlockModule) {
      throw new Error(
        'Optional dependency "redlock" is required to use the redis-based locking helpers'
      );
    }

    const RedlockCtor =
      (redlockModule as {
        default?: new (
          clients: Array<IORedisClient | IORedisCluster>,
          settings?: Partial<RedlockSettings>
        ) => Redlock;
      }).default ??
      ((redlockModule as unknown) as new (
        clients: Array<IORedisClient | IORedisCluster>,
        settings?: Partial<RedlockSettings>
      ) => Redlock);

    return new RedlockCtor(clients, settings);
  }

  private getOrCreateLockState(id: string): LockState {
    let state = this.locks.get(id);
    if (!state) {
      state = { pending: 0, tail: Promise.resolve() };
      this.locks.set(id, state);
    }
    return state;
  }

  private async acquireDistributedLock(
    id: string,
    waitIfLocked: boolean
  ): Promise<DistributedLockHandle> {
    const redlock = await this.ensureRedlock();
    if (!redlock) {
      return { release: async () => {} };
    }

    const resource = `uniquemutex:${id}`;
    const settings = waitIfLocked ? undefined : { retryCount: 0 };

    try {
      const lock = await redlock.acquire([resource], this.lockTTL, settings);
      const timer = this.createLockExtension(lock);
      return {
        release: async () => {
          if (timer) {
            clearInterval(timer);
          }
          await lock.release();
        },
      };
    } catch (error) {
      if (isResourceLockedError(error) || (!waitIfLocked && isExecutionError(error))) {
        throw new MutexLockedError(id, `Distributed mutex with id "${id}" is already locked`);
      }
      throw new DistributedLockError(
        error instanceof Error ? error.message : 'Failed to acquire distributed lock'
      );
    }
  }

  private createLockExtension(lock: RedlockLock): ReturnType<typeof setInterval> | undefined {
    if (!this.redlock) {
      return undefined;
    }

    if (this.lockExtendInterval <= 0) {
      return undefined;
    }

    const interval = setInterval(async () => {
      try {
        await lock.extend(this.lockTTL);
      } catch {
        // If extending fails we let the operation continue; release will throw later.
      }
    }, this.lockExtendInterval);

    interval.unref?.();
    return interval;
  }

  private allocateContext(): OperationContext {
    const context: OperationContext = {
      id: `${this.instanceId}:${++this.contextCounter}`,
      held: new Map(),
    };
    this.ensureContextToken(context);
    return context;
  }

  private ensureContextToken(context: OperationContext): MutexRunContextInternal {
    let token = this.contextTokens.get(context);
    if (!token) {
      token = Object.freeze({
        id: context.id,
        managerId: this.instanceId,
        [CONTEXT_TOKEN_SYMBOL]: true as const,
      });
      this.contextTokens.set(context, token);
      this.tokenContexts.set(token, context);
    }
    return token;
  }

  private getContextFromToken(token?: MutexRunContext): OperationContext | undefined {
    if (!token) {
      return undefined;
    }

    const internal = token as MutexRunContextInternal;
    if (internal[CONTEXT_TOKEN_SYMBOL] !== true || internal.managerId !== this.instanceId) {
      return undefined;
    }

    return this.tokenContexts.get(internal);
  }

  async runOperation<T>(
    id: string,
    operation: OperationType<T>,
    opts?: {
      waitIfLocked?: boolean;
      context?: MutexRunContext;
      timeoutMs?: number;
      signal?: AbortSignal;
      onAbort?: (abort: (reason?: unknown) => void) => void;
    }
  ): Promise<T> {
    const requestTime = Date.now();
    const requestSequence = ++UniqueMutexManager.globalRequestCounter;

    const abortController = new AbortController();
    const abortSignal = abortController.signal;
    const cleanupListeners: Array<() => void> = [];
    const abortCallbacks: Array<(reason?: unknown) => void> = [];
    const abort = (reason?: unknown) => {
      if (!abortSignal.aborted) {
        abortController.abort(reason);
      }
      for (const callback of [...abortCallbacks]) {
        try {
          callback(reason);
        } catch {
          // Ignore callback failures.
        }
      }
    };
    const registerAbortCallback = (callback: (reason?: unknown) => void): (() => void) => {
      abortCallbacks.push(callback);
      return () => {
        const index = abortCallbacks.indexOf(callback);
        if (index !== -1) {
          abortCallbacks.splice(index, 1);
        }
      };
    };
    const abortable = Boolean(opts?.signal || opts?.onAbort);

    if (opts?.signal) {
      if (opts.signal.aborted) {
        throw new MutexAbortedError(id, opts.signal.reason);
      }
      const forwardAbort = () => abort(opts.signal?.reason);
      opts.signal.addEventListener('abort', forwardAbort);
      cleanupListeners.push(() => opts.signal?.removeEventListener('abort', forwardAbort));
    }

    if (opts?.onAbort) {
      try {
        opts.onAbort((reason?: unknown) => abort(reason));
      } catch {
        // Ignore failures from consumer-provided hooks.
      }
    }

    const waitPreference = opts?.waitIfLocked ?? true;
    const timeoutMs = waitPreference ? opts?.timeoutMs : undefined;
    const canWait = waitPreference && (timeoutMs === undefined || timeoutMs > 0);

    const requestQueue = this.getRequestQueue(id);
    requestQueue.push(requestSequence);
    let queueEntryRemoved = false;
    const ensureQueueEntryRemoved = () => {
      if (!queueEntryRemoved) {
        this.removeRequestFromQueue(id, requestSequence);
        queueEntryRemoved = true;
      }
    };
    try {
      const preQueueToken = canWait
        ? await this.enqueueDistributedQueue(id, requestTime, requestSequence)
        : undefined;

      await this.contextReady;

      const requestedContext = this.getContextFromToken(opts?.context);
      const existingContext = this.context.getStore();
      const context = requestedContext ?? existingContext ?? this.allocateContext();
      this.ensureContextToken(context);

      const run = async () => {
        try {
          return await this.runWithContext(
            id,
            operation,
            waitPreference,
            timeoutMs,
            requestTime,
            requestSequence,
            ensureQueueEntryRemoved,
            preQueueToken,
            context,
            abortSignal,
            abort,
            abortable,
            registerAbortCallback
          );
        } finally {
          for (const cleanup of cleanupListeners) {
            try {
              cleanup();
            } catch {
              // Ignore listener cleanup errors.
            }
          }
          abortCallbacks.length = 0;
        }
      };

      if (existingContext === context) {
        return run();
      }

      return this.context.run(context, run);
    } catch (error) {
      ensureQueueEntryRemoved();
      const lockState = this.locks.get(id);
      if (lockState) {
        this.cleanupLockStateIfIdle(id, lockState);
      }
      throw error;
    }
  }

  private async runWithContext<T>(
    id: string,
    operation: OperationType<T>,
    waitPreference: boolean,
    timeoutMs: number | undefined,
    requestTime: number,
    requestSequence: number,
    removeQueueEntry: () => void,
    preQueueToken: string | undefined,
    context: OperationContext,
    abortSignal: AbortSignal,
    abortFn: (reason?: unknown) => void,
    abortable: boolean,
    registerAbortCallback: (callback: (reason?: unknown) => void) => () => void
  ): Promise<T> {
    const lockState = this.getOrCreateLockState(id);
    const mutex = new Mutex(id, lockState);
    const contextToken = this.ensureContextToken(context);
    let queueEntryRemoved = false;
    const ensureQueueEntryRemoved = () => {
      if (!queueEntryRemoved) {
        removeQueueEntry();
        queueEntryRemoved = true;
      }
    };
    const timeoutError =
      waitPreference && timeoutMs !== undefined ? new MutexTimeoutError(id, timeoutMs) : undefined;
    const canWait = waitPreference && (timeoutMs === undefined || timeoutMs > 0);
    let timedOut = false;
    let timeoutHandle: ReturnType<typeof setTimeout> | undefined;
    let aborted = abortSignal.aborted;
    const abortListeners: Array<() => void> = [];

    if (abortSignal.aborted) {
      ensureQueueEntryRemoved();
      this.cleanupLockStateIfIdle(id, lockState);
      throw new MutexAbortedError(id, abortSignal.reason);
    }

    const clearTimer = () => {
      if (timeoutHandle) {
        clearTimeout(timeoutHandle);
        timeoutHandle = undefined;
      }
    };

    const markTimedOut = () => {
      if (timedOut) {
        return;
      }
      timedOut = true;
      clearTimer();
      if (canWait) {
        const reset = this.updateWaitingState(context, undefined);
        if (reset) {
          reset.catch(() => undefined);
        }
      }
      if (timeoutError && !abortSignal.aborted) {
        abortFn(timeoutError);
      }
    };

    const markAborted = () => {
      if (aborted) {
        return;
      }
      aborted = true;
      clearTimer();
      if (canWait) {
        const reset = this.updateWaitingState(context, undefined);
        if (reset) {
          reset.catch(() => undefined);
        }
      }
    };

    const addAbortListener = (listener: () => void) => {
      abortSignal.addEventListener('abort', listener);
      abortListeners.push(listener);
    };

    const cleanupAbortListener = () => {
      while (abortListeners.length > 0) {
        const listener = abortListeners.pop();
        if (listener) {
          abortSignal.removeEventListener('abort', listener);
        }
      }
    };
    addAbortListener(() => {
      markAborted();
    });

    try {
      if (context.held.has(id)) {
        const startTime = Date.now();
        this.incrementHeldLock(context, id);
        try {
          const result = await operation({
            requestTime,
            startTime,
            currentMutex: mutex,
            heldMutexIds: this.getHeldMutexIds(context),
            contextToken,
            abortSignal,
          });
          if (aborted || abortSignal.aborted) {
            markAborted();
            throw new MutexAbortedError(id, abortSignal.reason);
          }
          return result;
        } finally {
          const releaseResult = this.decrementHeldLock(context, id);
          if (releaseResult.removed) {
            await this.clearRemoteOwner(id, context.id);
          }
          if (releaseResult.handle) {
            await releaseResult.handle.release().catch(() => undefined);
          }
          ensureQueueEntryRemoved();
          this.cleanupLockStateIfIdle(id, lockState);
        }
      }

      const queue = this.requestQueues.get(id);
      const hasEarlierRequest = Boolean(
        queue && queue.includes(requestSequence) && queue[0] !== requestSequence
      );
      const alreadyLocked = lockState.pending > 0 || hasEarlierRequest;
      if (!canWait && alreadyLocked) {
        if (timeoutError) {
          timedOut = true;
          ensureQueueEntryRemoved();
          this.cleanupLockStateIfIdle(id, lockState);
          throw timeoutError;
        }
        ensureQueueEntryRemoved();
        this.cleanupLockStateIfIdle(id, lockState);
        return undefined as unknown as T;
      }

      lockState.pending += 1;

      const previousTail = lockState.tail;
      let resolveReady: (() => void) | undefined;
      let rejectReady: ((error: unknown) => void) | undefined;
      const ready = new Promise<void>((resolve, reject) => {
        resolveReady = resolve;
        rejectReady = reject;
      });
      ready.catch(() => undefined);

      let runActual: (() => Promise<T>) | undefined;

      const runPromise = previousTail.then(async () => {
        await ready;
        return runActual!();
      });

      lockState.tail = runPromise
        .then(() => undefined)
        .catch(() => undefined);

      let waitingMarked = false;
      try {
        if (canWait) {
          waitingMarked = true;
          const waitingUpdate = this.updateWaitingState(context, id);
          if (waitingUpdate) {
            await waitingUpdate;
          }
        }

        const deadlockCycle = canWait
          ? this.coordinationClient
            ? await this.detectRemoteDeadlock(context, id)
            : this.detectLocalDeadlock(context, id)
          : undefined;
        if (deadlockCycle) {
          const reset = waitingMarked ? this.updateWaitingState(context, undefined) : undefined;
          if (reset) {
            await reset;
          }
          throw new MutexDeadlockError(id, deadlockCycle);
        }

        runActual = async () => {
          let distributedLock: DistributedLockHandle | undefined;
          let waitingCleared = !waitingMarked;
          let operationStarted = false;
          let queueToken: string | undefined = preQueueToken;

          try {
            if (timeoutError && timedOut) {
              throw timeoutError;
            }

            if (aborted || abortSignal.aborted) {
              markAborted();
              throw new MutexAbortedError(id, abortSignal.reason);
            }

            if (canWait) {
              if (!queueToken) {
                queueToken = await this.enqueueDistributedQueue(id, requestTime, requestSequence);
              }
              if (queueToken) {
                try {
                  await this.waitForDistributedTurn(id, queueToken, abortSignal);
                } catch (error) {
                  await this.removeDistributedQueueEntry(id, queueToken);
                  queueToken = undefined;
                  throw error;
                }
              }
            }

            try {
              distributedLock = await this.acquireDistributedLock(id, canWait);
            } catch (error) {
              if (timeoutError && error instanceof MutexLockedError) {
                throw timeoutError;
              }
              if (!canWait && error instanceof MutexLockedError) {
                return undefined as unknown as T;
              }
              throw error;
            }

            if (timeoutError && timedOut) {
              if (distributedLock) {
                await distributedLock.release().catch(() => undefined);
              }
              throw timeoutError;
            }

            if (aborted || abortSignal.aborted) {
              markAborted();
              if (distributedLock) {
                await distributedLock.release().catch(() => undefined);
              }
              throw new MutexAbortedError(id, abortSignal.reason);
            }

            if (waitingMarked) {
              const cleared = this.updateWaitingState(context, undefined);
              if (cleared) {
                await cleared;
              }
              waitingCleared = true;
            }

            clearTimer();
            operationStarted = true;

            const count = this.incrementHeldLock(context, id, distributedLock);
            if (count === 1) {
              await this.setRemoteOwner(id, context);
            }

            const startTime = Date.now();
            const result = await operation({
              requestTime,
              startTime,
              currentMutex: mutex,
              heldMutexIds: this.getHeldMutexIds(context),
              contextToken,
              abortSignal,
            });
            if (aborted || abortSignal.aborted) {
              markAborted();
              throw new MutexAbortedError(id, abortSignal.reason);
            }
            return result;
          } finally {
            if ((!waitingCleared || context.waitingFor === id) && waitingMarked) {
              const reset = this.updateWaitingState(context, undefined);
              if (reset) {
                await reset;
              }
            }

            if (operationStarted) {
              const releaseResult = this.decrementHeldLock(context, id);
              if (releaseResult.removed) {
                await this.clearRemoteOwner(id, context.id);
              }
              if (releaseResult.handle) {
                await releaseResult.handle.release().catch(() => undefined);
              } else if (distributedLock && releaseResult.removed) {
                await distributedLock.release().catch(() => undefined);
              }
            }

            if (queueToken) {
              await this.removeDistributedQueueEntry(id, queueToken);
              queueToken = undefined;
            }

            lockState.pending -= 1;
            ensureQueueEntryRemoved();
            this.cleanupLockStateIfIdle(id, lockState);
          }
        };

        resolveReady?.();
        resolveReady = undefined;
        rejectReady = undefined;
      } catch (error) {
        lockState.pending -= 1;
        ensureQueueEntryRemoved();
        this.cleanupLockStateIfIdle(id, lockState);
        rejectReady?.(error);
        resolveReady = undefined;
        rejectReady = undefined;
        throw error;
      }

      const hasTimeoutWrapper = Boolean(timeoutError && canWait && timeoutMs !== undefined && timeoutMs > 0);
      const needsWrapper = hasTimeoutWrapper || abortable;
      if (!needsWrapper) {
        return runPromise;
      }

      let settled = false;
      const abortCleanupFns: Array<() => void> = [];
      const settle = () => {
        if (settled) {
          return false;
        }
        settled = true;
        clearTimer();
        cleanupAbortListener();
        while (abortCleanupFns.length > 0) {
          const cleanup = abortCleanupFns.pop();
          try {
            cleanup?.();
          } catch {
            // Ignore cleanup failures.
          }
        }
        return true;
      };

      const races: Promise<T>[] = [runPromise];

      if (hasTimeoutWrapper && timeoutError && timeoutMs !== undefined) {
        races.push(
          new Promise<never>((_, reject) => {
            timeoutHandle = setTimeout(() => {
              if (!settle()) {
                return;
              }
              markTimedOut();
              reject(timeoutError);
            }, timeoutMs);

            timeoutHandle.unref?.();
          })
        );
      }

      if (abortable) {
        races.push(
          new Promise<never>((_, reject) => {
            const rejectAbort = () => {
              if (!settle()) {
                return;
              }
              markAborted();
              reject(new MutexAbortedError(id, abortSignal.reason));
            };

            if (aborted || abortSignal.aborted) {
              rejectAbort();
              return;
            }

            const unregister = registerAbortCallback(() => rejectAbort());
            abortCleanupFns.push(unregister);
          })
        );
      }

      return Promise.race(races).finally(() => {
        settle();
      });
    } finally {
      cleanupAbortListener();
    }
  }

  private getHeldMutexIds(context: OperationContext): string[] {
    return Array.from(context.held.keys());
  }

  private incrementHeldLock(
    context: OperationContext,
    id: string,
    distributedLock?: DistributedLockHandle
  ): number {
    const existing = context.held.get(id);
    if (existing) {
      existing.count += 1;
      if (distributedLock) {
        existing.distributedLock = distributedLock;
      }
      return existing.count;
    }

    context.held.set(id, { count: 1, distributedLock });
    this.lockOwners.set(id, context);
    return 1;
  }

  private decrementHeldLock(
    context: OperationContext,
    id: string
  ): { handle?: DistributedLockHandle; removed: boolean } {
    const state = context.held.get(id);
    if (!state) {
      return { removed: false };
    }

    state.count -= 1;
    if (state.count <= 0) {
      context.held.delete(id);
      if (this.lockOwners.get(id) === context) {
        this.lockOwners.delete(id);
      }
      const handle = state.distributedLock;
      state.distributedLock = undefined;
      return { handle, removed: true };
    }

    return { removed: false };
  }

  private getContextKey(contextId: string): string {
    return `${UniqueMutexManager.REDIS_PREFIX}:context:${contextId}`;
  }

  private getOwnerKey(id: string): string {
    return `${UniqueMutexManager.REDIS_PREFIX}:owner:${id}`;
  }

  private getQueueKey(id: string): string {
    return `${UniqueMutexManager.REDIS_PREFIX}:queue:${id}`;
  }

  private getQueuePositionKey(id: string): string {
    return `${UniqueMutexManager.REDIS_PREFIX}:queue:${id}:pos`;
  }

  private getQueueTurnKey(id: string): string {
    return `${UniqueMutexManager.REDIS_PREFIX}:queue:${id}:turn`;
  }

  private getQueueEntryPrefix(id: string): string {
    return `${UniqueMutexManager.REDIS_PREFIX}:queue:${id}:entry:`;
  }

  private getQueueEntryKey(id: string, position: string | number): string {
    return `${this.getQueueEntryPrefix(id)}${position}`;
  }

  private getRequestQueue(id: string): number[] {
    let queue = this.requestQueues.get(id);
    if (!queue) {
      queue = [];
      this.requestQueues.set(id, queue);
    }
    return queue;
  }

  private removeRequestFromQueue(id: string, sequence: number): void {
    const queue = this.requestQueues.get(id);
    if (!queue) {
      return;
    }

    const index = queue.indexOf(sequence);
    if (index !== -1) {
      queue.splice(index, 1);
    }

    if (queue.length === 0) {
      this.requestQueues.delete(id);
    }
  }

  private cleanupLockStateIfIdle(id: string, lockState: LockState): void {
    if (lockState.pending === 0) {
      const queue = this.requestQueues.get(id);
      if (!queue || queue.length === 0) {
        this.locks.delete(id);
      }
    }
  }

  private getQueueToken(
    requestTime: number,
    requestSequence: number
  ): string {
    const timeComponent = requestTime.toString().padStart(13, '0');
    const sequenceComponent = requestSequence.toString().padStart(16, '0');
    const localComponent = (++this.queueTokenCounter).toString().padStart(8, '0');
    return `${timeComponent}:${sequenceComponent}:${this.instanceId}:${localComponent}`;
  }

  private updateWaitingState(
    context: OperationContext,
    waitingFor?: string
  ): Promise<void> | void {
    context.waitingFor = waitingFor;

    if (!this.coordinationClient) {
      return undefined;
    }

    const key = this.getContextKey(context.id);
    const pipeline = this.coordinationClient.multi();

    pipeline.hset(key, 'instanceId', this.instanceId);

    if (waitingFor) {
      pipeline.hset(key, 'waitingFor', waitingFor);
      pipeline.pexpire(key, this.metadataTTL);
    } else {
      pipeline.hdel(key, 'waitingFor');
      if (context.held.size === 0) {
        pipeline.del(key);
      } else {
        pipeline.pexpire(key, this.metadataTTL);
      }
    }

    return pipeline
      .exec()
      .then(() => undefined)
      .catch(() => undefined);
  }

  private async enqueueDistributedQueue(
    id: string,
    requestTime: number,
    requestSequence: number
  ): Promise<string | undefined> {
    if (!this.coordinationClient) {
      return undefined;
    }

    const positionKey = this.getQueuePositionKey(id);
    const turnKey = this.getQueueTurnKey(id);

    const previous = UniqueMutexManager.registrationLocks.get(id) ?? Promise.resolve();
    let releaseLock: (() => void) | undefined;
    const current = new Promise<void>((resolve) => {
      releaseLock = resolve;
    });
    UniqueMutexManager.registrationLocks.set(id, current);

    try {
      await previous;

      const result = await this.coordinationClient.eval(
        `local posKey = KEYS[1]
local turnKey = KEYS[2]
local ttl = tonumber(ARGV[1])
local entryTtl = tonumber(ARGV[2])
local entryPrefix = ARGV[3]
local position = redis.call('INCR', posKey)
redis.call('PEXPIRE', posKey, ttl)
if redis.call('EXISTS', turnKey) == 0 then
  redis.call('SET', turnKey, 1, 'PX', ttl)
else
  redis.call('PEXPIRE', turnKey, ttl)
end
redis.call('SET', entryPrefix .. position, '', 'PX', entryTtl)
return tostring(position)
`,
        2,
        positionKey,
        turnKey,
        this.metadataTTL.toString(),
        this.queueEntryTTL.toString(),
        this.getQueueEntryPrefix(id)
      );
      return extractRedisString(result);
    } catch {
      return undefined;
    } finally {
      releaseLock?.();
    }
  }

  private async waitForDistributedTurn(
    id: string,
    token: string,
    abortSignal: AbortSignal
  ): Promise<void> {
    if (!this.coordinationClient) {
      return;
    }

    const position = Number(token);
    if (!Number.isFinite(position)) {
      return;
    }

    const turnKey = this.getQueueTurnKey(id);
    const entryKey = this.getQueueEntryKey(id, token);

    while (true) {
      if (abortSignal.aborted) {
        const reason = abortSignal.reason;
        if (reason instanceof Error) {
          throw reason;
        }
        throw new MutexAbortedError(id, reason);
      }

      try {
        await this.coordinationClient.pexpire(entryKey, this.queueEntryTTL);
      } catch {
        // Ignore heartbeat failures.
      }

      let currentTurn: number | undefined;

      try {
        const rawCurrent = await this.coordinationClient.get(turnKey);
        const current = rawCurrent === null ? undefined : extractRedisString(rawCurrent);
        if (current !== undefined) {
          const turn = Number(current);
          if (!Number.isNaN(turn)) {
            currentTurn = turn;
            if (turn === position) {
              return;
            }
            if (turn > position) {
              return;
            }
          }
        } else if (position === 1) {
          try {
            const setResult = await this.coordinationClient.set(
              turnKey,
              token,
              'PX',
              this.metadataTTL,
              'NX'
            );
            if (setResult) {
              return;
            }
          } catch {
            // Ignore failures to initialize the turn.
          }
        }
      } catch {
        // If we can't read the turn, allow retry.
      }

      if (currentTurn !== undefined && currentTurn < position) {
        await this.advanceDistributedTurnIfMissing(id, currentTurn);
      } else if (currentTurn === undefined && position > 1) {
        await this.advanceDistributedTurnIfMissing(id, position - 1);
      }

      await new Promise((resolve) => {
        setTimeout(resolve, 10);
      });
    }
  }

  private async advanceDistributedTurnIfMissing(id: string, position: number): Promise<void> {
    if (!this.coordinationClient) {
      return;
    }

    if (!Number.isFinite(position) || position < 1) {
      return;
    }

    const entryKey = this.getQueueEntryKey(id, position);
    const turnKey = this.getQueueTurnKey(id);
    const positionKey = this.getQueuePositionKey(id);

    try {
      await this.coordinationClient.eval(
        `local entryKey = KEYS[1]
local turnKey = KEYS[2]
local posKey = KEYS[3]
local position = tonumber(ARGV[1])
local ttl = tonumber(ARGV[2])
if position == nil then
  return 0
end
if redis.call('EXISTS', entryKey) == 1 then
  return 0
end
local highest = redis.call('GET', posKey)
if highest == false then
  redis.call('DEL', turnKey)
  return 1
end
local highestNum = tonumber(highest)
if highestNum ~= nil and highestNum <= position then
  redis.call('DEL', posKey)
  redis.call('DEL', turnKey)
  return 1
end
redis.call('SET', turnKey, position + 1, 'PX', ttl)
redis.call('PEXPIRE', posKey, ttl)
return 1
`,
        3,
        entryKey,
        turnKey,
        positionKey,
        position.toString(),
        this.metadataTTL.toString()
      );
    } catch {
      // Ignore cleanup failures.
    }
  }

  private async removeDistributedQueueEntry(id: string, token: string): Promise<void> {
    if (!this.coordinationClient) {
      return;
    }

    const position = Number(token);
    if (!Number.isFinite(position)) {
      return;
    }

    const turnKey = this.getQueueTurnKey(id);
    const positionKey = this.getQueuePositionKey(id);

    try {
      await this.coordinationClient.eval(
        `local turnKey = KEYS[1]
local posKey = KEYS[2]
local position = tonumber(ARGV[1])
local ttl = tonumber(ARGV[2])
local entryPrefix = ARGV[3]
redis.call('DEL', entryPrefix .. position)
local current = redis.call('GET', turnKey)
if current ~= false then
  local currentNum = tonumber(current)
  if currentNum ~= nil and currentNum <= position then
    redis.call('SET', turnKey, position + 1, 'PX', ttl)
  else
    redis.call('PEXPIRE', turnKey, ttl)
  end
else
  redis.call('SET', turnKey, position + 1, 'PX', ttl)
end
local highest = redis.call('GET', posKey)
if highest == false then
  redis.call('DEL', turnKey)
  return 1
end
local highestNum = tonumber(highest)
local nextTurn = redis.call('GET', turnKey)
if nextTurn == false then
  redis.call('DEL', posKey)
  return 1
end
local nextTurnNum = tonumber(nextTurn)
if nextTurnNum ~= nil and highestNum ~= nil and nextTurnNum > highestNum then
  redis.call('DEL', turnKey)
  redis.call('DEL', posKey)
else
  redis.call('PEXPIRE', posKey, ttl)
end
return 1
`,
        2,
        turnKey,
        positionKey,
        token,
        this.metadataTTL.toString(),
        this.getQueueEntryPrefix(id)
      );
    } catch {
      // Ignore cleanup failures.
    }
  }

  private async setRemoteOwner(id: string, context: OperationContext): Promise<void> {
    if (!this.coordinationClient) {
      return;
    }

    try {
      await this.coordinationClient.set(
        this.getOwnerKey(id),
        context.id,
        'PX',
        this.metadataTTL
      );
    } catch {
      // Metadata updates are best-effort.
    }
  }

  private async clearRemoteOwner(id: string, contextId: string): Promise<void> {
    if (!this.coordinationClient) {
      return;
    }

    try {
      const key = this.getOwnerKey(id);
      const rawOwner = await this.coordinationClient.get(key);
      const owner = rawOwner === null ? undefined : extractRedisString(rawOwner);
      if (owner === contextId) {
        await this.coordinationClient.del(key);
      }
    } catch {
      // Ignore failures.
    }
  }

  private async getRemoteOwner(id: string): Promise<string | undefined> {
    if (!this.coordinationClient) {
      return undefined;
    }

    try {
      const rawOwner = await this.coordinationClient.get(this.getOwnerKey(id));
      if (rawOwner === null) {
        return undefined;
      }
      return extractRedisString(rawOwner);
    } catch {
      return undefined;
    }
  }

  private async getRemoteWaiting(contextId: string): Promise<string | undefined> {
    if (!this.coordinationClient) {
      return undefined;
    }

    try {
      const waitingFor = await this.coordinationClient.hget(
        this.getContextKey(contextId),
        'waitingFor'
      );
      if (waitingFor === null) {
        return undefined;
      }
      return extractRedisString(waitingFor);
    } catch {
      return undefined;
    }
  }

  private detectLocalDeadlock(
    context: OperationContext,
    targetId: string
  ): string[] | undefined {
    const visitedContexts = new Set<OperationContext>();
    const visitedIds = new Set<string>();

    type StackNode =
      | { type: 'id'; id: string; path: string[] }
      | { type: 'context'; context: OperationContext; path: string[] };

    const stack: StackNode[] = [{ type: 'id', id: targetId, path: [targetId] }];

    while (stack.length > 0) {
      const node = stack.pop()!;

      if (node.type === 'id') {
        if (visitedIds.has(node.id)) {
          continue;
        }
        visitedIds.add(node.id);

        const owner = this.lockOwners.get(node.id);
        if (owner) {
          stack.push({ type: 'context', context: owner, path: node.path });
        }
        continue;
      }

      const currentContext = node.context;
      if (currentContext === context) {
        return [...node.path, targetId];
      }

      if (visitedContexts.has(currentContext)) {
        continue;
      }
      visitedContexts.add(currentContext);

      const waitingFor = currentContext.waitingFor;
      if (waitingFor) {
        stack.push({ type: 'id', id: waitingFor, path: [...node.path, waitingFor] });
      }
    }

    return undefined;
  }

  private async detectRemoteDeadlock(
    context: OperationContext,
    targetId: string
  ): Promise<string[] | undefined> {
    const visitedContexts = new Set<OperationContext>();
    const visitedIds = new Set<string>();
    const visitedRemoteContextIds = new Set<string>();

    type StackNode =
      | { type: 'id'; id: string; path: string[] }
      | { type: 'context'; context: OperationContext; path: string[] }
      | { type: 'remote-context'; contextId: string; path: string[] };

    const stack: StackNode[] = [{ type: 'id', id: targetId, path: [targetId] }];

    while (stack.length > 0) {
      const node = stack.pop()!;

      if (node.type === 'id') {
        if (visitedIds.has(node.id)) {
          continue;
        }
        visitedIds.add(node.id);

        const owner = this.lockOwners.get(node.id);
        if (owner) {
          stack.push({ type: 'context', context: owner, path: node.path });
          continue;
        }

        const remoteOwner = await this.getRemoteOwner(node.id);
        if (remoteOwner) {
          stack.push({ type: 'remote-context', contextId: remoteOwner, path: node.path });
        }
        continue;
      }

      if (node.type === 'remote-context') {
        if (visitedRemoteContextIds.has(node.contextId)) {
          continue;
        }
        visitedRemoteContextIds.add(node.contextId);

        if (node.contextId === context.id) {
          return [...node.path, targetId];
        }

        const waitingFor = await this.getRemoteWaiting(node.contextId);
        if (waitingFor) {
          stack.push({ type: 'id', id: waitingFor, path: [...node.path, waitingFor] });
        }
        continue;
      }

      const currentContext = node.context;
      if (currentContext === context) {
        return [...node.path, targetId];
      }

      if (visitedContexts.has(currentContext)) {
        continue;
      }
      visitedContexts.add(currentContext);

      const waitingFor = currentContext.waitingFor;
      if (waitingFor) {
        stack.push({ type: 'id', id: waitingFor, path: [...node.path, waitingFor] });
      }
    }

    return undefined;
  }
}
