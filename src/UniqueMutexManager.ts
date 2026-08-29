import {
  DistributedLockError,
  LockExtensionError,
  MutexAbortedError,
  MutexDeadlockError,
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

// This library is designed to be environment-agnostic.
// Optional dependencies (ioredis, redlock) and Node-specific APIs (node:async_hooks, node:crypto)
// are handled gracefully with fallbacks, making it suitable for use in Browsers and React Native.

export type OperationType<T = unknown> = (args: {
  requestTime: number;
  startTime: number;
  currentMutex: Mutex;
  heldMutexIds: string[];
  contextToken: MutexRunContext;
  abortSignal: AbortSignal;
  /**
   * Throws LockExtensionError if the lock extension has failed.
   * Call this between steps in your operation to detect lock loss early
   * and prevent work on stale locks.
   */
  assertLock: () => void;
}) => Promise<T>;

interface LockState extends MutexState {
  tail: Promise<void>;
}

/** Minimal Redis surface used by distributed locking and deadlock metadata. */
export interface RedisClientLike {
  evalsha(...args: any[]): Promise<unknown>;
  eval(...args: any[]): Promise<unknown>;
  set(...args: any[]): Promise<unknown>;
  get(key: string): Promise<string | null>;
  hset(...args: any[]): Promise<unknown>;
  hget(key: string, field: string): Promise<string | null>;
  pexpire(key: string, milliseconds: number): Promise<unknown>;
  del(...keys: string[]): Promise<unknown>;
  quit?: () => Promise<unknown>;
  disconnect?: () => void;
}

/** Redlock settings used by UniqueMutexManager without requiring Redlock at typecheck time. */
export interface RedlockSettingsLike {
  driftFactor?: number;
  retryCount?: number;
  retryDelay?: number;
  retryJitter?: number;
  automaticExtensionThreshold?: number;
}

/** Minimal immutable lease returned by a Redlock-compatible implementation. */
export interface RedlockLockLike {
  readonly resources: string[];
  expiration: number;
  release(): Promise<unknown>;
}

/** Minimal Redlock surface accepted by the manager. */
export interface RedlockLike {
  readonly settings?: RedlockSettingsLike;
  acquire(...args: any[]): Promise<RedlockLockLike>;
  extend(...args: any[]): Promise<RedlockLockLike>;
}

export interface RedisLockOptions {
  /**
   * Single URL or list of URLs for Redis instances. Omit when supplying
   * externally owned `clients`.
   */
  urls?: string | string[];
  /**
   * Existing Redis clients to share across managers. These clients are never
   * closed by `dispose()`. Do not provide both `clients` and `urls`.
   */
  clients?: RedisClientLike[];
  /** Optional Redlock settings overrides when creating the internal instance. */
  settings?: Partial<RedlockSettingsLike>;
  /**
   * Custom factory to create redis clients. The provided URL will be passed in.
   * This is useful when TLS or other configuration is required.
   */
  createClient?: (url: string) => RedisClientLike;
}

export interface UniqueMutexManagerOptions {
  /**
   * A Redlock instance to use for distributed locking. When provided the
   * `redis` option is ignored.
   */
  redlock?: RedlockLike;
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
  coordinationClient?: RedisClientLike;
  /**
   * Behavior when cross-process deadlock metadata cannot be read or written.
   * `strict` (default) fails nested distributed waits closed; `best-effort`
   * preserves availability but may temporarily lose deadlock detection.
   */
  coordinationFailureMode?: 'strict' | 'best-effort';
  /**
   * Optional distributed lock domain. Managers using different namespaces do
   * not contend even when they use the same logical mutex id. Supplying a
   * namespace also uses the collision-safe namespaced Redis key layout.
   */
  namespace?: string;
}

interface DistributedLockHandle {
  release: () => Promise<void>;
  /** Most recent immutable Redlock lease. */
  currentLock?: RedlockLockLike;
  /** Set to true when lock extension fails. */
  extensionFailed: boolean;
  extensionError?: Error;
  /** Called when extension fails - can be used to abort the operation. */
  onExtensionFailure?: (error: Error) => void;
}

const DEFAULT_LOCK_TTL = 30_000;
const MAX_TIMER_DELAY_MS = 2_147_483_647;

interface HeldLockState {
  count: number;
  ownershipToken: string;
  distributedLock?: DistributedLockHandle;
}

interface WaitingState {
  token: string;
  id: string;
  /** Exact lock ownership generations held when this wait began. */
  heldLocks: Map<string, string>;
}

interface PublishedOwnerState {
  value: string;
}

interface SerializedWaitingState {
  id: string;
  heldLocks: Record<string, string>;
}

interface OperationContext {
  id: string;
  held: Map<string, HeldLockState>;
  heldCounter: number;
  waits: Map<string, WaitingState>;
  waitCounter: number;
  publishedOwners: Map<string, PublishedOwnerState>;
  coordinationTail: Promise<void>;
}

const CONTEXT_TOKEN_SYMBOL = Symbol('unique-mutex-context');

export interface MutexRunContext {
  readonly id: string;
}

interface MutexRunOptionsBase {
  context?: MutexRunContext;
  timeoutMs?: number;
  signal?: AbortSignal;
  onAbort?: (abort: (reason?: unknown) => void) => void;
}

/** Options for the normal queueing mode. */
export interface MutexWaitOptions extends MutexRunOptionsBase {
  waitIfLocked?: true;
}

/** Options for a single non-blocking acquisition attempt. */
export interface MutexNoWaitOptions extends MutexRunOptionsBase {
  waitIfLocked: false;
}

export type MutexRunOptions = MutexWaitOptions | MutexNoWaitOptions;

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

function isExecutionError(
  error: unknown
): error is Error & { attempts?: Array<Promise<unknown> | unknown> } {
  return isErrorWithName(error, 'ExecutionError');
}

function isResourceLockedError(error: unknown): boolean {
  return isErrorWithName(error, 'ResourceLockedError');
}

export class UniqueMutexManager {
  private readonly locks = new Map<string, LockState>();
  private readonly createdRedisClients: RedisClientLike[] = [];
  private readonly lockTTL: number;
  private readonly lockExtendInterval: number;
  private context: AsyncLocalStorageLike<OperationContext>;
  private supportsAsyncContext: boolean;
  private readonly lockOwners = new Map<string, OperationContext>();
  private readonly coordinationClient?: RedisClientLike;
  private readonly instanceId: string;
  private redlock?: RedlockLike;
  private redlockPromise?: Promise<RedlockLike>;
  private contextCounter = 0;
  private readonly metadataTTL: number;
  private readonly contextTokens = new WeakMap<OperationContext, MutexRunContextInternal>();
  private readonly tokenContexts = new WeakMap<MutexRunContextInternal, OperationContext>();
  private readonly contextReady: Promise<void>;
  private readonly namespace?: string;
  private readonly redisPrefix: string;
  private readonly metadataPrefix: string;
  private readonly coordinationRefreshInterval: number;
  private readonly coordinationFailureMode: 'strict' | 'best-effort';

  private static readonly REDIS_PREFIX = 'uniquemutex';
  private static readonly REDIS_NAMESPACED_PREFIX = 'uniquemutex-v2';
  private static readonly REDIS_METADATA_V2_PREFIX = 'uniquemutex-meta-v2';
  private static readonly WAITS_V2_FIELD = 'waitsV2';
  private static readonly DELETE_OWNER_IF_MATCHES_SCRIPT = `
    if redis.call('get', KEYS[1]) == ARGV[1] then
      return redis.call('del', KEYS[1])
    end
    return 0
  `;

  constructor(options: UniqueMutexManagerOptions = {}) {
    this.context = createFallbackAsyncLocalStorage<OperationContext>();
    this.supportsAsyncContext = false;
    // Attempt to load Node's AsyncLocalStorage, falling back to a stack-based
    // implementation for non-Node environments (Browsers, React Native).
    this.contextReady = createAsyncLocalStorage<OperationContext>()
      .then(({ storage, supportsAsync }) => {
        this.context = storage;
        this.supportsAsyncContext = supportsAsync;
      })
      .catch(() => undefined);
    this.lockTTL = options.lockTTL ?? DEFAULT_LOCK_TTL;
    if (!Number.isSafeInteger(this.lockTTL) || this.lockTTL <= 0) {
      throw new Error('lockTTL must be a positive safe integer number of milliseconds');
    }
    if (this.lockTTL > MAX_TIMER_DELAY_MS) {
      throw new Error(`lockTTL must not exceed ${MAX_TIMER_DELAY_MS}ms`);
    }
    this.lockExtendInterval =
      options.lockExtendInterval ?? Math.max(Math.floor(this.lockTTL / 2), 0);
    if (!Number.isSafeInteger(this.lockExtendInterval)) {
      throw new Error('lockExtendInterval must be a safe integer number of milliseconds');
    }
    if (this.lockExtendInterval > MAX_TIMER_DELAY_MS) {
      throw new Error(`lockExtendInterval must not exceed ${MAX_TIMER_DELAY_MS}ms`);
    }
    if (this.lockExtendInterval > 0 && this.lockExtendInterval >= this.lockTTL) {
      throw new Error('lockExtendInterval must be shorter than lockTTL when automatic extension is enabled');
    }
    // Deadlock metadata should never linger for minutes after a crashed lease owner.
    // Keep enough floor for short-TTL/high-latency tests while refreshing long waits.
    this.metadataTTL = Math.max(this.lockTTL, 5_000);
    this.coordinationRefreshInterval = Math.max(250, Math.min(Math.floor(this.metadataTTL / 3), 30_000));
    this.coordinationFailureMode = options.coordinationFailureMode ?? 'strict';
    if (this.coordinationFailureMode !== 'strict' && this.coordinationFailureMode !== 'best-effort') {
      throw new Error('coordinationFailureMode must be "strict" or "best-effort"');
    }
    this.instanceId = generateInstanceId();
    const namespace = options.namespace?.trim();
    if (options.namespace !== undefined && !namespace) {
      throw new Error('Mutex namespace must be a non-empty string when provided');
    }
    this.namespace = namespace;
    this.redisPrefix = namespace
      ? `${UniqueMutexManager.REDIS_NAMESPACED_PREFIX}:${encodeURIComponent(namespace)}`
      : UniqueMutexManager.REDIS_PREFIX;
    this.metadataPrefix = namespace
      ? `${UniqueMutexManager.REDIS_METADATA_V2_PREFIX}:${encodeURIComponent(namespace)}`
      : `${UniqueMutexManager.REDIS_METADATA_V2_PREFIX}:legacy`;

    if (options.coordinationClient && !options.redlock && !options.redis) {
      throw new Error('coordinationClient requires distributed locking via redlock or redis');
    }

    if (options.redlock) {
      this.redlock = options.redlock;
      this.coordinationClient = options.coordinationClient;
    } else if (options.redis) {
      const suppliedClients = options.redis.clients;
      if (suppliedClients && options.redis.urls !== undefined) {
        throw new Error('Provide either redis.clients or redis.urls, not both');
      }
      if (suppliedClients && options.redis.createClient) {
        throw new Error('redis.createClient cannot be combined with redis.clients');
      }
      this.validateRedlockSettings(options.redis.settings);

      let clients: RedisClientLike[];
      if (suppliedClients) {
        if (suppliedClients.length === 0) {
          throw new Error('At least one Redis client is required when using redis.clients');
        }
        clients = [...suppliedClients];
      } else {
        if (options.redis.urls === undefined) {
          throw new Error('redis.urls or redis.clients is required when using Redis locking');
        }
        const urls = toArray(options.redis.urls);
        if (
          urls.length === 0 ||
          urls.some((url) => typeof url !== 'string' || url.trim().length === 0)
        ) {
          throw new Error('Redis URLs must be non-empty strings');
        }

        clients = urls.map((url) => {
          let client: RedisClientLike;
          if (options.redis?.createClient) {
            client = options.redis.createClient(url);
          } else {
            const IORedisModule = loadOptionalModule<{
              default?: new (...args: unknown[]) => RedisClientLike;
            }>('ioredis');
            if (!IORedisModule) {
              throw new Error(
                'Optional dependency "ioredis" is required to use the redis-based locking helpers'
              );
            }

            const IORedisCtor =
              IORedisModule.default ??
              ((IORedisModule as unknown) as new (...args: unknown[]) => RedisClientLike);
            client = new IORedisCtor(url);
          }
          // URL-based configuration delegates connection creation to this manager,
          // including clients created by a custom factory, so dispose owns them.
          this.createdRedisClients.push(client);
          return client;
        });
      }

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
        if (typeof client.quit === 'function') {
          try {
            await client.quit();
            return;
          } catch {
            // ignore and fall through to disconnect
          }
        }

        if (typeof client.disconnect === 'function') {
          client.disconnect();
        }
      })
    );
    this.createdRedisClients.length = 0;
  }

  private validateRedlockSettings(settings?: Partial<RedlockSettingsLike>): void {
    if (!settings) return;
    const finiteNonNegative = (value: unknown) =>
      typeof value === 'number' && Number.isFinite(value) && value >= 0;

    if (
      settings.driftFactor !== undefined &&
      (!finiteNonNegative(settings.driftFactor) || settings.driftFactor >= 1)
    ) {
      throw new Error('redis.settings.driftFactor must be finite and in [0, 1)');
    }
    if (
      settings.retryCount !== undefined &&
      (!Number.isSafeInteger(settings.retryCount) || settings.retryCount < -1)
    ) {
      throw new Error('redis.settings.retryCount must be an integer >= -1');
    }
    for (const [name, value] of [
      ['retryDelay', settings.retryDelay],
      ['retryJitter', settings.retryJitter],
      ['automaticExtensionThreshold', settings.automaticExtensionThreshold],
    ] as const) {
      if (value !== undefined && !finiteNonNegative(value)) {
        throw new Error(`redis.settings.${name} must be a finite non-negative number`);
      }
    }
  }

  private async ensureRedlock(): Promise<RedlockLike | undefined> {
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
    clients: RedisClientLike[],
    settings?: Partial<RedlockSettingsLike>
  ): Promise<RedlockLike> {
    const redlockModule = await importOptionalModule<{
      default?: new (
        clients: RedisClientLike[],
        settings?: Partial<RedlockSettingsLike>
      ) => RedlockLike;
    }>('redlock');
    if (!redlockModule) {
      throw new Error(
        'Optional dependency "redlock" is required to use the redis-based locking helpers'
      );
    }

    const RedlockCtor =
      redlockModule.default ??
      ((redlockModule as unknown) as new (
        clients: RedisClientLike[],
        settings?: Partial<RedlockSettingsLike>
      ) => RedlockLike);

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

  private getLeaseSafetyMarginMs(): number {
    return Math.max(5, Math.min(1_000, Math.floor(this.lockTTL * 0.05)));
  }

  private async acquireDistributedLock(
    id: string,
    waitIfLocked: boolean,
    abortSignal: AbortSignal,
    onContention?: () => Promise<void>
  ): Promise<DistributedLockHandle | undefined> {
    const redlock = await this.ensureRedlock();
    if (!redlock) {
      return { release: async () => { }, extensionFailed: false };
    }

    const resource = this.getDistributedResourceKey(id);

    const attempt = async (): Promise<DistributedLockHandle | undefined> => {
      try {
        // Always make one bounded Redlock attempt. Retrying here ourselves keeps
        // indefinite waits genuinely indefinite while still making AbortSignal and
        // timeout cancellation responsive between attempts.
        const acquisition = redlock.acquire([resource], this.lockTTL, { retryCount: 0 });
        let lock: RedlockLockLike;
        try {
          lock = await this.awaitWithAbort(acquisition, id, abortSignal);
        } catch (error) {
          if (error instanceof MutexTimeoutError || error instanceof MutexAbortedError) {
            // Redis may still finish an in-flight acquire after our caller has left.
            // If that happens, immediately release the late lease so it cannot
            // become a hidden lock that survives until TTL.
            acquisition.then(
              (lateLock) => lateLock.release().catch(() => undefined),
              () => undefined
            );
          }
          throw error;
        }
        const remainingLeaseMs = lock.expiration - Date.now();
        if (
          !Number.isFinite(remainingLeaseMs) ||
          remainingLeaseMs <= this.getLeaseSafetyMarginMs()
        ) {
          await lock.release().catch(() => undefined);
          throw new DistributedLockError(
            `Distributed mutex "${id}" was acquired with an invalid or unsafe lease deadline`
          );
        }
        let stopExtension: (() => void) | undefined;
        const handle: DistributedLockHandle = {
          release: async () => {
            stopExtension?.();
            await handle.currentLock!.release();
          },
          extensionFailed: false,
          currentLock: lock,
        };
        stopExtension = this.createLockExtension(id, handle);
        return handle;
      } catch (error) {
        if (
          error instanceof DistributedLockError ||
          error instanceof MutexTimeoutError ||
          error instanceof MutexAbortedError
        ) {
          throw error;
        }
        if (await this.isDistributedContention(error)) {
          return undefined;
        }
        throw new DistributedLockError(
          error instanceof Error ? error.message : 'Failed to acquire distributed lock',
          error
        );
      }
    };

    const firstAttempt = await attempt();
    if (firstAttempt || !waitIfLocked) {
      return firstAttempt;
    }

    let lastCoordinationRefresh = Date.now();
    if (onContention) {
      await onContention();
      lastCoordinationRefresh = Date.now();
    }

    let retryAttempt = 0;
    while (true) {
      await this.waitForDistributedRetry(redlock, id, abortSignal, retryAttempt);
      retryAttempt += 1;
      const acquired = await attempt();
      if (acquired) {
        return acquired;
      }

      if (
        onContention &&
        Date.now() - lastCoordinationRefresh >= this.coordinationRefreshInterval
      ) {
        await onContention();
        lastCoordinationRefresh = Date.now();
      }
    }
  }

  private async isDistributedContention(error: unknown): Promise<boolean> {
    if (isResourceLockedError(error)) {
      return true;
    }
    if (!isExecutionError(error)) {
      return false;
    }

    const attempts = error.attempts;
    if (!Array.isArray(attempts) || attempts.length === 0) {
      return false;
    }

    try {
      const stats = (await attempts[attempts.length - 1]) as {
        quorumSize?: number;
        votesAgainst?: Map<unknown, unknown>;
      };
      if (!stats?.quorumSize || !(stats.votesAgainst instanceof Map)) {
        return false;
      }

      let lockedVotes = 0;
      for (const vote of stats.votesAgainst.values()) {
        if (isResourceLockedError(vote)) {
          lockedVotes += 1;
        }
      }
      return lockedVotes >= stats.quorumSize;
    } catch {
      return false;
    }
  }

  private async waitForDistributedRetry(
    redlock: RedlockLike,
    id: string,
    abortSignal: AbortSignal,
    retryAttempt: number
  ): Promise<void> {
    if (abortSignal.aborted) {
      this.throwDistributedWaitAbort(id, abortSignal.reason);
    }

    const settings = redlock.settings;
    const configuredRetryDelay = settings?.retryDelay;
    const configuredRetryJitter = settings?.retryJitter;
    const retryDelay = typeof configuredRetryDelay === 'number' && Number.isFinite(configuredRetryDelay)
      ? Math.max(0, configuredRetryDelay)
      : 200;
    const retryJitter = typeof configuredRetryJitter === 'number' && Number.isFinite(configuredRetryJitter)
      ? Math.max(0, configuredRetryJitter)
      : 100;
    // Redlock's default ~200ms retry is conservative but makes short critical
    // sections feel needlessly sticky. Start quickly, then exponentially back
    // off to the configured delay so sustained contention does not hot-loop.
    const initialDelay = Math.min(10, retryDelay);
    const backoffCap = retryDelay === 0
      ? 0
      : Math.min(retryDelay, initialDelay * 2 ** Math.min(retryAttempt, 10));
    const jitterWindow = Math.min(retryJitter, Math.ceil(backoffCap / 2));
    const jitter = jitterWindow > 0 ? Math.floor(Math.random() * (jitterWindow + 1)) : 0;
    const delay = Math.max(0, backoffCap - jitter);

    await new Promise<void>((resolve, reject) => {
      let settled = false;
      const finish = (error?: Error) => {
        if (settled) {
          return;
        }
        settled = true;
        clearTimeout(timer);
        abortSignal.removeEventListener('abort', onAbort);
        if (error) {
          reject(error);
        } else {
          resolve();
        }
      };
      const onAbort = () => {
        try {
          this.throwDistributedWaitAbort(id, abortSignal.reason);
        } catch (error) {
          finish(error as Error);
        }
      };
      const timer = setTimeout(() => finish(), delay);
      timer.unref?.();
      abortSignal.addEventListener('abort', onAbort, { once: true });
      if (abortSignal.aborted) {
        onAbort();
      }
    });
  }

  private throwDistributedWaitAbort(id: string, reason: unknown): never {
    if (reason instanceof MutexTimeoutError) {
      throw reason;
    }
    throw new MutexAbortedError(id, reason);
  }

  private async awaitWithAbort<T>(
    promise: Promise<T>,
    id: string,
    abortSignal: AbortSignal
  ): Promise<T> {
    if (abortSignal.aborted) {
      this.throwDistributedWaitAbort(id, abortSignal.reason);
    }

    return new Promise<T>((resolve, reject) => {
      let settled = false;
      const finish = (callback: () => void) => {
        if (settled) return;
        settled = true;
        abortSignal.removeEventListener('abort', onAbort);
        callback();
      };
      const onAbort = () => {
        try {
          this.throwDistributedWaitAbort(id, abortSignal.reason);
        } catch (error) {
          finish(() => reject(error));
        }
      };
      abortSignal.addEventListener('abort', onAbort, { once: true });
      promise.then(
        (value) => finish(() => resolve(value)),
        (error) => finish(() => reject(error))
      );
      if (abortSignal.aborted) onAbort();
    });
  }

  private async extendBeforeSafeDeadline(
    redlock: RedlockLike,
    currentLock: RedlockLockLike,
    id: string
  ): Promise<RedlockLockLike> {
    const safetyMarginMs = this.getLeaseSafetyMarginMs();
    const deadlineDelayMs = currentLock.expiration - Date.now() - safetyMarginMs;
    if (!Number.isFinite(deadlineDelayMs) || deadlineDelayMs <= 0) {
      throw new LockExtensionError(
        id,
        `Lock for mutex "${id}" reached its safe renewal deadline before extension began.`
      );
    }

    const extension = redlock.extend(currentLock, this.lockTTL, { retryCount: 0 });
    let deadlineExceeded = false;
    let deadlineTimer: ReturnType<typeof setTimeout> | undefined;

    // If Redis completes an extension only after we have already declared the
    // old lease unsafe, release that late replacement immediately. Otherwise a
    // timed-out renewal could silently resurrect ownership behind the caller.
    extension.then(
      (lateLock) => {
        if (deadlineExceeded) {
          lateLock.release().catch(() => undefined);
        }
      },
      () => undefined
    );

    const deadline = new Promise<never>((_, reject) => {
      deadlineTimer = setTimeout(() => {
        deadlineExceeded = true;
        reject(new LockExtensionError(
          id,
          `Lock extension for mutex "${id}" did not complete before the safe lease deadline.`
        ));
      }, deadlineDelayMs);
      deadlineTimer.unref?.();
    });

    try {
      return await Promise.race([extension, deadline]);
    } finally {
      if (deadlineTimer) {
        clearTimeout(deadlineTimer);
      }
    }
  }

  private createLockExtension(
    id: string,
    handle: DistributedLockHandle
  ): (() => void) | undefined {
    const redlock = this.redlock;
    if (!redlock || this.lockExtendInterval <= 0) {
      return undefined;
    }

    let consecutiveFailures = 0;
    let stopped = false;
    let timer: ReturnType<typeof setTimeout> | undefined;

    const fail = (error: Error) => {
      handle.extensionFailed = true;
      handle.extensionError = error;
      if (handle.onExtensionFailure) {
        try {
          handle.onExtensionFailure(error);
        } catch {
          // Ignore consumer callback failures.
        }
      }
      stopped = true;
      if (timer) {
        clearTimeout(timer);
        timer = undefined;
      }
    };

    const schedule = (requestedDelay = this.lockExtendInterval) => {
      if (stopped || !handle.currentLock) {
        return;
      }
      const remainingMs = handle.currentLock.expiration - Date.now();
      const safetyMarginMs = this.getLeaseSafetyMarginMs();
      if (!Number.isFinite(remainingMs) || remainingMs <= safetyMarginMs) {
        fail(new LockExtensionError(
          id,
          `Lock for mutex "${id}" is too close to expiration to renew safely.`
        ));
        return;
      }
      const delay = Math.max(1, Math.min(requestedDelay, remainingMs - safetyMarginMs));
      timer = setTimeout(runExtension, delay);
      timer.unref?.();
    };

    const runExtension = async () => {
      if (stopped || !handle.currentLock) {
        return;
      }

      const currentLock = handle.currentLock;
      if (Date.now() >= currentLock.expiration) {
        fail(new LockExtensionError(
          id,
          `Lock for mutex "${id}" has expired. Mutual exclusion can no longer be guaranteed.`
        ));
        return;
      }

      try {
        // A lease extension must never inherit Redlock's generic multi-second
        // retry window: the old lease has a hard expiration deadline. Retry
        // single bounded attempts ourselves while there is still lease time.
        const extendedLock = await this.extendBeforeSafeDeadline(redlock, currentLock, id);
        if (stopped) {
          await extendedLock.release().catch(() => undefined);
          return;
        }
        handle.currentLock = extendedLock;
        const extendedRemainingMs = extendedLock.expiration - Date.now();
        if (
          !Number.isFinite(extendedRemainingMs) ||
          extendedRemainingMs <= this.getLeaseSafetyMarginMs()
        ) {
          fail(new LockExtensionError(
            id,
            `Lock extension for mutex "${id}" completed too close to expiration.`
          ));
          return;
        }
        consecutiveFailures = 0;
        schedule();
      } catch (error) {
        if (stopped) {
          return;
        }
        if (error instanceof LockExtensionError) {
          fail(error);
          return;
        }
        consecutiveFailures += 1;
        const definitelyLost = await this.isDistributedContention(error);
        const remainingMs = currentLock.expiration - Date.now();
        const safetyMarginMs = this.getLeaseSafetyMarginMs();
        if (definitelyLost || remainingMs <= safetyMarginMs) {
          fail(new LockExtensionError(
            id,
            definitelyLost
              ? `Lock for mutex "${id}" is no longer owned by this operation.`
              : [
                  `Lock extension failed ${consecutiveFailures} time(s) for mutex "${id}"`,
                  `before the safe renewal deadline: ${error instanceof Error ? error.message : 'unknown error'}`,
                ].join(' ')
          ));
          return;
        }

        const configuredRetryDelay = redlock.settings?.retryDelay;
        const extensionRetryDelay =
          typeof configuredRetryDelay === 'number' && Number.isFinite(configuredRetryDelay)
            ? Math.max(0, configuredRetryDelay)
            : 100;
        const baseRetryDelay = Math.max(
          1,
          Math.min(this.lockExtendInterval, extensionRetryDelay)
        );
        const retryBudgetMs = remainingMs - safetyMarginMs;
        schedule(Math.max(1, Math.min(baseRetryDelay, Math.floor(retryBudgetMs / 2))));
      }
    };

    schedule();
    return () => {
      stopped = true;
      if (timer) {
        clearTimeout(timer);
        timer = undefined;
      }
    };
  }

  private assertDistributedLock(
    id: string,
    handle?: DistributedLockHandle
  ): void {
    if (!handle) {
      return;
    }
    if (!handle.extensionFailed && !handle.currentLock) {
      return;
    }
    if (!handle.extensionFailed && handle.currentLock) {
      if (Date.now() < handle.currentLock.expiration) {
        return;
      }
      throw new LockExtensionError(
        id,
        `Lock for mutex "${id}" has expired; mutual exclusion has been lost.`
      );
    }
    throw handle.extensionError ?? new LockExtensionError(
      id,
      `Lock extension failed during operation for mutex "${id}"; mutual exclusion has been lost.`
    );
  }

  private allocateContext(): OperationContext {
    const context: OperationContext = {
      id: `${this.instanceId}:${++this.contextCounter}`,
      held: new Map(),
      heldCounter: 0,
      waits: new Map(),
      waitCounter: 0,
      publishedOwners: new Map(),
      coordinationTail: Promise.resolve(),
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

  runOperation<T>(
    id: string,
    operation: OperationType<T>,
    opts: MutexNoWaitOptions
  ): Promise<T | undefined>;
  runOperation<T>(
    id: string,
    operation: OperationType<T>,
    opts?: MutexWaitOptions
  ): Promise<T>;
  runOperation<T>(
    id: string,
    operation: OperationType<T>,
    opts: MutexRunOptions
  ): Promise<T | undefined>;
  async runOperation<T>(
    id: string,
    operation: OperationType<T>,
    opts?: MutexRunOptions
  ): Promise<T | undefined> {
    // Input validation - fail fast for client misuse
    if (!id || typeof id !== 'string' || id.trim().length === 0) {
      throw new Error(`Invalid mutex ID: "${id}". ID must be a non-empty string.`);
    }
    if (typeof operation !== 'function') {
      throw new Error(`Invalid operation callback: expected function, got ${typeof operation}.`);
    }
    if (opts?.timeoutMs !== undefined) {
      if (typeof opts.timeoutMs !== 'number' || !Number.isFinite(opts.timeoutMs)) {
        throw new Error('timeoutMs must be a finite number of milliseconds');
      }
      if (opts.timeoutMs > MAX_TIMER_DELAY_MS) {
        throw new Error(`timeoutMs must not exceed ${MAX_TIMER_DELAY_MS}ms`);
      }
    }

    await this.contextReady;

    const waitPreference = opts?.waitIfLocked ?? true;
    const timeoutMs = waitPreference ? opts?.timeoutMs : undefined;
    const requestedContext = this.getContextFromToken(opts?.context);
    const existingContext = this.context.getStore();
    const context = requestedContext ?? existingContext ?? this.allocateContext();
    this.ensureContextToken(context);

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

    const run = async () => {
      try {
        return await this.runWithContext(
          id,
          operation,
          waitPreference,
          timeoutMs,
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
  }

  private async runWithContext<T>(
    id: string,
    operation: OperationType<T>,
    waitPreference: boolean,
    timeoutMs: number | undefined,
    context: OperationContext,
    abortSignal: AbortSignal,
    abortFn: (reason?: unknown) => void,
    abortable: boolean,
    registerAbortCallback: (callback: (reason?: unknown) => void) => () => void
  ): Promise<T | undefined> {
    const requestTime = Date.now();
    const lockState = this.getOrCreateLockState(id);
    const mutex = new Mutex(id, lockState);
    const contextToken = this.ensureContextToken(context);
    const timeoutError =
      waitPreference && timeoutMs !== undefined ? new MutexTimeoutError(id, timeoutMs) : undefined;
    const canWait = waitPreference && (timeoutMs === undefined || timeoutMs > 0);
    let timedOut = false;
    let timeoutHandle: ReturnType<typeof setTimeout> | undefined;
    let aborted = abortSignal.aborted;
    let waitingState: WaitingState | undefined;
    const abortListeners: Array<() => void> = [];

    if (abortSignal.aborted) {
      throw new MutexAbortedError(id, abortSignal.reason);
    }

    const clearTimer = () => {
      if (timeoutHandle) {
        clearTimeout(timeoutHandle);
        timeoutHandle = undefined;
      }
    };

    const clearCurrentWaitingState = (): Promise<void> | void => {
      const current = waitingState;
      if (!current) return undefined;
      waitingState = undefined;
      context.waits.delete(current.token);
      return this.publishWaitingState(context);
    };

    const markTimedOut = () => {
      if (timedOut) {
        return;
      }
      timedOut = true;
      clearTimer();
      const reset = clearCurrentWaitingState();
      if (reset) reset.catch(() => undefined);
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
      const reset = clearCurrentWaitingState();
      if (reset) reset.catch(() => undefined);
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
        const inheritedDistributedLock = context.held.get(id)?.distributedLock;
        try {
          const result = await operation({
            requestTime,
            startTime,
            currentMutex: mutex,
            heldMutexIds: this.getHeldMutexIds(context),
            contextToken,
            abortSignal,
            assertLock: () => this.assertDistributedLock(id, inheritedDistributedLock),
          });
          this.assertDistributedLock(id, inheritedDistributedLock);
          if (aborted || abortSignal.aborted) {
            markAborted();
            throw new MutexAbortedError(id, abortSignal.reason);
          }
          return result;
        } finally {
          const releaseResult = this.decrementHeldLock(context, id);
          if (releaseResult.removed && context.waits.size > 0) {
            const refresh = this.publishWaitingState(context);
            if (refresh) refresh.catch(() => undefined);
          }
          if (releaseResult.handle) {
            await releaseResult.handle.release().catch(() => undefined);
          }
        }
      }

      const hasTimeoutWrapper = Boolean(
        timeoutError && canWait && timeoutMs !== undefined && timeoutMs > 0
      );
      let timeoutPromise: Promise<never> | undefined;
      if (hasTimeoutWrapper && timeoutError && timeoutMs !== undefined) {
        timeoutPromise = new Promise<never>((_, reject) => {
          const elapsedMs = Math.max(0, Date.now() - requestTime);
          const remainingMs = Math.max(0, timeoutMs - elapsedMs);
          timeoutHandle = setTimeout(() => {
            markTimedOut();
            reject(timeoutError);
          }, remainingMs);
          timeoutHandle.unref?.();
        });
        // The function may still be setting up deadlock metadata when this fires.
        // Attach a handler immediately so the early deadline cannot become an
        // unhandled rejection before the Promise.race below is installed.
        timeoutPromise.catch(() => undefined);
      }

      const alreadyLocked = lockState.pending > 0;
      if (!canWait && alreadyLocked) {
        if (timeoutError) {
          timedOut = true;
          throw timeoutError;
        }
        return undefined;
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

      let runActual: (() => Promise<T | undefined>) | undefined;

      const runPromise = previousTail.then(async () => {
        await ready;
        return runActual!();
      });

      lockState.tail = runPromise
        .then(() => undefined)
        .catch(() => undefined);

      const markWaitingAndCheckDeadlock = async () => {
        // A branch that owns no mutex cannot participate in a wait-for cycle.
        if (context.held.size === 0) return;

        if (!waitingState) {
          const heldLocks = new Map<string, string>();
          for (const [heldId, held] of context.held) {
            heldLocks.set(heldId, held.ownershipToken);
          }
          waitingState = {
            token: `${context.id}:wait:${++context.waitCounter}`,
            id,
            heldLocks,
          };
          context.waits.set(waitingState.token, waitingState);
        }

        const currentWait = waitingState;
        const waitingUpdate = this.publishWaitingState(context);
        try {
          if (waitingUpdate) {
            await this.awaitWithAbort(waitingUpdate, id, abortSignal);
          }
        } catch (error) {
          const reset = clearCurrentWaitingState();
          if (reset) reset.catch(() => undefined);
          throw error;
        }

        // Timeout/abort may have removed this wait while Redis metadata was publishing.
        if (!waitingState || waitingState.token !== currentWait.token || !context.waits.has(currentWait.token)) return;

        const deadlockCycle = this.coordinationClient
          ? await this.awaitWithAbort(
              this.detectRemoteDeadlock(context, currentWait),
              id,
              abortSignal
            )
          : this.detectLocalDeadlock(context, currentWait);
        if (deadlockCycle) {
          const reset = clearCurrentWaitingState();
          if (reset) reset.catch(() => undefined);
          throw new MutexDeadlockError(id, deadlockCycle);
        }
      };

      try {
        // Local contention is known before this operation reaches the queue head.
        // Remote contention is discovered by the first bounded Redlock attempt below.
        if (canWait && alreadyLocked) {
          await markWaitingAndCheckDeadlock();
        }

        runActual = async () => {
          let distributedLock: DistributedLockHandle | undefined;
          let operationStarted = false;

          try {
            if (timeoutError && timedOut) {
              throw timeoutError;
            }

            if (aborted || abortSignal.aborted) {
              markAborted();
              throw new MutexAbortedError(id, abortSignal.reason);
            }

            distributedLock = await this.acquireDistributedLock(
              id,
              canWait,
              abortSignal,
              canWait ? markWaitingAndCheckDeadlock : undefined
            );
            if (!distributedLock) {
              if (timeoutError) {
                throw timeoutError;
              }
              return undefined;
            }

            distributedLock.onExtensionFailure = (error) => {
              abortFn(error);
            };
            if (distributedLock.extensionFailed && distributedLock.extensionError) {
              abortFn(distributedLock.extensionError);
            }
            this.assertDistributedLock(id, distributedLock);

            if (timeoutError && timedOut) {
              throw timeoutError;
            }

            if (aborted || abortSignal.aborted) {
              markAborted();
              throw new MutexAbortedError(id, abortSignal.reason);
            }

            if (waitingState) {
              const cleared = clearCurrentWaitingState();
              if (cleared) {
                await this.awaitWithAbort(cleared, id, abortSignal);
              }
            }

            this.assertDistributedLock(id, distributedLock);
            clearTimer();
            this.incrementHeldLock(context, id, distributedLock);
            operationStarted = true;

            const startTime = Date.now();
            const result = await operation({
              requestTime,
              startTime,
              currentMutex: mutex,
              heldMutexIds: this.getHeldMutexIds(context),
              contextToken,
              abortSignal,
              assertLock: () => this.assertDistributedLock(id, distributedLock),
            });

            this.assertDistributedLock(id, distributedLock);

            if (aborted || abortSignal.aborted) {
              markAborted();
              throw new MutexAbortedError(id, abortSignal.reason);
            }
            return result;
          } finally {
            if (waitingState) {
              const reset = clearCurrentWaitingState();
              if (reset) reset.catch(() => undefined);
            }

            if (operationStarted) {
              const releaseResult = this.decrementHeldLock(context, id);
              if (releaseResult.removed && context.waits.size > 0) {
                const refresh = this.publishWaitingState(context);
                if (refresh) refresh.catch(() => undefined);
              }
              if (releaseResult.handle) {
                await releaseResult.handle.release().catch(() => undefined);
              } else if (distributedLock && releaseResult.removed) {
                await distributedLock.release().catch(() => undefined);
              }
            } else if (distributedLock) {
              // Acquisition succeeded but setup failed before user code started.
              await distributedLock.release().catch(() => undefined);
            }

            lockState.pending -= 1;
            if (lockState.pending === 0) {
              this.locks.delete(id);
            }
          }
        };

        resolveReady?.();
        resolveReady = undefined;
        rejectReady = undefined;
      } catch (error) {
        clearTimer();
        lockState.pending -= 1;
        if (lockState.pending === 0) {
          this.locks.delete(id);
        }
        rejectReady?.(error);
        resolveReady = undefined;
        rejectReady = undefined;
        throw error;
      }

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

      const races: Array<Promise<T | undefined>> = [runPromise];

      if (timeoutPromise) {
        races.push(timeoutPromise);
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

    context.held.set(id, {
      count: 1,
      ownershipToken: `${context.id}:held:${++context.heldCounter}`,
      distributedLock,
    });
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

  private getDistributedResourceKey(id: string): string {
    // Preserve the legacy key shape when no namespace is supplied so existing
    // distributed deployments can upgrade without splitting one lock domain.
    return this.namespace
      ? `${this.redisPrefix}:lock:${encodeURIComponent(id)}`
      : `${this.redisPrefix}:${id}`;
  }

  private getContextKey(contextId: string): string {
    return `${this.metadataPrefix}:context:${encodeURIComponent(contextId)}`;
  }

  private getOwnerKey(id: string): string {
    return `${this.metadataPrefix}:owner:${encodeURIComponent(id)}`;
  }

  private getLegacyContextKey(contextId: string): string | undefined {
    return this.namespace ? undefined : `${UniqueMutexManager.REDIS_PREFIX}:context:${contextId}`;
  }

  private getLegacyOwnerKey(id: string): string | undefined {
    return this.namespace ? undefined : `${UniqueMutexManager.REDIS_PREFIX}:owner:${id}`;
  }

  /** Publish only dependencies tied to the exact lock generations held when each wait began. */
  private publishWaitingState(context: OperationContext): Promise<void> | void {
    if (!this.coordinationClient) return undefined;

    const publication = context.coordinationTail.then(async () => {
      const serializedWaits: SerializedWaitingState[] = [];
      const desiredOwners = new Map<string, PublishedOwnerState>();
      for (const wait of context.waits.values()) {
        const heldLocks: Record<string, string> = {};
        for (const [heldId, capturedToken] of wait.heldLocks) {
          const current = context.held.get(heldId);
          if (!current || current.ownershipToken !== capturedToken) continue;
          heldLocks[heldId] = capturedToken;
          desiredOwners.set(heldId, {
            value: JSON.stringify({ contextId: context.id, ownershipToken: capturedToken }),
          });
        }
        if (Object.keys(heldLocks).length > 0) {
          serializedWaits.push({ id: wait.id, heldLocks });
        }
      }

      const client = this.coordinationClient!;
      const operations: Array<Promise<unknown>> = [];
      const contextKey = this.getContextKey(context.id);
      if (serializedWaits.length > 0) {
        operations.push(
          client.hset(
            contextKey,
            'instanceId',
            this.instanceId,
            UniqueMutexManager.WAITS_V2_FIELD,
            JSON.stringify(serializedWaits)
          ),
          client.pexpire(contextKey, this.metadataTTL)
        );
        for (const [heldId, owner] of desiredOwners) {
          operations.push(
            client.set(this.getOwnerKey(heldId), owner.value, 'PX', this.metadataTTL)
          );
        }
      } else {
        operations.push(client.del(contextKey));
      }

      for (const [heldId, published] of context.publishedOwners) {
        const desired = desiredOwners.get(heldId);
        if (desired?.value === published.value) continue;
        operations.push(
          client.eval(
            UniqueMutexManager.DELETE_OWNER_IF_MATCHES_SCRIPT,
            1,
            this.getOwnerKey(heldId),
            published.value
          )
        );
      }

      await Promise.all(operations);
      context.publishedOwners = desiredOwners;
    });
    const result = publication.catch((error) => {
      if (this.coordinationFailureMode === 'best-effort') return;
      throw error instanceof DistributedLockError
        ? error
        : new DistributedLockError('Failed to publish distributed deadlock metadata', error);
    });
    context.coordinationTail = result.catch(() => undefined);
    return result;
  }

  private async getRemoteOwner(
    id: string
  ): Promise<{ contextId: string; ownershipToken?: string } | undefined> {
    if (!this.coordinationClient) return undefined;
    try {
      const v2Owner = await this.coordinationClient.get(this.getOwnerKey(id));
      if (v2Owner) {
        try {
          const parsed = JSON.parse(v2Owner) as {
            contextId?: unknown;
            ownershipToken?: unknown;
          };
          if (
            typeof parsed.contextId === 'string' &&
            typeof parsed.ownershipToken === 'string'
          ) {
            return {
              contextId: parsed.contextId,
              ownershipToken: parsed.ownershipToken,
            };
          }
        } catch {
          // Ignore malformed v2 metadata and try rolling-upgrade fallback below.
        }
      }

      const legacyOwnerKey = this.getLegacyOwnerKey(id);
      if (!legacyOwnerKey) return undefined;
      const legacyOwner = await this.coordinationClient.get(legacyOwnerKey);
      return legacyOwner ? { contextId: legacyOwner } : undefined;
    } catch (error) {
      if (this.coordinationFailureMode === 'best-effort') return undefined;
      throw error instanceof DistributedLockError
        ? error
        : new DistributedLockError('Failed to read distributed lock ownership metadata', error);
    }
  }

  private async getRemoteWaits(
    contextId: string,
    heldId: string,
    ownershipToken?: string
  ): Promise<string[]> {
    if (!this.coordinationClient) return [];
    try {
      const waitsV2 = await this.coordinationClient.hget(
        this.getContextKey(contextId),
        UniqueMutexManager.WAITS_V2_FIELD
      );
      if (waitsV2) {
        try {
          const parsed = JSON.parse(waitsV2) as unknown;
          if (Array.isArray(parsed)) {
            const result: string[] = [];
            for (const value of parsed) {
              if (!value || typeof value !== 'object') continue;
              const wait = value as { id?: unknown; heldLocks?: unknown };
              if (
                typeof wait.id !== 'string' ||
                !wait.heldLocks ||
                typeof wait.heldLocks !== 'object'
              ) {
                continue;
              }
              const captured = (wait.heldLocks as Record<string, unknown>)[heldId];
              if (
                typeof captured === 'string' &&
                (ownershipToken === undefined || captured === ownershipToken)
              ) {
                result.push(wait.id);
              }
            }
            return result;
          }
        } catch {
          // Ignore malformed v2 metadata and try rolling-upgrade fallback below.
        }
      }

      const legacyContextKey = this.getLegacyContextKey(contextId);
      if (!legacyContextKey) return [];
      const legacyWaitingFor = await this.coordinationClient.hget(
        legacyContextKey,
        'waitingFor'
      );
      return legacyWaitingFor ? [legacyWaitingFor] : [];
    } catch (error) {
      if (this.coordinationFailureMode === 'best-effort') return [];
      throw error instanceof DistributedLockError
        ? error
        : new DistributedLockError('Failed to read distributed wait metadata', error);
    }
  }

  private detectLocalDeadlock(
    context: OperationContext,
    targetWait: WaitingState
  ): string[] | undefined {
    const visitedIds = new Set<string>();
    const visitedOwnerEdges = new Set<string>();
    type StackNode =
      | { type: 'id'; id: string; path: string[] }
      | { type: 'context'; context: OperationContext; heldId: string; ownershipToken: string; path: string[] };
    const stack: StackNode[] = [{ type: 'id', id: targetWait.id, path: [targetWait.id] }];

    while (stack.length > 0) {
      const node = stack.pop()!;
      if (node.type === 'id') {
        if (visitedIds.has(node.id)) continue;
        visitedIds.add(node.id);
        const owner = this.lockOwners.get(node.id);
        const heldState = owner?.held.get(node.id);
        if (owner && heldState) {
          stack.push({
            type: 'context', context: owner, heldId: node.id,
            ownershipToken: heldState.ownershipToken, path: node.path,
          });
        }
        continue;
      }

      const edgeKey = `${node.context.id}\0${node.heldId}\0${node.ownershipToken}`;
      if (visitedOwnerEdges.has(edgeKey)) continue;
      visitedOwnerEdges.add(edgeKey);
      for (const wait of node.context.waits.values()) {
        if (wait.heldLocks.get(node.heldId) !== node.ownershipToken) continue;
        if (node.context === context && wait.token === targetWait.token) {
          return [...node.path, targetWait.id];
        }
        stack.push({ type: 'id', id: wait.id, path: [...node.path, wait.id] });
      }
    }
    return undefined;
  }

  private async detectRemoteDeadlock(
    context: OperationContext,
    targetWait: WaitingState
  ): Promise<string[] | undefined> {
    const visitedIds = new Set<string>();
    const visitedLocalOwnerEdges = new Set<string>();
    const visitedRemoteOwnerEdges = new Set<string>();
    type StackNode =
      | { type: 'id'; id: string; path: string[] }
      | { type: 'context'; context: OperationContext; heldId: string; ownershipToken: string; path: string[] }
      | { type: 'remote-context'; contextId: string; heldId: string; ownershipToken?: string; path: string[] };
    const stack: StackNode[] = [{ type: 'id', id: targetWait.id, path: [targetWait.id] }];

    while (stack.length > 0) {
      const node = stack.pop()!;
      if (node.type === 'id') {
        if (visitedIds.has(node.id)) continue;
        visitedIds.add(node.id);
        const localOwner = this.lockOwners.get(node.id);
        const localHeldState = localOwner?.held.get(node.id);
        if (localOwner && localHeldState) {
          stack.push({
            type: 'context', context: localOwner, heldId: node.id,
            ownershipToken: localHeldState.ownershipToken, path: node.path,
          });
          continue;
        }
        const remoteOwner = await this.getRemoteOwner(node.id);
        if (remoteOwner) {
          if (remoteOwner.contextId === context.id) continue;
          stack.push({
            type: 'remote-context', contextId: remoteOwner.contextId, heldId: node.id,
            ownershipToken: remoteOwner.ownershipToken, path: node.path,
          });
        }
        continue;
      }

      if (node.type === 'remote-context') {
        const edgeKey = `${node.contextId}\0${node.heldId}\0${node.ownershipToken ?? ''}`;
        if (visitedRemoteOwnerEdges.has(edgeKey)) continue;
        visitedRemoteOwnerEdges.add(edgeKey);
        const waits = await this.getRemoteWaits(node.contextId, node.heldId, node.ownershipToken);
        for (const waitingFor of waits) {
          stack.push({ type: 'id', id: waitingFor, path: [...node.path, waitingFor] });
        }
        continue;
      }

      const edgeKey = `${node.context.id}\0${node.heldId}\0${node.ownershipToken}`;
      if (visitedLocalOwnerEdges.has(edgeKey)) continue;
      visitedLocalOwnerEdges.add(edgeKey);
      for (const wait of node.context.waits.values()) {
        if (wait.heldLocks.get(node.heldId) !== node.ownershipToken) continue;
        if (node.context === context && wait.token === targetWait.token) {
          return [...node.path, targetWait.id];
        }
        stack.push({ type: 'id', id: wait.id, path: [...node.path, wait.id] });
      }
    }
    return undefined;
  }

}
