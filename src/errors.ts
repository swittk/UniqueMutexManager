/**
 * @deprecated `waitIfLocked: false` now resolves to `undefined` on contention.
 * Retained for source compatibility with 0.1.x consumers.
 */
export class MutexLockedError extends Error {
  public readonly id: string;

  constructor(id: string, message?: string) {
    super(message ?? `Mutex with id "${id}" is already locked`);
    this.name = 'MutexLockedError';
    this.id = id;
  }
}

export class MutexTimeoutError extends Error {
  public readonly id: string;
  public readonly timeoutMs: number;

  constructor(id: string, timeoutMs: number) {
    super(`Timed out waiting ${timeoutMs}ms to acquire mutex "${id}"`);
    this.name = 'MutexTimeoutError';
    this.id = id;
    this.timeoutMs = timeoutMs;
  }
}

export class MutexAbortedError extends Error {
  public readonly id: string;
  public readonly reason: unknown;

  constructor(id: string, reason: unknown) {
    super(`Aborted while waiting to acquire mutex "${id}"`);
    this.name = 'MutexAbortedError';
    this.id = id;
    this.reason = reason;
  }
}

export class DistributedLockError extends Error {
  public readonly cause: unknown;

  constructor(message: string, cause?: unknown) {
    super(message);
    this.name = 'DistributedLockError';
    this.cause = cause;
  }
}

export class LockExtensionError extends Error {
  public readonly id: string;

  constructor(id: string, message?: string) {
    super(message ?? `Lock extension failed for mutex "${id}" - lock may have expired`);
    this.name = 'LockExtensionError';
    this.id = id;
  }
}

export class MutexDeadlockError extends Error {
  public readonly requestedId: string;
  public readonly cycle: string[];

  constructor(requestedId: string, cycle: string[]) {
    super(
      `Deadlock detected while attempting to acquire mutex "${requestedId}". Cycle: ${cycle.join(
        ' -> '
      )}`
    );
    this.name = 'MutexDeadlockError';
    this.requestedId = requestedId;
    this.cycle = cycle;
  }
}
