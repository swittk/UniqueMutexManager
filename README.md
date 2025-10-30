# UniqueMutexManager

A TypeScript-first mutex manager with optional Redis-backed distributed locking. Use it to serialize asynchronous operations in a single Node.js process, inside browser or edge runtimes, or across multiple processes that share a Redis deployment.

## Installation

```bash
npm install uniquemutexmanager
# Optional peer dependencies if you want distributed locking
npm install ioredis redlock
```

The package publishes CommonJS and modern ES module builds along with bundled type declarations so it can run in Node.js,
browser bundlers, workers, and other JavaScript runtimes. The compiled output targets ES2015 (ES6) to remain compatible with
modern browsers while still supporting async/await semantics. Install the optional peer dependencies only when you plan to
enable Redis-backed coordination.

## Usage

```ts
import { UniqueMutexManager } from 'uniquemutexmanager';

const manager = new UniqueMutexManager();

await manager.runOperation('user:123', async ({
  requestTime,
  startTime,
  currentMutex,
  heldMutexIds,
  contextToken,
  abortSignal,
}) => {
  console.log('Queue size', currentMutex.queueSize);
  console.log('Currently held locks', heldMutexIds);
  console.log('Context token', contextToken.id);
  console.log('Aborted?', abortSignal.aborted);
  // perform your work here
});
```

### Skipping or timing out when locked

Operations wait indefinitely by default. If you prefer to fail fast without joining the queue:

```ts
await manager.runOperation('user:123', doWork, { waitIfLocked: false });
```

A `MutexLockedError` will be thrown when the mutex is already occupied.

To bound how long a caller waits before giving up, provide `timeoutMs` (in milliseconds). When the timeout expires a
`MutexTimeoutError` is raised and the queued operation is cancelled before it executes:

```ts
await manager.runOperation('user:123', doWork, { timeoutMs: 250 });
```

Passing `timeoutMs: 0` (or any negative value) performs an immediate attempt that surfaces a `MutexTimeoutError` instead of
`MutexLockedError` when the lock cannot be acquired right away.

### Cancelling queued operations

Provide an `AbortSignal` to stop waiting for a mutex or to propagate cancellation into the running callback. When the signal
fires before the lock is acquired, the queued operation is removed from the wait list and the returned promise rejects with
`MutexAbortedError`:

```ts
const controller = new AbortController();

const queued = manager.runOperation('user:123', async ({ abortSignal }) => {
  abortSignal.throwIfAborted?.();
  return doWork();
}, { signal: controller.signal });

controller.abort(new Error('no longer needed'));

await queued; // rejects with MutexAbortedError
```

When you need to cancel a pending call from outside the scope of an `AbortController`, use the `onAbort` hook to capture a
callback that aborts the queued work:

```ts
let cancel: (() => void) | undefined;

const queued = manager.runOperation('user:123', doWork, {
  onAbort: (abort) => {
    cancel = abort;
  },
});

cancel?.(); // Rejects the queued operation with MutexAbortedError
```

### Distributed locking with Redis

Provide a Redlock instance to coordinate across processes:

```ts
import { UniqueMutexManager } from 'uniquemutexmanager';
import Redis from 'ioredis';
import Redlock from 'redlock';

const clients = [new Redis(process.env.REDIS_URL!)];
const redlock = new Redlock(clients);

const manager = new UniqueMutexManager({
  redlock,
  lockTTL: 60000,
  coordinationClient: clients[0],
});
```

Alternatively let the manager create a Redlock instance by specifying Redis URLs (requires `ioredis` and `redlock` to be installed):

```ts
const manager = new UniqueMutexManager({
  redis: {
    urls: process.env.REDIS_URL!,
  },
});
```

Call `dispose()` when you are done to close any Redis clients that were created internally:

```ts
await manager.dispose();
```

When Redis-backed locking is enabled, the manager also shares lock ownership metadata so that deadlock detection continues to
work across process boundaries. If you supply your own Redlock instance, pass a Redis client via `coordinationClient` so that
this metadata can be published. The `timeoutMs` option applies across distributed acquisitions as well; a waiting process throws
`MutexTimeoutError` when its local timeout expires even if the mutex is still held by another instance.

## API

### `runOperation(id, operation, options?)`

Queues `operation` under the specified `id`. The provided callback receives:

- `requestTime`: the timestamp when the operation was enqueued.
- `startTime`: the timestamp when the operation actually started executing.
- `currentMutex`: a snapshot exposing `id`, `queueSize`, and `isLocked`.
- `heldMutexIds`: the set of mutex identifiers currently owned by the surrounding call context (useful for advanced coordination).
- `contextToken`: a token representing the current logical call chain. Pass it to follow-up `runOperation` calls via the
  `context` option when async context propagation is unavailable.
- `abortSignal`: an `AbortSignal` that reflects the lifecycle of the queued operation. It is aborted when timeouts or external
  cancellation requests occur so your callback can clean up cooperatively.

Nested calls to `runOperation` with the same `id` within a single asynchronous call chain are supported. The manager tracks lock
ownership per async context, so re-entrant acquisitions run immediately without rejoining the queue.

To guard against potential soft locks caused by cyclic dependencies (for example, `A -> B -> A`), the manager performs cycle
detection before waiting on another mutex. When a cycle is discovered, the pending acquisition throws `MutexDeadlockError`
instead of waiting forever, allowing your code to handle the situation explicitly. With Redis coordination enabled, this
protection extends across managers running in separate Node.js processes so that cross-instance dependency chains are also
surfaced.

`options` accepts:

- `waitIfLocked`: defaults to `true`. Set to `false` to throw immediately when the mutex is already owned.
- `timeoutMs`: optional number of milliseconds to wait before throwing `MutexTimeoutError`. When omitted the operation waits
  indefinitely. Supplying `0` (or a negative value) performs a single attempt that fails with `MutexTimeoutError` if the mutex
  is not free.
- `context`: provide a `MutexRunContext` returned by `getCurrentContext()` or `createContext()` when you need to stitch together
  asynchronous hops manually (see below).
- `signal`: optional `AbortSignal` that, when aborted, removes the queued operation and rejects the returned promise with
  `MutexAbortedError`.
- `onAbort`: optional hook that receives an `abort(reason?: unknown)` callback. Invoke it to cancel the queued operation without
  providing your own `AbortController`.

### Manual context propagation (browsers, workers, and other runtimes)

In Node.js the manager uses `AsyncLocalStorage`, so the current context flows through promises, timers, and event emitters
automatically. Browser and edge runtimes do not expose that API yet, so the manager falls back to a lightweight stack-based
tracker. That fallback cannot follow asynchronous hops automatically, which would otherwise disable re-entrant locking and
deadlock detection.

To keep those safeguards in place outside of Node.js, capture the provided `contextToken` and pass it to future `runOperation`
calls via the `context` option:

```ts
manager.runOperation('primary', async ({ contextToken }) => {
  setTimeout(() => {
    void manager.runOperation(
      'secondary',
      async ({ contextToken: nested }) => {
        // nested === contextToken
        // This re-entrant acquisition works even without AsyncLocalStorage support.
        return manager.runOperation('primary', doWork, { context: nested });
      },
      { context: contextToken }
    );
  }, 0);
});
```

You can also call `manager.createContext()` to pre-allocate a token for manual propagation, or
`manager.getCurrentContext()` from within an operation to reuse the active token.
Use `manager.isAsyncContextTrackingSupported()` to determine whether your runtime needs manual context passing.

### `MutexLockedError`

Thrown when an operation is configured not to wait for the mutex and the mutex is already occupied. The error has an `id` property that contains the mutex identifier.

### `MutexTimeoutError`

Thrown when `timeoutMs` elapses before the mutex becomes available. The error exposes the requested `id` and the configured `timeoutMs` duration.

### `MutexAbortedError`

Thrown when an operation is cancelled via `AbortSignal` or the `onAbort` hook. The error exposes the requested `id` along with
the supplied `reason` (if any) so callers can differentiate between cancellation sources.

### `DistributedLockError`

Thrown when a distributed lock cannot be acquired due to communication or other unexpected errors.

### `MutexDeadlockError`

Thrown when the manager detects that waiting for a mutex would introduce a deadlock. The error exposes the `requestedId` and the
detected `cycle` of mutex identifiers (for example, `['beta', 'alpha', 'beta']`). Deadlocks are detected before any timeout logic is evaluated, so cycles are surfaced immediately even when operations are configured to wait indefinitely.

### `createContext()`

Creates a detached `MutexRunContext` token. Pass it to `runOperation` via the `context` option when you need to share ownership
across asynchronous boundaries manually.

### `getCurrentContext()`

Returns the active `MutexRunContext` when called inside an operation. Useful for handing the token to other callbacks without
waiting for the promise to resolve.

### `isAsyncContextTrackingSupported()`

Returns `true` when the runtime exposes `AsyncLocalStorage` (Node.js 14+). When it returns `false`, you should propagate
`contextToken`s manually as described above to keep deadlock detection and re-entrancy support intact.

## License

ISC
