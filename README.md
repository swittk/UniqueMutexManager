# UniqueMutexManager

A TypeScript-first mutex manager with optional Redis-backed distributed locking. Use it to serialize asynchronous operations in a single Node.js process, inside browser or edge runtimes, or across multiple processes that share a Redis deployment.

## Installation

```bash
npm install uniquemutexmanager
# Optional peer dependencies if you want distributed locking
npm install ioredis redlock
```

The package publishes CommonJS and modern ES module builds along with bundled type declarations so it can run in Node.js,
browser bundlers, workers, and other JavaScript runtimes. The compiled output targets ES2020. Local-only TypeScript consumers
do not need `ioredis` or `redlock` installed just to resolve this package's declarations; install the optional peers only when
you actually enable Redis-backed coordination.

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

If the mutex is already occupied, the call resolves to `undefined` and the callback is not executed. This behavior is identical for local and Redis-backed managers.

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
  coordinationClient: clients[0],
  namespace: 'payments',
  lockTTL: 60000,
});
```

Alternatively let the manager create a Redlock instance by specifying Redis URLs (requires `ioredis` and `redlock` to be installed):

```ts
const manager = new UniqueMutexManager({
  redis: {
    urls: process.env.REDIS_URL!,
  },
  namespace: 'payments',
});
```

For applications with many mutex managers, reuse externally owned Redis clients instead of opening one connection per manager:

```ts
const redis = new Redis(process.env.REDIS_URL!);

const paymentMutexes = new UniqueMutexManager({
  redis: { clients: [redis] },
  namespace: 'payments',
});
const inventoryMutexes = new UniqueMutexManager({
  redis: { clients: [redis] },
  namespace: 'inventory',
});
```

Externally supplied `redis.clients` are never closed by `dispose()`. URL-based clients, including clients returned by `redis.createClient`, are owned by the manager and are closed by `dispose()`. You can also share a single `Redlock` instance by passing `redlock` plus `coordinationClient`. Both `Redis` and `ioredis.Cluster` clients are supported; deadlock metadata uses independent single-key commands rather than a cross-slot Redis Cluster pipeline.

Call `dispose()` when you are done to close only Redis clients that the manager owns:

```ts
await manager.dispose();
```

When Redis-backed locking is enabled, the manager can also share wait-for metadata so that deadlock detection continues across process boundaries. Metadata is published lazily only when a branch already holds a mutex and must wait for another one; ordinary uncontended acquisitions and top-level contention do not pay this coordination cost. Wait edges carry the exact ownership generation that existed when the wait began, so concurrent branches that reuse one manual context cannot create false cycles by acquiring or releasing unrelated locks later. New metadata lives under a disjoint `uniquemutex-meta-v2:*` prefix, so arbitrary legacy mutex ids cannot collide with it. For rolling upgrades, 0.2 still reads 0.1.x `owner`/`context` metadata as a fallback but does not write new metadata into that collision-prone legacy space. If you supply your own Redlock instance, pass a Redis client via `coordinationClient` to enable cross-process deadlock detection.

Cross-process deadlock coordination is **strict by default**. If metadata cannot be published or read, a nested distributed wait fails with `DistributedLockError` rather than silently disabling deadlock protection. Set `coordinationFailureMode: 'best-effort'` when availability is more important and you accept temporarily losing cross-process cycle detection. Timeouts and abort signals also bound metadata I/O itself, so a hung coordination client cannot make a finite wait unbounded.

Distributed acquisition always performs one bounded Redlock attempt at a time and owns the retry loop itself. With no `timeoutMs`, waiting is genuinely indefinite rather than being capped by Redlock's configured `retryCount`. Under contention it starts with a fast retry and exponentially backs off to the configured Redlock `retryDelay`, avoiding both ~200 ms handoff latency for short critical sections and a Redis hot loop under sustained contention. Redis/quorum failures are surfaced as `DistributedLockError` instead of being mistaken for normal contention.

`namespace` creates a distinct distributed lock domain with encoded components under a separate `uniquemutex-v2:*` key prefix. Managers with the same namespace and mutex id coordinate; different namespaces do not, and namespaced keys cannot collide with arbitrary legacy 0.1.x ids. Omitting `namespace` preserves the legacy `uniquemutex:<id>` lock-key layout for rolling compatibility. For new multi-manager applications, explicit namespaces are recommended. Do not mix namespaced and legacy managers for one logical lock domain because they intentionally use different Redis keys.

Automatic lease extension requires a positive `lockTTL` and, when enabled, a positive `lockExtendInterval` shorter than the TTL. Extension attempts are serialized and use single bounded Redlock attempts so a generic Redlock retry window cannot run past the current lease deadline. If lease ownership is lost, `abortSignal` is aborted and `assertLock()` throws `LockExtensionError`. Long-running work should check `abortSignal` or call `assertLock()` between irreversible steps.

A distributed mutex is a lease, not a database transaction or fencing mechanism. Persistent uniqueness, money, inventory, and other durable invariants should still use database constraints, compare-and-set/version checks, idempotency keys, or another authoritative persistence-layer guard where appropriate.

## 0.2 migration notes

- Upgrade distributed users from 0.1.5 before enabling multi-instance locking. 0.1.5 did not retain the immutable replacement lock returned by Redlock v5 extension, which could allow a long-running lease to overlap another owner after the original TTL.
- `waitIfLocked: false` now has one contract everywhere: the callback is skipped and the promise resolves to `undefined` on contention. `MutexLockedError` remains exported only for source compatibility.
- Omitting `namespace` preserves the legacy `uniquemutex:<id>` **lock** resource so 0.2 can coordinate mutual exclusion with 0.1.x during a rolling upgrade. New deadlock metadata is written only under `uniquemutex-meta-v2:*`; 0.2 can read old metadata as a fallback.
- Adding `namespace` intentionally creates a new lock domain under `uniquemutex-v2:*`. All participants in one logical lock domain must use the same namespace.
- When cross-process deadlock coordination is available, nested waits use `coordinationFailureMode: 'strict'` by default. Choose `'best-effort'` explicitly if you prefer availability when metadata Redis is unavailable.
- For applications with many managers, prefer one shared `redis.clients` connection (or one shared Redlock + coordination client) instead of creating a Redis connection per manager.

## API

### Distributed constructor options

- `redlock`: externally owned Redlock instance. Pair with `coordinationClient` when cross-process deadlock detection is required.
- `redis.urls`: one or more Redis URLs for manager-owned clients.
- `redis.clients`: externally owned `ioredis` Redis/Cluster clients that can be shared by many mutex managers.
- `redis.settings`: Redlock settings used when the manager constructs Redlock. Acquisition still uses one bounded attempt at a time; `retryDelay`/`retryJitter` define the backoff envelope.
- `namespace`: explicit distributed lock domain. Recommended when an application has multiple independent mutex registries.
- `coordinationFailureMode`: `strict` by default, or `best-effort` to continue nested waits when deadlock metadata is unavailable.
- `lockTTL`: positive safe-integer lease duration in milliseconds.
- `lockExtendInterval`: renewal interval; `0` or a negative value disables automatic renewal, otherwise it must be shorter than `lockTTL`.

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

- `waitIfLocked`: defaults to `true`. Set to `false` for a single non-blocking attempt; the callback is skipped and the call resolves to `undefined` when the mutex is occupied.
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

Retained as an exported compatibility type for applications that referenced earlier 0.1.x behavior. Current `waitIfLocked: false` calls resolve to `undefined` on contention so local and distributed managers have the same non-blocking contract.

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

WTFPL
