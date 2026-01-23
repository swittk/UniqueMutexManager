/**
 * Nightmare scenario worker for true multi-process distributed testing.
 * Uses IPC for test coordination but logs to Redis for overlap detection.
 * Supports nested lock patterns (A→B→A) with configurable latency.
 */
import { UniqueMutexManager } from '../../src/UniqueMutexManager';
import IORedis from 'ioredis';

console.log(`[Nightmare] Worker starting... ID=${process.env.WORKER_ID}`);


const redisUrl = process.env.REDIS_URL;
const workerId = process.env.WORKER_ID ?? process.pid.toString();
const lockPattern = process.env.LOCK_PATTERN ?? 'single'; // 'single', 'nested-aba'
const operationLatency = parseInt(process.env.OPERATION_LATENCY ?? '500', 10);
const coordKey = process.env.COORD_KEY ?? 'nightmare-test:coord';

if (!redisUrl) {
    console.error('Missing REDIS_URL');
    process.exit(1);
}

const redis = new IORedis(redisUrl);
const manager = new UniqueMutexManager({
    redis: {
        urls: [redisUrl],
        settings: {
            retryCount: 20,
            retryDelay: 150,
            retryJitter: 100,
        }
    },
    lockTTL: 30000,
    lockExtendInterval: 10000,
});

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const randomLatency = () => sleep(operationLatency + Math.random() * 300);

interface OperationRecord {
    workerId: string;
    lockId: string;
    phase: 'enter' | 'exit';
    timestamp: number;
}

async function recordOperation(lockId: string, phase: 'enter' | 'exit'): Promise<void> {
    const record: OperationRecord = {
        workerId,
        lockId,
        phase,
        timestamp: Date.now(),
    };
    await redis.rpush(`${coordKey}:log`, JSON.stringify(record));
}

async function log(msg: string) {
    await redis.rpush(`${coordKey}:debug`, `[${workerId}] ${msg} (${Date.now()})`);
}

async function runSingleLock(): Promise<void> {
    await log('Requesting single lock');
    await manager.runOperation('nightmare-mutex', async () => {
        await log('Acquired single lock');
        await recordOperation('nightmare-mutex', 'enter');
        await randomLatency();
        await recordOperation('nightmare-mutex', 'exit');
        await log('Released single lock');
    }, { timeoutMs: 20000 });
}

async function runNestedABA(): Promise<void> {
    await log('Requesting lock A (outer)');
    // Pattern: lockA → lockB → lockA (nested reentrant back to A)
    await manager.runOperation('nightmare-A', async () => {
        await log('Acquired lock A (outer)');
        await recordOperation('nightmare-A', 'enter');
        await randomLatency();

        await log('Requesting lock B');
        await manager.runOperation('nightmare-B', async () => {
            await log('Acquired lock B');
            await recordOperation('nightmare-B', 'enter');
            await randomLatency();

            await log('Requesting lock A (reentrant)');
            // Reentrant back to A
            await manager.runOperation('nightmare-A', async () => {
                await log('Acquired lock A (reentrant)');
                await recordOperation('nightmare-A-reentrant', 'enter');
                await randomLatency();
                await recordOperation('nightmare-A-reentrant', 'exit');
                await log('Released lock A (reentrant)');
            }, { timeoutMs: 20000 });

            await recordOperation('nightmare-B', 'exit');
            await log('Released lock B');
        }, { timeoutMs: 20000 });

        await recordOperation('nightmare-A', 'exit');
        await log('Released lock A (outer)');
    }, { timeoutMs: 30000 });
}

async function runCrash(): Promise<void> {
    await log('Requesting lock for crash test');
    await manager.runOperation('nightmare-crash', async () => {
        await log('Acquired lock - ready to crash');
        await recordOperation('nightmare-crash', 'enter');
        // Signal ready to be killed
        if (process.send) process.send({ type: 'ready-to-crash', workerId });
        // Hold lock and wait to be killed
        await new Promise(() => { });
    }, { timeoutMs: 10000 });
}

async function runRecovery(): Promise<void> {
    await log('Recovery worker waiting for lock...');
    // Should block until TTL expires
    await manager.runOperation('nightmare-crash', async () => {
        await log('Acquired lock after crash recovery');
        await recordOperation('nightmare-crash', 'enter');
        await sleep(100);
        await recordOperation('nightmare-crash', 'exit');
    }, { timeoutMs: 45000 }); // Should succeed after TTL (which is 30s)
}

async function runDeepNestedABCAB(): Promise<void> {
    const opts = { timeoutMs: 30000 };
    await log('Start ABCAB pattern');

    // A
    await manager.runOperation('nightmare-A', async () => {
        await log('Got A');
        await recordOperation('nightmare-A', 'enter');
        await randomLatency();

        // B
        await manager.runOperation('nightmare-B', async () => {
            await log('Got B');
            await recordOperation('nightmare-B', 'enter');
            await randomLatency();

            // C
            await manager.runOperation('nightmare-C', async () => {
                await log('Got C');
                await recordOperation('nightmare-C', 'enter');
                await randomLatency();

                // A (Reentrant)
                await manager.runOperation('nightmare-A', async () => {
                    await log('Got A (reentrant)');
                    await recordOperation('nightmare-A-reentrant', 'enter');
                    await randomLatency();
                    await recordOperation('nightmare-A-reentrant', 'exit');
                }, opts);

                // B (Reentrant)
                await manager.runOperation('nightmare-B', async () => {
                    await log('Got B (reentrant)');
                    await recordOperation('nightmare-B-reentrant', 'enter');
                    await randomLatency();
                    await recordOperation('nightmare-B-reentrant', 'exit');
                }, opts);

                await recordOperation('nightmare-C', 'exit');
            }, opts);

            await recordOperation('nightmare-B', 'exit');
        }, opts);

        await recordOperation('nightmare-A', 'exit');
        await log('Finished ABCAB pattern');
    }, { timeoutMs: 60000 }); // Longer timeout for deep nest
}

async function run(): Promise<void> {
    // Wait for ping from parent before checking in
    // This prevents "ready" message being sent before parent attaches listener
    await new Promise<void>((resolve) => {
        const handler = (message: unknown) => {
            console.log('Worker received message:', message);
            if (message && typeof message === 'object' && (message as { type?: string }).type === 'ping') {
                console.log('Worker received PING, proceeding...');
                process.off('message', handler);
                resolve();
            }
        };
        process.on('message', handler);
    });

    console.log('Worker passed ping barrier. Sending ready signal...');
    // Signal ready via IPC
    if (process.send) {
        console.log('process.send exists, sending ready.');
        process.send({ type: 'ready', workerId });
    } else {
        console.log('process.send is MISSING!');
    }

    // Wait for start signal via IPC
    await new Promise<void>((resolve) => {
        const handler = (message: unknown) => {
            if (message && typeof message === 'object' && (message as { type?: string }).type === 'start') {
                process.off('message', handler);
                resolve();
            }
        };
        process.on('message', handler);
    });

    try {
        if (lockPattern === 'nested-aba') {
            await runNestedABA();
        } else if (lockPattern === 'nested-abcab') {
            await runDeepNestedABCAB();
        } else if (lockPattern === 'crash-victim') {
            await runCrash();
        } else if (lockPattern === 'crash-recovery') {
            await runRecovery();
        } else {
            await runSingleLock();
        }

        if (process.send) process.send({ type: 'complete', workerId });
    } catch (error) {
        await redis.rpush(
            `${coordKey}:errors`,
            JSON.stringify({
                workerId,
                error: error instanceof Error ? error.message : String(error),
                timestamp: Date.now(),
            })
        );
        if (process.send) process.send({
            type: 'error',
            workerId,
            error: error instanceof Error ? error.message : String(error),
        });
    } finally {
        if (lockPattern !== 'crash-victim') { // Don't dispose if we're crashing
            await manager.dispose();
            await redis.quit();
        }
    }
}

process.on('message', async (message: unknown) => {
    if (message && typeof message === 'object' && (message as { type?: string }).type === 'shutdown') {
        if (lockPattern !== 'crash-victim') {
            await manager.dispose();
            await redis.quit();
        }
        if (process.send) process.send({ type: 'shutdown-complete', workerId });
        process.exit(0);
    }
});

process.on('disconnect', () => {
    if (lockPattern !== 'crash-victim') {
        void manager.dispose().then(() => redis.quit()).finally(() => process.exit(0));
    }
});

run().catch((error) => {
    if (process.send) process.send({
        type: 'fatal-error',
        workerId,
        error: error instanceof Error ? error.message : String(error),
    });
    // Cleanup unless crashing
    if (lockPattern !== 'crash-victim') {
        void manager.dispose().then(() => redis.quit()).finally(() => process.exit(1));
    }
});

