import { UniqueMutexManager } from '../../src/UniqueMutexManager';

const redisUrl = process.env.REDIS_URL;
const lockId = process.env.LOCK_ID ?? 'long-op-test';
const operationDuration = parseInt(process.env.OPERATION_DURATION ?? '2000', 10);
const workerId = process.env.WORKER_ID ?? process.pid.toString();

if (!redisUrl) {
    throw new Error('Missing REDIS_URL environment variable');
}

const manager = new UniqueMutexManager({
    redis: { urls: [redisUrl] },
    // Use longer TTL for multi-process reliability
    // Very short TTLs (e.g. 500ms) can cause race conditions due to:
    // - Process scheduling delays
    // - Network latency to Redis
    // - GC pauses
    // These can prevent timely extension, causing lock expiry and overlap
    lockTTL: 2000,
    lockExtendInterval: 600,
});

interface RunResult {
    workerId: string;
    startTime: number;
    endTime: number;
    overlapped: boolean;
}

async function run(): Promise<void> {
    process.send?.({ type: 'ready', workerId });

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
        const result = await manager.runOperation(lockId, async () => {
            const startTime = Date.now();
            process.send?.({ type: 'operation-started', workerId, startTime });

            // Simulate long-running work
            await new Promise((resolve) => setTimeout(resolve, operationDuration));

            const endTime = Date.now();
            return { workerId, startTime, endTime, overlapped: false } as RunResult;
        });

        process.send?.({ type: 'operation-complete', workerId, result });
    } catch (error) {
        process.send?.({
            type: 'operation-error',
            workerId,
            error: error instanceof Error ? { name: error.name, message: error.message } : { message: String(error) },
        });
    }
}

process.on('message', async (message: unknown) => {
    if (message && typeof message === 'object' && (message as { type?: string }).type === 'shutdown') {
        await manager.dispose();
        process.send?.({ type: 'shutdown-complete', workerId });
        process.exit(0);
    }
});

process.on('disconnect', () => {
    void manager.dispose().finally(() => process.exit(0));
});

run().catch((error) => {
    process.send?.({
        type: 'fatal-error',
        workerId,
        error: error instanceof Error ? { name: error.name, message: error.message } : { message: String(error) },
    });
    void manager.dispose().finally(() => process.exit(1));
});
