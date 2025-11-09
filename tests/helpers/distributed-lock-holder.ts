import { setTimeout as sleep } from 'node:timers/promises';

import { UniqueMutexManager } from '../../src/UniqueMutexManager';

const redisUrl = process.env.REDIS_URL;
if (!redisUrl) {
  process.send?.({ type: 'error', message: 'Missing REDIS_URL' });
  process.exit(1);
}

const lockTTL = Number(process.env.LOCK_TTL ?? '500');
const lockExtendInterval = Number(process.env.LOCK_EXTEND_INTERVAL ?? '0');

const manager = new UniqueMutexManager({
  redis: { urls: redisUrl },
  lockTTL,
  lockExtendInterval,
});

process.on('message', (message: unknown) => {
  if (typeof message !== 'object' || message === null) {
    return;
  }

  const typed = message as { type?: string; id?: string };

  if (typed.type === 'acquire' && typeof typed.id === 'string') {
    void (async () => {
      try {
        await manager.runOperation(typed.id, async () => {
          process.send?.({ type: 'acquired', id: typed.id });
          await sleep(10_000);
        });
        process.send?.({ type: 'released', id: typed.id });
      } catch (error) {
        process.send?.({
          type: 'error',
          message: error instanceof Error ? error.message : String(error),
          name: error instanceof Error ? error.name : undefined,
        });
      }
    })();
  } else if (typed.type === 'shutdown') {
    void manager.dispose().finally(() => process.exit(0));
  }
});

process.send?.({ type: 'ready' });
