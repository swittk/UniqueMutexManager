const { parentPort, workerData } = require('node:worker_threads');
const { setTimeout: sleep } = require('node:timers/promises');

require('./register-ts');

const { UniqueMutexManager } = require('../../src/UniqueMutexManager');

const {
  redisUrl,
  workerId,
  mutexId,
  operations,
  delayMs = 0,
  lockTTL,
  lockExtendInterval,
} = workerData;

const manager = new UniqueMutexManager({
  redis: { urls: redisUrl },
  lockTTL,
  lockExtendInterval,
});

parentPort.postMessage({ type: 'ready', workerId });

const run = async () => {
  try {
    await Promise.all(
      Array.from({ length: operations }, (_, index) =>
        manager.runOperation(mutexId, async () => {
          parentPort.postMessage({ type: 'started', workerId, index });
          if (delayMs > 0) {
            await sleep(delayMs);
          }
          parentPort.postMessage({ type: 'ended', workerId, index });
          return `${workerId}:${index}`;
        })
      )
    );
    parentPort.postMessage({ type: 'done', workerId });
  } catch (error) {
    parentPort.postMessage({
      type: 'error',
      workerId,
      message: error instanceof Error ? error.message : String(error),
      name: error instanceof Error ? error.name : undefined,
    });
  } finally {
    await manager.dispose();
    parentPort.close();
  }
};

parentPort.once('message', (message) => {
  if (message && message.type === 'start') {
    void run();
  } else {
    parentPort.postMessage({
      type: 'error',
      workerId,
      message: 'Unexpected message received',
    });
    parentPort.close();
  }
});
