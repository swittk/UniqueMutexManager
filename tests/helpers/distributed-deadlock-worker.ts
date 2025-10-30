import { UniqueMutexManager } from '../../src/UniqueMutexManager';
import { MutexDeadlockError } from '../../src/errors';

const redisUrl = process.env.REDIS_URL;
const primaryId = process.env.PRIMARY_ID;
const nestedId = process.env.NESTED_ID;
const workerId = process.env.WORKER_LABEL ?? process.pid.toString();

if (!redisUrl || !primaryId || !nestedId) {
  throw new Error('Missing required environment configuration for distributed-deadlock-worker');
}

const manager = new UniqueMutexManager({ redis: { urls: [redisUrl] } });

let proceedRequested = false;
let proceedResolver: (() => void) | undefined;
let activeOperation: Promise<void> | undefined;

function requestProceed(): void {
  if (proceedResolver) {
    const resolver = proceedResolver;
    proceedResolver = undefined;
    resolver();
  } else {
    proceedRequested = true;
  }
}

function waitForProceedSignal(): Promise<void> {
  if (proceedRequested) {
    proceedRequested = false;
    return Promise.resolve();
  }

  return new Promise<void>((resolve) => {
    proceedResolver = () => {
      proceedResolver = undefined;
      proceedRequested = false;
      resolve();
    };
  });
}

async function runCycle(): Promise<void> {
  try {
    const result = await manager.runOperation(primaryId, async () => {
      process.send?.({
        type: 'outer-started',
        workerId,
        primaryId,
      });

      await waitForProceedSignal();

      try {
        const nestedResult = await manager.runOperation(nestedId, async () => `${primaryId}->${nestedId}`);
        process.send?.({
          type: 'nested-result',
          workerId,
          status: 'resolved',
          value: nestedResult,
        });
      } catch (error) {
        if (error instanceof MutexDeadlockError) {
          process.send?.({
            type: 'nested-result',
            workerId,
            status: 'deadlock',
            message: error.message,
            cycle: error.cycle,
            requestedId: error.requestedId,
          });
        } else if (error instanceof Error) {
          process.send?.({
            type: 'nested-result',
            workerId,
            status: 'error',
            message: error.message,
            name: error.name,
          });
        } else {
          process.send?.({
            type: 'nested-result',
            workerId,
            status: 'error',
            message: 'Unknown error type thrown during nested acquisition',
          });
        }
      }

      return `${primaryId}-outer`;
    });

    process.send?.({
      type: 'outer-complete',
      workerId,
      value: result,
    });
  } catch (error) {
    const serialized = error instanceof Error ? { name: error.name, message: error.message } : { message: 'Unknown outer operation failure' };
    process.send?.({
      type: 'outer-error',
      workerId,
      error: serialized,
    });
  }
}

process.on('message', (message: unknown) => {
  if (!message || typeof message !== 'object') {
    return;
  }

  const payload = message as { type?: string };

  switch (payload.type) {
    case 'start-cycle': {
      if (!activeOperation) {
        activeOperation = runCycle().finally(() => {
          activeOperation = undefined;
        });
      }
      break;
    }
    case 'proceed-nested': {
      requestProceed();
      break;
    }
    case 'shutdown': {
      (async () => {
        try {
          if (activeOperation) {
            await activeOperation;
          }
          await manager.dispose();
          process.send?.({ type: 'shutdown-complete', workerId });
        } finally {
          process.exit(0);
        }
      })();
      break;
    }
    default:
      break;
  }
});

process.on('disconnect', () => {
  void manager.dispose().finally(() => process.exit(0));
});

process.send?.({ type: 'ready', workerId, primaryId, nestedId });

async function handleFatal(error: Error): Promise<void> {
  process.send?.({
    type: 'fatal-error',
    workerId,
    name: error.name,
    message: error.message,
    stack: error.stack,
  });
  try {
    await manager.dispose();
  } finally {
    process.exit(1);
  }
}

process.on('uncaughtException', (error) => {
  void handleFatal(error);
});

process.on('unhandledRejection', (reason) => {
  const error = reason instanceof Error ? reason : new Error(`Unhandled rejection: ${String(reason)}`);
  void handleFatal(error);
});
