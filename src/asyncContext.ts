import type { AsyncLocalStorage as NodeAsyncLocalStorage } from 'node:async_hooks';

declare const require: undefined | ((moduleName: string) => unknown);

export type AsyncLocalStorageLike<T> = {
  run<R>(store: T, callback: () => R): R;
  getStore(): T | undefined;
};

function isPromise<T>(value: unknown): value is Promise<T> {
  return (
    typeof value === 'object' &&
    value !== null &&
    'then' in value &&
    typeof (value as { then: unknown }).then === 'function'
  );
}

class StackAsyncLocalStorage<T> implements AsyncLocalStorageLike<T> {
  private readonly stack: T[] = [];

  run<R>(store: T, callback: () => R): R {
    this.stack.push(store);
    let result: R;
    try {
      result = callback();
    } catch (error) {
      this.stack.pop();
      throw error;
    }

    if (isPromise(result)) {
      return (result as Promise<R>).finally(() => {
        this.stack.pop();
      }) as R;
    }

    this.stack.pop();
    return result;
  }

  getStore(): T | undefined {
    if (this.stack.length === 0) {
      return undefined;
    }
    return this.stack[this.stack.length - 1];
  }
}

function loadNodeAsyncLocalStorage<T>(): NodeAsyncLocalStorage<T> | undefined {
  const globalCtor = (globalThis as {
    AsyncLocalStorage?: new () => NodeAsyncLocalStorage<T>;
  }).AsyncLocalStorage;
  if (typeof globalCtor === 'function') {
    return new (globalCtor as new () => NodeAsyncLocalStorage<T>)();
  }

  if (typeof require !== 'function') {
    return undefined;
  }

  try {
    const module = require('node:async_hooks') as {
      AsyncLocalStorage?: new () => NodeAsyncLocalStorage<T>;
    };
    if (module?.AsyncLocalStorage) {
      return new module.AsyncLocalStorage();
    }
  } catch {
    // ignore
  }

  return undefined;
}

export interface AsyncLocalContext<T> {
  storage: AsyncLocalStorageLike<T>;
  supportsAsync: boolean;
}

export function createAsyncLocalStorage<T>(): AsyncLocalContext<T> {
  const nodeInstance = loadNodeAsyncLocalStorage<T>();
  if (nodeInstance) {
    return { storage: nodeInstance, supportsAsync: true };
  }

  return { storage: new StackAsyncLocalStorage<T>(), supportsAsync: false };
}
