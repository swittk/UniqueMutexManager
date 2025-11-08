import type { AsyncLocalStorage as NodeAsyncLocalStorage } from 'node:async_hooks';

import { importOptionalModule, loadOptionalModule } from './moduleLoader';

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

    try {
      const result = callback();
      if (isPromise(result)) {
        this.stack.pop();
        return result;
      }

      this.stack.pop();
      return result;
    } catch (error) {
      this.stack.pop();
      throw error;
    }
  }

  getStore(): T | undefined {
    if (this.stack.length === 0) {
      return undefined;
    }
    return this.stack[this.stack.length - 1];
  }
}

async function loadNodeAsyncLocalStorage<T>(): Promise<NodeAsyncLocalStorage<T> | undefined> {
  const globalCtor = (globalThis as {
    AsyncLocalStorage?: new () => NodeAsyncLocalStorage<T>;
  }).AsyncLocalStorage;
  if (typeof globalCtor === 'function') {
    return new (globalCtor as new () => NodeAsyncLocalStorage<T>)();
  }

  if (typeof require === 'function') {
    try {
      const module = loadOptionalModule<{ AsyncLocalStorage?: new () => NodeAsyncLocalStorage<T> }>(
        'node:async_hooks'
      );
      if (module?.AsyncLocalStorage) {
        return new module.AsyncLocalStorage();
      }
    } catch {
      // ignore
    }
  }

  try {
    const asyncHooks = await importOptionalModule<{
      AsyncLocalStorage?: new () => NodeAsyncLocalStorage<T>;
    }>('node:async_hooks');
    if (asyncHooks?.AsyncLocalStorage) {
      return new asyncHooks.AsyncLocalStorage();
    }
  } catch {
    // ignore failures from dynamic import as well.
  }

  return undefined;
}

export interface AsyncLocalContext<T> {
  storage: AsyncLocalStorageLike<T>;
  supportsAsync: boolean;
}

export async function createAsyncLocalStorage<T>(): Promise<AsyncLocalContext<T>> {
  const nodeInstance = await loadNodeAsyncLocalStorage<T>();
  if (nodeInstance) {
    return { storage: nodeInstance, supportsAsync: true };
  }

  return { storage: new StackAsyncLocalStorage<T>(), supportsAsync: false };
}

export function createFallbackAsyncLocalStorage<T>(): AsyncLocalStorageLike<T> {
  return new StackAsyncLocalStorage<T>();
}
