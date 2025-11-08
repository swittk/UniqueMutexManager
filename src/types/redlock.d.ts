declare module 'redlock' {
  export { default } from 'redlock/dist/index.js';
  export { ExecutionError, Lock, Settings } from 'redlock/dist/index.js';
  export type { ExecutionResult, ExecutionStats, ClientExecutionResult, RedlockAbortSignal } from 'redlock/dist/index.js';
}
