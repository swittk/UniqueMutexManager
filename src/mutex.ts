export interface MutexState {
  pending: number;
}

export class Mutex {
  constructor(public readonly id: string, private readonly state: MutexState) {}

  /**
   * Number of operations queued for this mutex, including the active one.
   */
  get queueSize(): number {
    return this.state.pending;
  }

  /**
   * Whether the mutex currently has an operation running.
   */
  get isLocked(): boolean {
    return this.state.pending > 0;
  }
}
