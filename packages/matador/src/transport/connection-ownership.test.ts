import { describe, expect, it, mock } from 'bun:test';
import { ConnectionManager } from './connection-manager.js';

function deferred() {
  let resolve!: () => void;
  let reject!: (error: Error) => void;
  const promise = new Promise<void>((done, fail) => {
    resolve = done;
    reject = fail;
  });
  return { promise, resolve, reject };
}

async function bounded<T>(promise: Promise<T>): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_, reject) => {
        timer = setTimeout(
          () => reject(new Error('Connection lifecycle did not settle')),
          1000,
        );
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

describe('ConnectionManager generation ownership', () => {
  it.each(['resolve', 'reject'] as const)(
    'does not revive after a late connect %s',
    async (outcome) => {
      const pending = deferred();
      const connect = mock(() => pending.promise);
      const manager = new ConnectionManager(connect, async () => {}, {
        maxReconnectAttempts: 1,
      });
      const states: string[] = [];
      manager.onStateChange((state) => states.push(state.status));
      const connecting = manager.connect().catch(() => {});
      try {
        await bounded(manager.disconnect());
        const stoppedAt = states.length;
        if (outcome === 'resolve') pending.resolve();
        else pending.reject(new Error('late failure'));
        await bounded(connecting);
        expect(manager.state.status).toBe('disconnected');
        expect(states.slice(stoppedAt)).toEqual([]);
        expect(connect).toHaveBeenCalledTimes(1);
      } finally {
        pending.resolve();
        try {
          await bounded(connecting);
        } finally {
          await bounded(manager.disconnect());
        }
      }
    },
  );

  it('joins simultaneous connect operations', async () => {
    const pending = deferred();
    const connect = mock(() => pending.promise);
    const manager = new ConnectionManager(connect, async () => {});
    const first = manager.connect();
    const second = manager.connect();
    void first.catch(() => {});
    void second.catch(() => {});
    try {
      expect(connect).toHaveBeenCalledTimes(1);
    } finally {
      pending.resolve();
      try {
        await bounded(Promise.all([first, second]));
      } finally {
        await bounded(manager.disconnect());
      }
    }
  });

  it('settles a cancelled reconnect wait without connecting again', async () => {
    const reconnecting = deferred();
    const connect = mock(async () => {
      throw new Error('broker unavailable');
    });
    const manager = new ConnectionManager(connect, async () => {}, {
      initialReconnectDelay: 10_000,
    });
    manager.onStateChange((state) => {
      if (state.status === 'reconnecting') reconnecting.resolve();
    });
    const connecting = manager.connect().then(
      () => 'settled',
      () => 'settled',
    );
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      await bounded(reconnecting.promise);
      await bounded(manager.disconnect());
      const result = await Promise.race([
        connecting,
        new Promise<string>((resolve) => {
          timer = setTimeout(() => resolve('still pending'), 100);
        }),
      ]);
      expect(result).toBe('settled');
      expect(connect).toHaveBeenCalledTimes(1);
      expect(manager.state.status).toBe('disconnected');
    } finally {
      clearTimeout(timer);
      await bounded(manager.disconnect());
    }
  });
});
