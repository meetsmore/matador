import { afterEach, describe, expect, it, mock } from 'bun:test';
import { ShutdownInProgressError } from '../errors/index.js';
import { TopologyBuilder } from '../topology/index.js';
import { LocalTransport } from '../transport/local/local-transport.js';
import type { Subscription, Transport } from '../transport/transport.js';
import { Matador } from './matador.js';

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((complete) => {
    resolve = complete;
  });
  return { promise, resolve };
}

async function bounded<T>(promise: Promise<T>): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_, reject) => {
        timer = setTimeout(
          () => reject(new Error('Lifecycle operation did not settle')),
          1000,
        );
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

const instances: Matador[] = [];
afterEach(async () => {
  for (const instance of instances.splice(0)) {
    await bounded(instance.shutdown());
  }
});

function fixture(retry = true) {
  const unsubscribe = mock(async () => {});
  const subscription: Subscription = { isActive: true, unsubscribe };
  const connect = mock(async () => {});
  const disconnect = mock(async () => {});
  const applyTopology = mock(async () => {});
  const subscribe = mock(
    async (_queue: string): Promise<Subscription> => subscription,
  );
  const transport: Transport = {
    name: 'startup-contract',
    capabilities: new LocalTransport().capabilities,
    connect,
    disconnect,
    applyTopology,
    subscribe,
    isConnected: () => true,
    send: async () => 'startup-contract',
    complete: async () => {},
  };
  const matador = new Matador({
    transport,
    topology: TopologyBuilder.create()
      .withNamespace('startup-contract')
      .addQueue('first')
      .addQueue('second')
      .build(),
    schema: {},
    consumeFrom: ['first', 'second'],
    retryIntervalMs: 0,
    ...(retry ? { startupRetry: { retryIntervalMs: 5 } } : {}),
  });
  instances.push(matador);
  return {
    matador,
    connect,
    disconnect,
    applyTopology,
    subscribe,
    unsubscribe,
  };
}

describe('Matador startup lifecycle', () => {
  it('joins concurrent startup until all queues are subscribed', async () => {
    const f = fixture();
    const entered = deferred<void>();
    const release = deferred<void>();
    f.connect.mockImplementation(async () => {
      entered.resolve();
      await release.promise;
    });
    const first = f.matador.start();
    const second = f.matador.start();
    try {
      await bounded(entered.promise);
      expect(f.connect).toHaveBeenCalledTimes(1);
    } finally {
      release.resolve();
      await bounded(Promise.all([first, second]));
    }
    expect(f.subscribe).toHaveBeenCalledTimes(2);
  });

  it.each(['connect', 'applyTopology', 'subscribe'] as const)(
    'retries rejected %s without duplicating successful subscriptions',
    async (stage) => {
      const f = fixture();
      const failure = new Error(`temporary ${stage} failure`);
      if (stage === 'subscribe') {
        let secondCalls = 0;
        f.subscribe.mockImplementation(async (queue) => {
          if (queue.endsWith('.second') && secondCalls++ === 0) throw failure;
          return { isActive: true, unsubscribe: f.unsubscribe };
        });
      } else {
        f[stage].mockRejectedValueOnce(failure);
      }
      await bounded(f.matador.start());
      const firstQueueCalls = f.subscribe.mock.calls.filter(([queue]) =>
        queue.endsWith('.first'),
      );
      expect(firstQueueCalls).toHaveLength(1);
      expect(
        f.subscribe.mock.calls.filter(([queue]) => queue.endsWith('.second')),
      ).toHaveLength(stage === 'subscribe' ? 2 : 1);
    },
  );

  it('preserves the original transport error when startup retry is omitted', async () => {
    const f = fixture(false);
    const failure = new Error('connection refused');
    f.connect.mockRejectedValueOnce(failure);
    await expect(f.matador.start()).rejects.toBe(failure);
    expect(f.connect).toHaveBeenCalledTimes(1);
  });

  for (const operation of ['stopReceiving', 'shutdown'] as const) {
    it.each(['connect', 'applyTopology', 'subscribe'] as const)(
      `${operation} cancels pending %s and releases late resources`,
      async (stage) => {
        const f = fixture();
        const entered = deferred<void>();
        const release = deferred<void>();
        const lateUnsubscribe = mock(async () => {});
        if (stage === 'subscribe') {
          f.subscribe.mockImplementation(async () => {
            entered.resolve();
            await release.promise;
            return { isActive: true, unsubscribe: lateUnsubscribe };
          });
        } else {
          f[stage].mockImplementation(async () => {
            entered.resolve();
            await release.promise;
          });
        }
        const starting = f.matador.start().then(
          () => 'ready',
          (error: unknown) => error,
        );
        let disconnectsBeforeLateCompletion = 0;
        try {
          await bounded(entered.promise);
          await bounded(f.matador[operation]());
          expect(await bounded(starting)).toBeInstanceOf(
            ShutdownInProgressError,
          );
          await expect(f.matador.start()).rejects.toBeInstanceOf(
            ShutdownInProgressError,
          );
        } finally {
          disconnectsBeforeLateCompletion = f.disconnect.mock.calls.length;
          release.resolve();
          await bounded(starting);
          // Drain callbacks attached to the late operation before inspecting cleanup.
          await new Promise((resolve) => setTimeout(resolve, 10));
        }
        if (stage === 'subscribe') {
          expect(lateUnsubscribe).toHaveBeenCalledTimes(1);
          expect(f.subscribe).toHaveBeenCalledTimes(1);
        } else if (stage === 'connect') {
          expect(f.disconnect.mock.calls.length).toBeGreaterThan(
            disconnectsBeforeLateCompletion,
          );
          expect(f.applyTopology).not.toHaveBeenCalled();
        } else {
          expect(f.subscribe).not.toHaveBeenCalled();
        }
      },
    );
  }

  it('joins a pending receiving stop before disconnecting during shutdown', async () => {
    const f = fixture();
    const entered = deferred<void>();
    const release = deferred<void>();
    const pauseForShutdown = mock(async () => {
      entered.resolve();
      await release.promise;
    });
    f.subscribe.mockResolvedValue({
      isActive: true,
      unsubscribe: f.unsubscribe,
      pauseForShutdown,
    });
    await bounded(f.matador.start());
    const stopping = f.matador.stopReceiving();
    let shutdown: Promise<void> | undefined;
    try {
      await bounded(entered.promise);
      shutdown = f.matador.shutdown();
      expect(f.disconnect).not.toHaveBeenCalled();
    } finally {
      release.resolve();
      await bounded(stopping);
      if (shutdown) await bounded(shutdown);
    }
    expect(f.disconnect).toHaveBeenCalledTimes(1);
  });

  it('keeps exactly one consumer per queue when subscription retry follows a reconnect', async () => {
    const f = fixture();
    const active = new Map<string, number>();
    let failed = false;
    let generation = 0;
    f.connect.mockImplementation(async () => {
      generation++;
      // A broker reconnect restores existing subscription intents itself.
      for (const queue of active.keys()) active.set(queue, 1);
    });
    f.subscribe.mockImplementation(async (queue) => {
      if (queue.endsWith('.second') && !failed) {
        failed = true;
        await f.connect();
        throw new Error('subscription failed across broker reconnect');
      }
      active.set(queue, (active.get(queue) ?? 0) + 1);
      return {
        isActive: true,
        unsubscribe: async () => {
          active.delete(queue);
        },
      };
    });
    await bounded(f.matador.start());
    expect(generation).toBeGreaterThanOrEqual(2);
    expect([...active.values()]).toEqual([1, 1]);
    expect(
      f.subscribe.mock.calls.filter(([queue]) => queue.endsWith('.first')),
    ).toHaveLength(1);
  });
});
