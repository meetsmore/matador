import { afterEach, describe, expect, it, mock, spyOn } from 'bun:test';
import { EventEmitter } from 'node:events';
import amqplib, { type ChannelModel, type Options } from 'amqplib';
import { TopologyBuilder } from '../../topology/index.js';
import { createDummyEnvelope } from '../../types/index.js';
import type { MessageReceipt } from '../transport.js';
import { RabbitMQTransport } from './rabbitmq-transport.js';

interface PublishedMessage {
  exchange: string;
  routingKey: string;
  content: Buffer;
  options: Options.Publish;
  confirm: (error: Error | null) => void;
}

class PublisherChannel extends EventEmitter {
  closed = false;
  messages: PublishedMessage[] = [];
  assertExchange = mock(async () => {});
  assertQueue = mock(async () => {});
  bindQueue = mock(async () => {});
  ack = mock(() => {});
  publish = mock(
    (
      exchange: string,
      routingKey: string,
      content: Buffer,
      options: Options.Publish,
      confirm: (error: Error | null) => void,
    ) => {
      if (this.closed) throw new Error('Channel closed');
      this.messages.push({ exchange, routingKey, content, options, confirm });
      return true;
    },
  );
  close = mock(async () => {
    this.closed = true;
    this.emit('close');
    for (const message of this.messages)
      message.confirm(new Error('channel closed'));
  });
  returnMessage(message: PublishedMessage) {
    this.emit('return', {
      content: message.content,
      properties: message.options,
      fields: {
        replyCode: 312,
        replyText: 'NO_ROUTE',
        exchange: message.exchange,
        routingKey: message.routingKey,
      },
    });
  }
}

class BrokerConnection extends EventEmitter {
  initial = new PublisherChannel();
  createConfirmChannel = mock(async () => this.initial);
  createChannel = mock(async () => new PublisherChannel());
  close = mock(async () => {
    await this.initial.close();
    this.emit('close');
  });
}

const transports: RabbitMQTransport[] = [];
const restore: (() => void)[] = [];
const tick = () => new Promise((resolve) => setTimeout(resolve, 0));
const observed = (promise: Promise<unknown>) =>
  promise.then(
    (value) => value,
    (error: unknown) => error,
  );

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((done) => {
    resolve = done;
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
          () => reject(new Error('AMQP lifecycle did not settle')),
          1000,
        );
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

async function withTrackedTimers(
  run: (pendingCount: () => number) => Promise<void>,
) {
  const schedule = globalThis.setTimeout;
  const cancel = globalThis.clearTimeout;
  const pending = new Set<ReturnType<typeof setTimeout>>();
  const scheduled = spyOn(globalThis, 'setTimeout').mockImplementation(((
    callback: (...args: unknown[]) => void,
    delay?: number,
    ...args: unknown[]
  ) => {
    const handle = schedule(() => {
      pending.delete(handle);
      callback(...args);
    }, delay);
    pending.add(handle);
    return handle;
  }) as typeof setTimeout);
  const cancelled = spyOn(globalThis, 'clearTimeout').mockImplementation(
    (handle: Parameters<typeof clearTimeout>[0]) => {
      pending.delete(handle as ReturnType<typeof setTimeout>);
      cancel(handle);
    },
  );
  try {
    await run(() => pending.size);
  } finally {
    scheduled.mockRestore();
    cancelled.mockRestore();
    // Failed assertions still release real timers; the census is asserted first.
    for (const handle of pending) cancel(handle);
  }
}

function unstarted(
  connectImplementation: () => Promise<ChannelModel>,
  enableDelayedMessages = false,
) {
  const connect = spyOn(amqplib, 'connect').mockImplementation(
    connectImplementation,
  );
  restore.push(() => connect.mockRestore());
  const transport = new RabbitMQTransport({
    url: 'amqp://unused',
    connectionName: 'publisher-contract',
    enableDelayedMessages,
    publishTimeoutMs: 100,
  });
  transports.push(transport);
  return { transport, connect };
}

async function start(enableDelayedMessages = false) {
  const connection = new BrokerConnection();
  const { transport, connect } = unstarted(
    async () => connection as unknown as ChannelModel,
    enableDelayedMessages,
  );
  await bounded(transport.connect());
  await bounded(
    transport.applyTopology(
      TopologyBuilder.create()
        .withNamespace('publisher-contract')
        .addQueue('work')
        .build(),
    ),
  );
  return { transport, connection, channel: connection.initial, connect };
}

afterEach(async () => {
  try {
    const results = await Promise.allSettled(
      transports.splice(0).map((transport) => bounded(transport.disconnect())),
    );
    const failures = results.filter((result) => result.status === 'rejected');
    if (failures.length) {
      throw new AggregateError(
        failures.map((failure) => failure.reason),
        'Transport fixture cleanup failed',
      );
    }
  } finally {
    for (const reset of restore.splice(0)) reset();
  }
});

describe('RabbitMQ publisher lifecycle', () => {
  it('closes a late broker connection instead of reviving a stopped transport', async () => {
    const pending = deferred<BrokerConnection>();
    const connection = new BrokerConnection();
    const { transport } = unstarted(
      async () => (await pending.promise) as unknown as ChannelModel,
    );
    const connecting = observed(transport.connect());
    try {
      await bounded(transport.disconnect());
      pending.resolve(connection);
      await bounded(connecting);
      expect(transport.isConnected()).toBe(false);
      expect(connection.close).toHaveBeenCalledTimes(1);
      expect(connection.createConfirmChannel).not.toHaveBeenCalled();
    } finally {
      pending.resolve(connection);
      await bounded(connecting);
    }
  });

  it('discards a late initial confirm channel without announcing a connection', async () => {
    const pending = deferred<PublisherChannel>();
    const entered = deferred<void>();
    const connection = new BrokerConnection();
    connection.createConfirmChannel.mockImplementation(async () => {
      entered.resolve();
      return pending.promise;
    });
    const { transport } = unstarted(
      async () => connection as unknown as ChannelModel,
    );
    const connected = mock(() => {});
    transport.onConnected(connected);
    const connecting = observed(transport.connect());
    const channel = new PublisherChannel();
    try {
      await bounded(entered.promise);
      await bounded(transport.disconnect());
      pending.resolve(channel);
      await bounded(connecting);
      expect(transport.isConnected()).toBe(false);
      expect(connected).not.toHaveBeenCalled();
      expect(channel.close).toHaveBeenCalledTimes(1);
    } finally {
      pending.resolve(channel);
      await bounded(connecting);
    }
  });

  it('preserves the fresh connection when an older handshake finishes after restart', async () => {
    const pending = deferred<BrokerConnection>();
    const old = new BrokerConnection();
    const fresh = new BrokerConnection();
    const { transport, connect } = unstarted(
      async () => (await pending.promise) as unknown as ChannelModel,
    );
    // The first call remains the held handshake, followed by the fresh one.
    connect.mockImplementationOnce(
      async () => (await pending.promise) as unknown as ChannelModel,
    );
    connect.mockResolvedValueOnce(fresh as unknown as ChannelModel);
    const connected = mock(() => {});
    transport.onConnected(connected);
    const connecting = observed(transport.connect());
    try {
      await bounded(transport.disconnect());
      await bounded(transport.connect());
      pending.resolve(old);
      await bounded(connecting);
      expect(transport.isConnected()).toBe(true);
      expect(connected).toHaveBeenCalledTimes(1);
      expect(old.close).toHaveBeenCalledTimes(1);
      expect(old.createConfirmChannel).not.toHaveBeenCalled();
      expect(fresh.close).not.toHaveBeenCalled();
    } finally {
      pending.resolve(old);
      await bounded(connecting);
    }
  });

  it('preserves a fresh connection while an older disconnect finishes cleanup', async () => {
    const { transport, connection: old, connect } = await start();
    const entered = deferred<void>();
    const release = deferred<void>();
    old.close.mockImplementation(async () => {
      entered.resolve();
      await release.promise;
      old.emit('close');
    });
    const fresh = new BrokerConnection();
    connect.mockResolvedValue(fresh as unknown as ChannelModel);
    const stopping = transport.disconnect();
    void stopping.catch(() => {});
    let restarting: Promise<void> | undefined;
    try {
      await bounded(entered.promise);
      restarting = transport.connect();
      void restarting.catch(() => {});
      release.resolve();
      await bounded(Promise.all([stopping, restarting]));
      expect(transport.isConnected()).toBe(true);
      expect(fresh.close).not.toHaveBeenCalled();
      expect(fresh.createConfirmChannel).toHaveBeenCalledTimes(1);
    } finally {
      release.resolve();
      await bounded(stopping);
      if (restarting) await bounded(restarting);
    }
  });

  it('rejects only the returned send when concurrent sends share an envelope ID', async () => {
    await withTrackedTimers(async (pendingCount) => {
      const { transport, channel } = await start();
      const envelope = createDummyEnvelope({ tenantId: 'tenant-a' });
      const returned = observed(transport.send('missing', envelope));
      const delivered = observed(transport.send('work', envelope));
      await tick();
      const missing = channel.messages[0];
      const valid = channel.messages[1];
      if (!missing || !valid)
        throw new Error('Both publications must reach AMQP');
      channel.returnMessage(missing);
      missing.confirm(null);
      valid.confirm(null);
      expect(await returned).toBeInstanceOf(Error);
      expect(await delivered).toBe('rabbitmq');
      expect(missing.options.mandatory).toBe(true);
      expect(valid.options.messageId).toBe(missing.options.messageId);
      expect(pendingCount()).toBe(0);
    });
  });

  it('recovers a closed publisher channel without replacing the TCP connection', async () => {
    const { transport, connection, channel } = await start();
    const replacement = new PublisherChannel();
    connection.createConfirmChannel.mockResolvedValue(replacement);
    await channel.close();
    const first = observed(
      transport.send('work', createDummyEnvelope({ n: 1 })),
    );
    const second = observed(
      transport.send('work', createDummyEnvelope({ n: 2 })),
    );
    await tick();
    for (const message of replacement.messages) message.confirm(null);
    expect(await first).toBe('rabbitmq');
    expect(await second).toBe('rabbitmq');
    expect(replacement.messages).toHaveLength(2);
    expect(connection.createConfirmChannel).toHaveBeenCalledTimes(2);
    expect(connection.close).not.toHaveBeenCalled();
    expect(replacement.assertExchange).toHaveBeenCalled();
  });

  it('retries channel creation failure and shares the successful replacement', async () => {
    const { transport, channel, connection } = await start();
    const replacement = new PublisherChannel();
    connection.createConfirmChannel
      .mockRejectedValueOnce(new Error('channel unavailable'))
      .mockResolvedValue(replacement);
    await channel.close();
    await expect(
      transport.send('work', createDummyEnvelope({})),
    ).rejects.toThrow();
    const sends = [1, 2].map((n) =>
      observed(transport.send('work', createDummyEnvelope({ n }))),
    );
    await tick();
    for (const message of replacement.messages) message.confirm(null);
    expect(await Promise.all(sends)).toEqual(['rabbitmq', 'rabbitmq']);
    expect(connection.createConfirmChannel).toHaveBeenCalledTimes(3);
    expect(connection.close).not.toHaveBeenCalled();
  });

  it('does not accumulate return listeners across concurrent repeated publishes', async () => {
    await withTrackedTimers(async (pendingCount) => {
      const { transport, channel } = await start();
      for (let round = 0; round < 3; round++) {
        const offset = channel.messages.length;
        const sends = Array.from({ length: 16 }, () =>
          observed(transport.send('work', createDummyEnvelope({ round }))),
        );
        await tick();
        expect(channel.listenerCount('return')).toBeLessThanOrEqual(1);
        for (const message of channel.messages.slice(offset))
          message.confirm(null);
        expect(await Promise.all(sends)).toEqual(Array(16).fill('rabbitmq'));
        expect(pendingCount()).toBe(0);
      }
    });
  });

  it('preserves delayed exchange publishing without mandatory routing', async () => {
    const { transport, channel } = await start(true);
    const sent = observed(
      transport.send('work', createDummyEnvelope({}), { delay: 500 }),
    );
    await tick();
    const message = channel.messages[0];
    if (!message) throw new Error('Delayed publication must reach AMQP');
    message.confirm(null);
    expect(await sent).toBe('rabbitmq');
    expect(message.exchange).toContain('.delayed');
    expect(message.options.mandatory).not.toBe(true);
    expect(message.options.headers?.['x-delay']).toBe(500);
  });

  it.each(['nack', 'throw', 'timeout'] as const)(
    'settles %s and ignores its late outcomes for the next send',
    async (failure) => {
      await withTrackedTimers(async (pendingCount) => {
        const { transport, channel } = await start();
        if (failure === 'throw')
          channel.publish.mockImplementationOnce(() => {
            throw new Error('sync publish failed');
          });
        const failed = observed(
          transport.send('work', createDummyEnvelope({})),
        );
        await tick();
        const old = channel.messages[0];
        if (failure === 'nack') old?.confirm(new Error('broker nack'));
        expect(await bounded(failed)).toBeInstanceOf(Error);
        expect(pendingCount()).toBe(0);
        const listeners = channel.listenerCount('return');
        let settled = false;
        const next = observed(
          transport.send('work', createDummyEnvelope({})),
        ).then((value) => {
          settled = true;
          return value;
        });
        await tick();
        if (old) {
          channel.returnMessage(old);
          old.confirm(null);
        }
        await tick();
        expect(settled).toBe(false);
        channel.messages.at(-1)?.confirm(null);
        expect(await next).toBe('rabbitmq');
        expect(channel.listenerCount('return')).toBe(listeners);
        expect(pendingCount()).toBe(0);
      });
    },
  );

  it.each([false, true])(
    'releases a late replacement channel after shutdown (restart=%s)',
    async (restart) => {
      const { transport, channel, connection, connect } = await start();
      const pending = deferred<PublisherChannel>();
      const late = new PublisherChannel();
      connection.createConfirmChannel.mockReturnValueOnce(pending.promise);
      await channel.close();
      const oldSend = observed(transport.send('work', createDummyEnvelope({})));
      try {
        await tick();
        expect(connection.createConfirmChannel).toHaveBeenCalledTimes(2);
        await bounded(transport.disconnect());
        const fresh = new BrokerConnection();
        if (restart) {
          connect.mockResolvedValue(fresh as unknown as ChannelModel);
          await bounded(transport.connect());
        }
        pending.resolve(late);
        await bounded(oldSend);
        channel.emit('close');
        expect(late.close).toHaveBeenCalledTimes(1);
        expect(late.publish).not.toHaveBeenCalled();
        expect(transport.isConnected()).toBe(restart);
        if (restart) {
          const sent = observed(
            transport.send('work', createDummyEnvelope({})),
          );
          await tick();
          fresh.initial.messages[0]?.confirm(null);
          expect(await sent).toBe('rabbitmq');
          expect(fresh.close).not.toHaveBeenCalled();
        }
      } finally {
        pending.resolve(late);
        await bounded(oldSend);
      }
    },
  );

  it('does not acknowledge the original delivery after an unroutable DLQ publish', async () => {
    const { transport, channel } = await start();
    const delivery = new PublisherChannel();
    const receipt: MessageReceipt = {
      handle: { channel: delivery, msg: {} },
      sourceQueue: 'work',
      sourceTransport: 'rabbitmq',
      redelivered: false,
      attemptNumber: 1,
      deliveryCount: 1,
    };
    const envelope = createDummyEnvelope({ tenantId: 'tenant-a' });
    const failed = observed(
      transport.sendToDeadLetter(
        receipt,
        'unhandled',
        envelope,
        'handler failed',
      ),
    );
    await tick();
    const returned = channel.messages[0];
    if (!returned) throw new Error('DLQ publication must reach AMQP');
    channel.returnMessage(returned);
    returned.confirm(null);
    expect(await failed).toBeInstanceOf(Error);
    expect(delivery.ack).not.toHaveBeenCalled();
    const success = observed(
      transport.sendToDeadLetter(
        receipt,
        'unhandled',
        envelope,
        'handler failed',
      ),
    );
    await tick();
    channel.messages[1]?.confirm(null);
    expect(await success).toBeUndefined();
    expect(delivery.ack).toHaveBeenCalledTimes(1);
  });

  it('ignores returns and confirms from a previously closed channel', async () => {
    await withTrackedTimers(async (pendingCount) => {
      const { transport, channel, connection } = await start();
      const envelope = createDummyEnvelope({});
      const old = observed(transport.send('work', envelope));
      await tick();
      const oldMessage = channel.messages[0];
      if (!oldMessage) throw new Error('Original publication must reach AMQP');
      const replacement = new PublisherChannel();
      connection.createConfirmChannel.mockResolvedValue(replacement);
      await channel.close();
      expect(await old).toBeInstanceOf(Error);
      let settled = false;
      const next = observed(transport.send('work', envelope)).then((value) => {
        settled = true;
        return value;
      });
      await tick();
      channel.returnMessage(oldMessage);
      oldMessage.confirm(null);
      await tick();
      expect(settled).toBe(false);
      replacement.messages[0]?.confirm(null);
      expect(await next).toBe('rabbitmq');
      expect(pendingCount()).toBe(0);
    });
  });
});
