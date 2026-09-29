import { afterAll, afterEach, beforeAll, describe, expect, it } from 'bun:test';
import { randomUUID } from 'node:crypto';
import {
  RabbitMQContainer,
  type StartedRabbitMQContainer,
} from '@testcontainers/rabbitmq';
import amqplib, { type Channel } from 'amqplib';
import {
  applyPrefix,
  resolveTargetQueueName,
  TopologyBuilder,
} from '../../src/topology/index.js';
import type { MessageReceipt } from '../../src/transport/transport.js';
import { RabbitMQTransport } from '../../src/transport/rabbitmq/rabbitmq-transport.js';
import { createDummyEnvelope } from '../../src/types/index.js';

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

async function within<T>(promise: Promise<T>): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_, reject) => {
        timer = setTimeout(
          () => reject(new Error('Broker recovery did not complete')),
          10_000,
        );
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

const channelOf = (receipt: MessageReceipt) =>
  (receipt.handle as { channel: Channel }).channel;

describe('RabbitMQ recovery against a real broker', () => {
  let container: StartedRabbitMQContainer;
  let url: string;
  const closers: (() => Promise<unknown>)[] = [];

  beforeAll(async () => {
    container = await new RabbitMQContainer('rabbitmq:3.13-management').start();
    url = container.getAmqpUrl();
  }, 120_000);

  afterEach(async () => {
    const results = await Promise.allSettled(
      closers
        .splice(0)
        .reverse()
        .map((close) => close()),
    );
    const failures = results.filter((result) => result.status === 'rejected');
    if (failures.length)
      throw new AggregateError(
        failures.map((result) => result.reason),
        'Broker fixture cleanup failed',
      );
  });

  afterAll(async () => {
    if (container) await container.stop();
  });

  async function fixture() {
    const namespace = `recovery-${randomUUID()}`;
    const topology = TopologyBuilder.create()
      .withNamespace(namespace)
      .addQueue('work', { concurrency: 1 })
      .addQueue('independent', { concurrency: 1 })
      .build();
    const transport = new RabbitMQTransport({
      url,
      connectionName: namespace,
      enableDelayedMessages: false,
      connection: {
        maxReconnectAttempts: 3,
        initialReconnectDelay: 100,
        maxReconnectDelay: 500,
      },
    });
    closers.push(() => transport.disconnect());
    const adminConnection = await amqplib.connect(url);
    closers.push(() => adminConnection.close());
    const admin = await adminConnection.createChannel();
    await transport.connect();
    await transport.applyTopology(topology);
    return {
      transport,
      admin,
      topology,
      work: resolveTargetQueueName(topology, 'work'),
      independent: resolveTargetQueueName(topology, 'independent'),
      exchange: applyPrefix(topology.prefix, `${namespace}.exchange`),
    };
  }

  it('recovers publisher-only closure while preserving the consumer and its unACKed delivery', async () => {
    const { transport, admin, work, exchange } = await fixture();
    const original = createDummyEnvelope({ id: 'original' });
    const later = createDummyEnvelope({ id: 'later' });
    const first = deferred<MessageReceipt>();
    const next = deferred<MessageReceipt>();
    const release = deferred<void>();
    const deliveries: string[] = [];
    let consumerClosed = false;
    try {
      await transport.subscribe(
        work,
        async (envelope, receipt) => {
          deliveries.push(envelope.id);
          if (envelope.id === original.id) {
            first.resolve(receipt);
            await release.promise;
            await transport.complete(receipt);
          } else {
            await transport.complete(receipt);
            next.resolve(receipt);
          }
        },
        { concurrency: 1 },
      );
      await transport.send(work, original);
      const receipt = await within(first.promise);
      const consumer = channelOf(receipt);
      consumer.once('close', () => {
        consumerClosed = true;
      });
      await admin.deleteExchange(exchange);
      await expect(
        transport.send(work, createDummyEnvelope({})),
      ).rejects.toThrow();
      expect(transport.isConnected()).toBe(true);
      expect(consumerClosed).toBe(false);
      await admin.assertExchange(exchange, 'direct', { durable: true });
      await admin.bindQueue(work, exchange, work);
      await expect(transport.send(work, later)).resolves.toBe('rabbitmq');
      expect(await admin.checkQueue(work)).toMatchObject({
        messageCount: 1,
        consumerCount: 1,
      });
      expect(deliveries).toEqual([original.id]);
      release.resolve();
      const subsequent = await within(next.promise);
      expect(channelOf(subsequent)).toBe(consumer);
      expect(subsequent.redelivered).toBe(false);
      expect(consumerClosed).toBe(false);
      expect(deliveries).toEqual([original.id, later.id]);
    } finally {
      release.resolve();
    }
  }, 30_000);

  it('rejects a returned publish despite broker ACK and sends after binding restoration', async () => {
    const { transport, admin, work, exchange } = await fixture();
    await admin.unbindQueue(work, exchange, work);
    await expect(
      transport.send(work, createDummyEnvelope({ id: 'missing' })),
    ).rejects.toThrow();
    expect((await admin.checkQueue(work)).messageCount).toBe(0);
    await admin.bindQueue(work, exchange, work);
    const delivered = deferred<string>();
    const envelope = createDummyEnvelope({ id: 'restored' });
    await transport.subscribe(work, async (received, receipt) => {
      await transport.complete(receipt);
      delivered.resolve(received.id);
    });
    await transport.send(work, envelope);
    expect(await within(delivered.promise)).toBe(envelope.id);
  }, 30_000);

  it('recovers only the failed consumer channel and keeps old ACKs away from the redelivery', async () => {
    const { transport, admin, work, independent } = await fixture();
    const original = createDummyEnvelope({ id: 'original' });
    const later = createDummyEnvelope({ id: 'later' });
    const first = deferred<MessageReceipt>();
    const redelivered = deferred<MessageReceipt>();
    const oldFinished = deferred<void>();
    const next = deferred<void>();
    const releaseOld = deferred<void>();
    const releaseNew = deferred<void>();
    const independentFirst = deferred<MessageReceipt>();
    const independentNext = deferred<MessageReceipt>();
    const deliveries: string[] = [];
    let independentCount = 0;
    try {
      await transport.subscribe(
        work,
        async (envelope, receipt) => {
          deliveries.push(envelope.id);
          if (envelope.id === later.id) {
            await transport.complete(receipt);
            next.resolve();
          } else if (receipt.redelivered) {
            redelivered.resolve(receipt);
            await releaseNew.promise;
            await transport.complete(receipt);
          } else {
            first.resolve(receipt);
            await releaseOld.promise;
            await transport.complete(receipt);
            oldFinished.resolve();
          }
        },
        { concurrency: 1 },
      );
      await transport.subscribe(independent, async (_, receipt) => {
        await transport.complete(receipt);
        (independentCount++ === 0 ? independentFirst : independentNext).resolve(
          receipt,
        );
      });
      await transport.send(independent, createDummyEnvelope({}));
      const untouched = channelOf(await within(independentFirst.promise));
      await transport.send(work, original);
      const oldChannel = channelOf(await within(first.promise));
      // Fail before forcing close if old code lacks an error listener: avoid an
      // uncaught EventEmitter error masking the intended consumer-recovery RED.
      expect(oldChannel.listenerCount('error')).toBeGreaterThan(0);
      await expect(
        oldChannel.checkQueue(`missing-${randomUUID()}`),
      ).rejects.toThrow();
      const recovered = await within(redelivered.promise);
      const newChannel = channelOf(recovered);
      expect(newChannel).not.toBe(oldChannel);
      expect(newChannel.connection).toBe(oldChannel.connection);
      await transport.send(independent, createDummyEnvelope({}));
      expect(channelOf(await within(independentNext.promise))).toBe(untouched);
      releaseOld.resolve();
      await within(oldFinished.promise);
      await transport.send(work, later);
      expect(await admin.checkQueue(work)).toMatchObject({
        messageCount: 1,
        consumerCount: 1,
      });
      expect(deliveries).toEqual([original.id, original.id]);
      releaseNew.resolve();
      await within(next.promise);
      expect(deliveries).toEqual([original.id, original.id, later.id]);
    } finally {
      releaseOld.resolve();
      releaseNew.resolve();
    }
  }, 30_000);

  it.each(['unsubscribe', 'disconnect'] as const)(
    'does not revive a consumer when %s races with channel recovery',
    async (operation) => {
      const { transport, admin, work } = await fixture();
      const first = deferred<MessageReceipt>();
      const release = deferred<void>();
      const stopped = deferred<void>();
      let stopFailure: unknown;
      let calls = 0;
      try {
        const subscription = await transport.subscribe(
          work,
          async (_, receipt) => {
            calls++;
            first.resolve(receipt);
            await release.promise;
            await transport.complete(receipt);
          },
          { concurrency: 1 },
        );
        await transport.send(work, createDummyEnvelope({}));
        const channel = channelOf(await within(first.promise));
        expect(channel.listenerCount('error')).toBeGreaterThan(0);
        channel.once('close', () => {
          const stopping =
            operation === 'unsubscribe'
              ? subscription.unsubscribe()
              : transport.disconnect();
          void stopping.then(
            () => stopped.resolve(),
            (error: unknown) => {
              stopFailure = error;
              stopped.resolve();
            },
          );
        });
        await expect(
          channel.checkQueue(`missing-${randomUUID()}`),
        ).rejects.toThrow();
        await within(stopped.promise);
        expect(stopFailure).toBeUndefined();
        release.resolve();
        if (operation === 'unsubscribe')
          expect(subscription.isActive).toBe(false);
        for (let round = 0; round < 16; round++) {
          await new Promise((resolve) => setTimeout(resolve, 100));
          expect(await admin.checkQueue(work)).toMatchObject({
            consumerCount: 0,
            messageCount: 1,
          });
          if (operation === 'disconnect')
            expect(transport.isConnected()).toBe(false);
        }
        expect(calls).toBe(1);
      } finally {
        release.resolve();
      }
    },
    30_000,
  );

  it('isolates unfinished work and its ACK after basic.cancel replaces the consumer', async () => {
    const { transport, admin, topology, work } = await fixture();
    const original = createDummyEnvelope({ id: 'original' });
    const subsequent = createDummyEnvelope({ id: 'replacement' });
    const last = createDummyEnvelope({ id: 'last' });
    const first = deferred<MessageReceipt>();
    const recovered = deferred<MessageReceipt>();
    const releaseOld = deferred<void>();
    const releaseNew = deferred<void>();
    const oldFinished = deferred<void>();
    const nextFinished = deferred<void>();
    const cancelled = deferred<void>();
    const closed = deferred<void>();
    const calls: string[] = [];
    try {
      await transport.subscribe(
        work,
        async (envelope, receipt) => {
          calls.push(envelope.id);
          if (envelope.id === original.id) {
            first.resolve(receipt);
            await releaseOld.promise;
            await transport.complete(receipt);
            oldFinished.resolve();
          } else if (envelope.id === subsequent.id) {
            recovered.resolve(receipt);
            await releaseNew.promise;
            await transport.complete(receipt);
          } else {
            await transport.complete(receipt);
            nextFinished.resolve();
          }
        },
        { concurrency: 1 },
      );
      await transport.send(work, original);
      const originalChannel = channelOf(await within(first.promise));
      expect(originalChannel.listenerCount('error')).toBeGreaterThan(0);
      originalChannel.once('cancel', () => cancelled.resolve());
      originalChannel.once('close', () => closed.resolve());
      await admin.deleteQueue(work);
      await within(cancelled.promise);
      await within(closed.promise);
      await transport.applyTopology(topology);
      await transport.send(work, subsequent);
      const replacementChannel = channelOf(await within(recovered.promise));
      expect(replacementChannel).not.toBe(originalChannel);
      expect(replacementChannel.connection).toBe(originalChannel.connection);
      releaseOld.resolve();
      await within(oldFinished.promise);
      await transport.send(work, last);
      expect(await admin.checkQueue(work)).toMatchObject({
        consumerCount: 1,
        messageCount: 1,
      });
      expect(calls).toEqual([original.id, subsequent.id]);
      releaseNew.resolve();
      await within(nextFinished.promise);
      expect(calls).toEqual([original.id, subsequent.id, last.id]);
    } finally {
      releaseOld.resolve();
      releaseNew.resolve();
    }
  }, 30_000);
});
