# Startup lifecycle contract

- Status: interface contract; runtime implementation and regression tests follow.
- Matador owns connection, topology, subscription recovery and late-resource cleanup. Callers use `start()`, `stopReceiving()` and `shutdown()`; they do not wrap individual Transport operations.

## Configuration

- `MatadorConfig.startupRetry?: MatadorStartupRetryConfig` opts into startup recovery. Omission keeps startup failures observable immediately when an underlying operation rejects; a transport's own reconnect policy remains independent.
- `startupRetry: {}` uses `retryIntervalMs: 1000`. The interval must be finite and greater than zero; invalid configuration fails before transport operations.
- Only rejected connection, topology and subscription operations are retried. Schema/configuration validation errors fail before startup and are never retried. Message processing and publish retries retain their existing policies.
- A retry waits asynchronously and is cancellable. There is no attempt limit while enabled; `start()` stays pending until ready, terminal validation failure or cancellation.

## Public lifecycle

| Operation | Contract |
|---|---|
| `start(): Promise<void>` | Concurrent calls join one startup operation. Resolve only after connection, topology and every requested subscription are ready. Calling again after readiness is a no-op. |
| `stopReceiving(): Promise<boolean>` | Prevent new subscription acquisition and cancel pending startup/retry. Return true for the first receiving-stop transition, including during startup; false before startup or after an existing stop. Keep an established transport available for in-flight handlers to send. |
| `shutdown(): Promise<void>` | Cancel pending startup/retry, stop receiving, drain owned in-flight handlers under existing shutdown timeout, then disconnect. Concurrent calls join cleanup. Also works before startup has reached readiness. |

- State progression is idle → starting → ready → receive-stopped → stopped, with starting also allowed to transition to receive-stopped/stopped. A shutdown instance is terminal; restarting requires a new instance. Repeated shutdown is safe.
- Cancellation rejects the pending `start()` promise with the existing `ShutdownInProgressError`. Late operation rejections are observed; they do not produce unhandled rejections or initiate retry.
- Calling `start()` after receiving has been stopped or shutdown has begun rejects with `ShutdownInProgressError`; it cannot silently restart consumers.
- With recovery disabled, the original transport rejection remains visible. The failed startup attempt does not leave subscriptions accumulating; a later explicit `start()` can retry if stop/shutdown has not begun.

## Partial startup and resource ownership

- Successful subscriptions belong to the current startup generation. Retry the failed/missing subscription without duplicating already active consumers; reconnect may recreate the owned set through the transport's existing recovery contract.
- `send()` can be used once connection and topology are ready, even while later queues are still subscribing. Existing subscribers can self-dispatch during this interval. This is not full startup readiness: callers requiring readiness await `start()`.
- The external application may impose a stricter send gate until its `start()` promise resolves. Matador must not break the existing early subscriber self-dispatch behavior to implement that gate.
- Stop/shutdown invalidates pending startup work before awaiting cleanup. A connection or subscription completing afterward cannot restore readiness, schedule retries or register a new active consumer. Matador releases the late resource.
- A pending non-cancellable Promise is not an owned cleanup barrier: stop/shutdown must not wait forever for it. Cleanup of resources already acquired is awaited; cleanup/rejection observation remains attached to late operations. Transport generation ownership prevents a late callback from clearing a newer resource.
- Graceful receiving stop uses `pauseForShutdown` when a subscription supplies it, otherwise `unsubscribe`. Existing LocalTransport/MultiTransport self-dispatch during drain remains supported; no new generic handler guard may discard such callbacks.
- Stop receiving does not by itself prohibit sending. Shutdown retains the existing drain-before-enqueue-stop ordering. Publisher confirm, return correlation, DLQ failure/ACK protection and consumer recovery remain transport responsibilities.

## Regression obligations

- Preserve existing start idempotence, self-dispatch during later subscription acquisition, LocalTransport/MultiTransport drain dispatch and external send after stopReceiving tests.
- Add coverage for concurrent startup, opt-in connect/topology/subscription retry, no duplicate partial subscriptions, default rejection, cancelled retry, stop/shutdown during each pending startup stage, late resource release and no resurrection.
- Exercise clean package exports and real RabbitMQ transport recovery in CI. This document records the contract, not passing runtime evidence.
