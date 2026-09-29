/**
 * Opt-in recovery for failures while starting Matador.
 *
 * This controls startup orchestration, not message retries or a transport's
 * own reconnection policy. Omitting MatadorConfig.startupRetry preserves
 * fail-fast behavior when a startup transport operation rejects.
 */
export interface MatadorStartupRetryConfig {
  /**
   * Delay between failed startup operations, in milliseconds.
   * Must be finite and greater than zero. Retries end when startup succeeds
   * or stopReceiving()/shutdown() cancels startup.
   *
   * @default 1000
   */
  readonly retryIntervalMs?: number | undefined;
}
