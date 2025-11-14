/**
 * Queue system exports for SignalCast ingestion
 * Provides centralized access to all queue implementations and utilities
 */

// Core types and interfaces
export * from './types';

// Base queue implementation
export { BaseQueue } from './base-queue';

// Individual queue implementations
export { EventsQueue, EventsQueueProcessor } from './events.queue';
export { MarketsQueue, MarketsQueueProcessor } from './markets.queue';
export { OutcomesQueue, OutcomesQueueProcessor } from './outcomes.queue';
export { TicksQueue, TicksQueueProcessor } from './ticks.queue';
export { UpdatesQueue, UpdatesQueueProcessor } from './updates.queue';
export { DeadletterQueue, DeadletterQueueProcessor } from './dead-letter.queue';

// Re-export commonly used imports for convenience
export {
  DEFAULT_QUEUE_CONFIGS,
  queueMessageGuards,
  QueueProcessingStatus,
  QueueConfig,
  BatchProcessingConfig,
} from './types';