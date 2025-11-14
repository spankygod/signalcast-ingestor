/**
 * Redis client management for SignalCast ingestion system
 * Provides reconnection logic, stream management, consumer groups, and error handling
 */

import type { RedisClientType } from 'redis';
import {
  redisClient as externalClient,
  REDIS_STREAMS,
  REDIS_KEYS,
  STREAM_CONSUMER_CONFIG,
  STREAM_WRITER_CONFIG,
  REDIS_HEALTH_CHECK,
  closeRedisConnection,
  features
} from '../config';

/**
 * Redis stream message interface
 */
export interface StreamMessage {
  id: string;
  fields: Record<string, string>;
  stream: string;
}

/**
 * Redis consumer group info
 */
export interface ConsumerGroupInfo {
  name: string;
  lastDeliveredId: string;
  pendingCount: number;
  consumerCount: number;
}

/**
 * Redis stream info
 */
export interface StreamInfoExtended {
  name: string;
  length: number;
  radixTreeKeys: number;
  radixTreeNodes: number;
  lastGeneratedId: string;
  groups: ConsumerGroupInfo[];
  consumers: Array<{
    name: string;
    pendingCount: number;
    idleTime: number;
  }>;
}

/**
 * Redis connection status
 */
export interface RedisConnectionStatus {
  connected: boolean;
  ready: boolean;
  reconnecting: boolean;
  lastError?: string;
  lastConnectTime?: Date;
  uptime: number;
  info?: Record<string, string>;
}

/**
 * Redis metrics
 */
export interface RedisMetrics {
  memoryUsed: number;
  memoryMax: number;
  connectedClients: number;
  totalCommandsProcessed: number;
  opsPerSecond: number;
  keyspaceHits: number;
  keyspaceMisses: number;
  hitRate: number;
}

/**
 * Enhanced Redis client wrapper
 */
class EnhancedRedisClient {
  private client: RedisClientType;
  private isHealthy: boolean = false;
  private lastHealthCheck: Date = new Date(0);
  private healthCheckInterval: NodeJS.Timeout | null = null;
  private metricsInterval: NodeJS.Timeout | null = null;
  private currentMetrics: RedisMetrics = {
    memoryUsed: 0,
    memoryMax: 0,
    connectedClients: 0,
    totalCommandsProcessed: 0,
    opsPerSecond: 0,
    keyspaceHits: 0,
    keyspaceMisses: 0,
    hitRate: 0,
  };

  constructor(client: RedisClientType) {
    this.client = client;
    this.setupEventHandlers();
    this.startHealthChecking();
    this.startMetricsCollection();
  }

  /**
   * Get underlying Redis client
   */
  get getClient(): RedisClientType {
    return this.client;
  }

  /**
   * Check if client is ready
   */
  get isConnected(): boolean {
    return this.client.isOpen && this.isHealthy;
  }

  /**
   * Execute Redis command with error handling
   */
  async execute<T>(command: string, ...args: any[]): Promise<T> {
    try {
      const result = await this.client.sendCommand([command, ...args]);
      return result as T;
    } catch (error) {
      throw this.handleRedisError(error as Error, command, args);
    }
  }

  /**
   * Add message to stream
   */
  async addToStream(
    streamName: string,
    data: Record<string, string>,
    maxLength?: number
  ): Promise<string> {
    try {
      const options = maxLength
        ? {
            TRIM: {
              strategy: 'MAXLEN' as const,
              strategyModifier: '~' as const,
              threshold: maxLength,
            },
          }
        : undefined;

      return await this.client.xAdd(streamName, '*', data, options);
    } catch (error) {
      throw new RedisStreamError(
        `Failed to add message to stream ${streamName}: ${(error as Error).message}`,
        streamName
      );
    }
  }

  /**
   * Batch add messages to stream
   */
  async batchAddToStream(
    streamName: string,
    messages: Record<string, string>[],
    maxLength?: number
  ): Promise<string[]> {
    const messageIds: string[] = [];

    for (const message of messages) {
      try {
        const messageId = await this.addToStream(streamName, message, maxLength);
        messageIds.push(messageId);
      } catch (error) {
        // Continue with other messages but log the error
        console.error(`Failed to add message to stream ${streamName}:`, error);
      }
    }

    return messageIds;
  }

  /**
   * Read messages from consumer group
   */
  async readFromGroup(
    consumerGroup: string,
    consumerName: string,
    streams: string[],
    options: {
      count?: number;
      blockMs?: number;
      ids?: string[];
    } = {}
  ): Promise<StreamMessage[]> {
    const {
      count = STREAM_CONSUMER_CONFIG.COUNT_PER_READ,
      blockMs = STREAM_CONSUMER_CONFIG.BLOCK_TIMEOUT_MS,
      ids = streams.map(() => '>'),
    } = options;

    try {
      const streamEntries = streams.map((stream, index) => ({
        key: stream,
        id: ids[index] ?? '>',
      }));

      const readOptions = {
        COUNT: count,
        BLOCK: blockMs,
      };

      const results = await this.client.xReadGroup(
        consumerGroup,
        consumerName,
        streamEntries,
        readOptions
      );

      if (!results || (results as any).length === 0) {
        return [];
      }

      const messages: StreamMessage[] = [];
      for (const result of results as any) {
        for (const entry of result.messages || []) {
          messages.push({
            id: entry.id,
            fields: this.normalizeMessageFields(entry.message),
            stream: result.name,
          });
        }
      }

      return messages;
    } catch (error) {
      throw new RedisStreamError(
        `Failed to read from consumer group ${consumerGroup}: ${(error as Error).message}`,
        streams.join(',')
      );
    }
  }

  /**
   * Acknowledge processed messages
   */
  async acknowledgeMessages(
    streamName: string,
    consumerGroup: string,
    messageIds: string[]
  ): Promise<number> {
    if (messageIds.length === 0) {
      return 0;
    }

    try {
      const args: Array<string> = ['XACK', streamName, consumerGroup, ...messageIds];
      const acknowledgedCount = await this.client.sendCommand<number>(args);
      return acknowledgedCount ?? 0;
    } catch (error) {
      throw new RedisStreamError(
        `Failed to acknowledge messages: ${(error as Error).message}`,
        streamName
      );
    }
  }

  /**
   * Move message to dead letter queue
   */
  async moveToDeadletter(
    originalStream: string,
    messageId: string,
    error: string,
    metadata?: Record<string, string>
  ): Promise<string> {
    try {
      const deadletterData = {
        original_stream: originalStream,
        original_id: messageId,
        error,
        timestamp: new Date().toISOString(),
        worker_id: process.pid.toString(),
        ...metadata,
      };

      return await this.addToStream(
        REDIS_STREAMS.DEADLETTER,
        deadletterData,
        STREAM_WRITER_CONFIG.MAX_STREAM_LENGTH
      );
    } catch (err) {
      throw new RedisStreamError(
        `Failed to move message to deadletter: ${(err as Error).message}`,
        REDIS_STREAMS.DEADLETTER
      );
    }
  }

  /**
   * Get stream information
   */
  async getStreamInfo(streamName: string): Promise<StreamInfoExtended> {
    try {
      const [streamInfoRaw, groupInfoRaw] = await Promise.all([
        this.client.sendCommand<any[]>(['XINFO', 'STREAM', streamName]),
        this.client.sendCommand<any[]>(['XINFO', 'GROUPS', streamName]),
      ]);

      const streamInfo = this.parseRedisArray(streamInfoRaw);
      const length = Number(streamInfo['length'] ?? 0);
      const radixTreeKeys = Number(streamInfo['radix-tree-keys'] ?? 0);
      const radixTreeNodes = Number(streamInfo['radix-tree-nodes'] ?? 0);
      const lastGeneratedId = streamInfo['last-generated-id'] ?? '0-0';

      const groups = Array.isArray(groupInfoRaw)
        ? await Promise.all(
            groupInfoRaw.map(async (groupEntry) => {
              const group = this.parseRedisArray(groupEntry);
              const name = group['name'] ?? '';
              const consumersRaw = await this.client.sendCommand<any[]>([
                'XINFO',
                'CONSUMERS',
                streamName,
                name,
              ]);
              const consumers = Array.isArray(consumersRaw)
                ? consumersRaw.map((consumerEntry) => {
                    const consumer = this.parseRedisArray(consumerEntry);
                    return {
                      name: consumer['name'] ?? '',
                      pendingCount: Number(consumer['pending'] ?? 0),
                      idleTime: Number(consumer['idle'] ?? 0),
                    };
                  })
                : [];

              return {
                name,
                lastDeliveredId: group['last-delivered-id'] ?? '0-0',
                pendingCount: Number(group['pending'] ?? 0),
                consumerCount: Number(group['consumers'] ?? 0),
                consumers,
              };
            })
          )
        : [];

      return {
        name: streamName,
        length,
        radixTreeKeys,
        radixTreeNodes,
        lastGeneratedId,
        groups: groups.map((group) => ({
          name: group.name,
          lastDeliveredId: group.lastDeliveredId,
          pendingCount: group.pendingCount,
          consumerCount: group.consumerCount,
        })),
        consumers: groups.flatMap((group) => group.consumers),
      };
    } catch (error) {
      throw new RedisStreamError(
        `Failed to get stream info for ${streamName}: ${(error as Error).message}`,
        streamName
      );
    }
  }

  /**
   * Create consumer group
   */
  async createConsumerGroup(streamName: string, groupName: string, id: string = '0'): Promise<void> {
    try {
      await this.client.xGroupCreate(streamName, groupName, id, {
        MKSTREAM: true,
      });
    } catch (error: any) {
      if (error.code === 'BUSYGROUP') {
        // Consumer group already exists, which is fine
        if (features.debug) {
          console.log(`Consumer group ${groupName} already exists for stream ${streamName}`);
        }
      } else {
        throw new RedisStreamError(
          `Failed to create consumer group ${groupName}: ${error.message}`,
          streamName
        );
      }
    }
  }

  /**
   * Delete consumer group
   */
  async deleteConsumerGroup(streamName: string, groupName: string): Promise<void> {
    try {
      await this.client.xGroupDestroy(streamName, groupName);
    } catch (error) {
      throw new RedisStreamError(
        `Failed to delete consumer group ${groupName}: ${(error as Error).message}`,
        streamName
      );
    }
  }

  /**
   * Trim stream length
   */
  async trimStream(streamName: string, maxLength: number): Promise<number> {
    try {
      const result = await this.client.sendCommand<number>([
        'XTRIM',
        streamName,
        'MAXLEN',
        '~',
        maxLength.toString(),
      ]);
      return result ?? 0;
    } catch (error) {
      throw new RedisStreamError(
        `Failed to trim stream ${streamName}: ${(error as Error).message}`,
        streamName
      );
    }
  }

  /**
   * Set key with expiration
   */
  async setWithExpiry(key: string, value: string, ttlSeconds: number): Promise<void> {
    await this.client.setEx(key, ttlSeconds, value);
  }

  /**
   * Get key value
   */
  async get(key: string): Promise<string | null> {
    const result = await this.client.get(key);
    return result as string | null;
  }

  /**
   * Delete key
   */
  async delete(key: string): Promise<number> {
    return await this.client.del(key);
  }

  /**
   * Check if key exists
   */
  async exists(key: string): Promise<boolean> {
    return (await this.client.exists(key)) > 0;
  }

  /**
   * Increment counter
   */
  async increment(key: string, amount: number = 1): Promise<number> {
    return await this.client.incrBy(key, amount);
  }

  /**
   * Set hash field
   */
  async hSet(key: string, field: string, value: string): Promise<number> {
    return await this.client.hSet(key, field, value);
  }

  /**
   * Get hash field
   */
  async hGet(key: string, field: string): Promise<string | undefined> {
    const value = await this.client.hGet(key, field);
    return (value as string | undefined) ?? undefined;
  }

  /**
   * Get all hash fields
   */
  async hGetAll(key: string): Promise<Record<string, string>> {
    return await this.client.hGetAll(key);
  }

  /**
   * Acquire distributed lock
   */
  async acquireLock(
    lockKey: string,
    ttlSeconds: number,
    retryCount: number = 3
  ): Promise<string | null> {
    const lockValue = `${process.pid}-${Date.now()}-${Math.random()}`;

    for (let attempt = 0; attempt < retryCount; attempt++) {
      try {
        const result = await this.client.set(
          lockKey,
          lockValue,
          {
            NX: true, // Only set if key doesn't exist
            EX: ttlSeconds, // Set expiration
          }
        );

        if (result === 'OK') {
          return lockValue;
        }

        // Lock acquisition failed, wait before retry
        if (attempt < retryCount - 1) {
          await new Promise(resolve => setTimeout(resolve, 100 * (attempt + 1)));
        }
      } catch (error) {
        console.error(`Lock acquisition attempt ${attempt + 1} failed:`, error);
      }
    }

    return null;
  }

  /**
   * Release distributed lock
   */
  async releaseLock(lockKey: string, lockValue: string): Promise<boolean> {
    try {
      const script = `
        if redis.call("get", KEYS[1]) == ARGV[1] then
          return redis.call("del", KEYS[1])
        else
          return 0
        end
      `;

      const result = await this.client.eval(script, {
        keys: [lockKey],
        arguments: [lockValue],
      });

      return result === 1;
    } catch (error) {
      console.error('Lock release failed:', error);
      return false;
    }
  }

  /**
   * Perform health check
   */
  async healthCheck(): Promise<boolean> {
    if (!this.client.isOpen) {
      this.isHealthy = false;
      return false;
    }

    try {
      const startTime = Date.now();
      const response = await this.client.ping();
      const responseTime = Date.now() - startTime;

      this.isHealthy = response === 'PONG';
      this.lastHealthCheck = new Date();

      if (responseTime > REDIS_HEALTH_CHECK.TIMEOUT_MS / 2) {
        console.warn(`Redis health check slow: ${responseTime}ms`);
      }

      return this.isHealthy;
    } catch (error) {
      this.isHealthy = false;
      console.error('Redis health check failed:', error);
      return false;
    }
  }

  /**
   * Get connection status
   */
  async getConnectionStatus(): Promise<RedisConnectionStatus> {
    const status: RedisConnectionStatus = {
      connected: this.client.isOpen,
      ready: this.isHealthy,
      reconnecting: false,
      uptime: 0,
    };

    try {
      const info = await this.client.info();
      const infoMap = this.parseInfo(info);

      status.info = infoMap;
      status.uptime = parseInt(infoMap['uptime_in_seconds'] || '0') * 1000;
      status.lastConnectTime = this.lastHealthCheck;
    } catch (error) {
      status.lastError = (error as Error).message;
    }

    return status;
  }

  /**
   * Get current metrics
   */
  async getMetrics(): Promise<RedisMetrics> {
    try {
      const info = await this.client.info();
      const infoMap = this.parseInfo(info);

      this.currentMetrics = {
        memoryUsed: parseInt(infoMap['used_memory'] || '0'),
        memoryMax: parseInt(infoMap['maxmemory'] || '0'),
        connectedClients: parseInt(infoMap['connected_clients'] || '0'),
        totalCommandsProcessed: parseInt(infoMap['total_commands_processed'] || '0'),
        opsPerSecond: parseFloat(infoMap['instantaneous_ops_per_sec'] || '0'),
        keyspaceHits: parseInt(infoMap['keyspace_hits'] || '0'),
        keyspaceMisses: parseInt(infoMap['keyspace_misses'] || '0'),
        hitRate: this.calculateHitRate(
          parseInt(infoMap['keyspace_hits'] || '0'),
          parseInt(infoMap['keyspace_misses'] || '0')
        ),
      };

      return this.currentMetrics;
    } catch (error) {
      console.error('Failed to get Redis metrics:', error);
      return this.currentMetrics;
    }
  }

  /**
   * Close Redis connection
   */
  async close(): Promise<void> {
    this.stopHealthChecking();
    this.stopMetricsCollection();

    await closeRedisConnection();
  }

  /**
   * Setup event handlers
   */
  private setupEventHandlers(): void {
    this.client.on('connect', () => {
      console.log('Redis client connected');
    });

    this.client.on('ready', () => {
      console.log('Redis client ready for commands');
      this.isHealthy = true;
    });

    this.client.on('error', (err) => {
      console.error('Redis client error:', err);
      this.isHealthy = false;
    });

    this.client.on('reconnecting', () => {
      console.log('Redis client reconnecting...');
      this.isHealthy = false;
    });

    this.client.on('end', () => {
      console.log('Redis client connection ended');
      this.isHealthy = false;
      this.stopHealthChecking();
      this.stopMetricsCollection();
    });
  }

  /**
   * Start periodic health checking
   */
  private startHealthChecking(): void {
    this.stopHealthChecking();

    this.healthCheck().catch(() => {
      // Ignore initial health check failure
    });

    this.healthCheckInterval = setInterval(() => {
      this.healthCheck().catch(() => {
        // Health check failures are logged in the method
      });
    }, REDIS_HEALTH_CHECK.INTERVAL_MS);
  }

  /**
   * Start metrics collection
   */
  private startMetricsCollection(): void {
    this.stopMetricsCollection();

    this.metricsInterval = setInterval(() => {
      this.getMetrics().catch(() => {
        // Metrics collection failures are logged in the method
      });
    }, 60000); // Collect metrics every minute
  }

  /**
   * Calculate cache hit rate
   */
  private calculateHitRate(hits: number, misses: number): number {
    const total = hits + misses;
    return total > 0 ? (hits / total) * 100 : 0;
  }

  /**
   * Stop the periodic health check interval
   */
  private stopHealthChecking(): void {
    if (this.healthCheckInterval) {
      clearInterval(this.healthCheckInterval);
      this.healthCheckInterval = null;
    }
  }

  /**
   * Stop the metrics collection interval
   */
  private stopMetricsCollection(): void {
    if (this.metricsInterval) {
      clearInterval(this.metricsInterval);
      this.metricsInterval = null;
    }
  }

  /**
   * Normalize Redis stream message fields
   */
  private normalizeMessageFields(message: Record<string, any>): Record<string, string> {
    const normalized: Record<string, string> = {};
    for (const [key, value] of Object.entries(message)) {
      if (value === null || value === undefined) {
        normalized[key] = '';
      } else if (typeof value === 'string') {
        normalized[key] = value;
      } else if (typeof value === 'object' && Buffer.isBuffer(value)) {
        normalized[key] = value.toString();
      } else {
        normalized[key] = String(value);
      }
    }
    return normalized;
  }

  /**
   * Convert alternating key/value array to object
   */
  private parseRedisArray(entry: any): Record<string, any> {
    const result: Record<string, any> = {};
    if (!Array.isArray(entry)) {
      return result;
    }

    for (let i = 0; i < entry.length; i += 2) {
      const key = entry[i];
      const value = entry[i + 1];
      if (typeof key === 'string') {
        if (Array.isArray(value)) {
          result[key] = this.parseRedisArray(value);
        } else if (typeof value === 'object' && value !== null && 'toString' in value) {
          result[key] = Buffer.isBuffer(value) ? value.toString() : (value as any).toString();
        } else {
          result[key] = value;
        }
      }
    }

    return result;
  }

  /**
   * Parse INFO response into map
   */
  private parseInfo(info: string): Record<string, string> {
    const map: Record<string, string> = {};
    info
      .split('\n')
      .map(line => line.trim())
      .forEach(line => {
        if (!line || line.startsWith('#')) {
          return;
        }
        const separatorIndex = line.indexOf(':');
        if (separatorIndex === -1) {
          return;
        }
        const key = line.substring(0, separatorIndex);
        const value = line.substring(separatorIndex + 1);
        map[key] = value;
      });
    return map;
  }

  /**
   * Handle Redis errors
   */
  private handleRedisError(error: Error, command: string, args: any[]): Error {
    const errorMessage = error.message.toLowerCase();

    // Connection errors
    if (errorMessage.includes('connection') || errorMessage.includes('timeout')) {
      return new RedisConnectionError(error.message, command, args);
    }

    // Authentication errors
    if (errorMessage.includes('auth') || errorMessage.includes('password')) {
      return new RedisAuthError(error.message, command, args);
    }

    // Memory errors
    if (errorMessage.includes('memory')) {
      return new RedisMemoryError(error.message, command, args);
    }

    // Generic Redis error
    return new RedisCommandError(error.message, command, args);
  }
}

/**
 * Custom Redis error classes
 */
export class RedisError extends Error {
  public readonly command: string;
  public readonly args: any[];
  public readonly code: string = 'REDIS_ERROR';

  constructor(message: string, command: string, args: any[]) {
    super(`Redis error: ${message}`);
    this.name = 'RedisError';
    this.command = command;
    this.args = args;
  }
}

export class RedisConnectionError extends RedisError {
  public override readonly code: string = 'CONNECTION_ERROR';

  constructor(message: string, command: string, args: any[]) {
    super(`Connection error: ${message}`, command, args);
    this.name = 'RedisConnectionError';
  }
}

export class RedisAuthError extends RedisError {
  public override readonly code: string = 'AUTH_ERROR';

  constructor(message: string, command: string, args: any[]) {
    super(`Authentication error: ${message}`, command, args);
    this.name = 'RedisAuthError';
  }
}

export class RedisMemoryError extends RedisError {
  public override readonly code: string = 'MEMORY_ERROR';

  constructor(message: string, command: string, args: any[]) {
    super(`Memory error: ${message}`, command, args);
    this.name = 'RedisMemoryError';
  }
}

export class RedisCommandError extends RedisError {
  public override readonly code: string = 'COMMAND_ERROR';

  constructor(message: string, command: string, args: any[]) {
    super(`Command error: ${message}`, command, args);
    this.name = 'RedisCommandError';
  }
}

export class RedisStreamError extends RedisError {
  public override readonly code: string = 'STREAM_ERROR';
  public readonly streamName: string;

  constructor(message: string, streamName: string) {
    super(message, 'XREAD', [streamName]);
    this.name = 'RedisStreamError';
    this.streamName = streamName;
  }
}

/**
 * Create enhanced Redis client
 */
export function createRedisClient(): EnhancedRedisClient {
  return new EnhancedRedisClient(externalClient);
}

/**
 * Export EnhancedRedisClient class for direct usage
 */
export { EnhancedRedisClient };

/**
 * Global Redis client instance
 */
export const redisConnection = createRedisClient();

/**
 * Redis utility functions
 */
export const redisUtils = {
  /**
   * Build Redis key with prefix
   */
  buildKey(prefix: string, ...parts: string[]): string {
    return [prefix, ...parts].join(':');
  },

  /**
   * Create worker heartbeat key
   */
  getWorkerHeartbeatKey(workerType: string, pid: number): string {
    return redisUtils.buildKey(REDIS_KEYS.WORKER_HEARTBEAT, workerType, pid.toString());
  },

  /**
   * Create worker status key
   */
  getWorkerStatusKey(workerType: string, pid: number): string {
    return redisUtils.buildKey(REDIS_KEYS.WORKER_STATUS, workerType, pid.toString());
  },

  /**
   * Create lock key
   */
  getLockKey(lockName: string): string {
    return redisUtils.buildKey(REDIS_KEYS.LOCK, lockName);
  },

  /**
   * Create metrics key
   */
  getMetricsKey(metricName: string): string {
    return redisUtils.buildKey(REDIS_KEYS.METRICS, metricName);
  },

  /**
   * Create cache key
   */
  getCacheKey(cacheName: string, identifier: string): string {
    return redisUtils.buildKey(REDIS_KEYS.CACHE, cacheName, identifier);
  },

  /**
   * Increment a numeric counter atomically.
   */
  async increment(key: string, amount: number = 1): Promise<number> {
    return redisConnection.increment(key, amount);
  },

  /**
   * Persist structured metadata in Redis hashes.
   */
  async hSet(key: string, field: string, value: string): Promise<number> {
    return redisConnection.hSet(key, field, value);
  },

  /**
   * Retrieve cached string payloads.
   */
  async get(key: string): Promise<string | null> {
    return redisConnection.get(key);
  },

  /**
   * Set a short-lived cache value with TTL semantics.
   */
  async setWithExpiry(key: string, value: string, ttlSeconds: number): Promise<void> {
    await redisConnection.setWithExpiry(key, value, ttlSeconds);
  },

  /**
   * Append a structured payload to a Redis stream respecting trim policies.
   */
  async addToStream(
    streamName: string,
    data: Record<string, string>,
    maxLength: number = STREAM_WRITER_CONFIG.MAX_STREAM_LENGTH
  ): Promise<string> {
    return redisConnection.addToStream(streamName, data, maxLength);
  },

  /**
   * Parse stream message data
   */
  parseStreamMessage(message: StreamMessage): Record<string, any> {
    const parsed: Record<string, any> = { ...message.fields };

    // Parse numeric fields
    for (const [key, value] of Object.entries(parsed)) {
      if (/^\d+$/.test(value)) {
        parsed[key] = parseInt(value, 10);
      } else if (/^\d*\.\d+$/.test(value)) {
        parsed[key] = parseFloat(value);
      } else if (value === 'true' || value === 'false') {
        parsed[key] = value === 'true';
      }
    }

    return parsed;
  },

  /**
   * Validate stream data
   */
  validateStreamData(data: Record<string, string>): boolean {
    if (!data || typeof data !== 'object') {
      return false;
    }

    // Check for required timestamp field
    if (!data['timestamp']) {
      return false;
    }

    // Validate timestamp format
    const timestamp = new Date(data['timestamp']);
    if (isNaN(timestamp.getTime())) {
      return false;
    }

    return true;
  },

  /**
   * Create consumer name
   */
  createConsumerName(workerType: string): string {
    return `${workerType}-${process.pid}-${Date.now()}`;
  },

  /**
   * Calculate backoff delay for retries
   */
  calculateBackoffDelay(attempt: number, baseDelay: number = 1000, maxDelay: number = 30000): number {
    const delay = Math.min(baseDelay * Math.pow(2, attempt), maxDelay);
    // Add jitter
    return delay + Math.random() * 1000;
  },
};

/**
 * Export Redis connection and utilities
 */
export default redisConnection;
