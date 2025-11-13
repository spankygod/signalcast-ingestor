import Redis from 'ioredis';
import dotenv from 'dotenv';
import { settings } from '../config/settings';

dotenv.config();

const redisHost = process.env.REDIS_HOST || 'localhost';
const redisPort = process.env.REDIS_PORT || '6379';
const redisPassword = process.env.REDIS_PASSWORD;

const redisUrl = redisPassword 
  ? `redis://:${redisPassword}@${redisHost}:${redisPort}`
  : `redis://${redisHost}:${redisPort}`;

let redisInstance: Redis | null = null;
let redisAvailable = false;

// Create a no-op Redis fallback for when Redis is not available
class NoOpRedis {
  async setex(): Promise<void> {
    // No-op when Redis is not available
  }

  async get(): Promise<string | null> {
    return null;
  }

  async del(): Promise<void> {
    // No-op when Redis is not available
  }

  async lpush(): Promise<void> {
    // No-op when Redis is not available
  }

  async rpop(): Promise<string | null> {
    return null;
  }

  async llen(): Promise<number> {
    return 0;
  }

  on(event: string, callback: (...args: any[]) => void): void {
    // No-op when Redis is not available
  }

  removeAllListeners(): void {
    // No-op
  }

  quit(): void {
    // No-op
  }
}

// Try to connect to Redis
async function initializeRedis(): Promise<void> {
  // Skip Redis initialization if disabled
  if (!settings.redisEnabled) {
    console.log('[redis] Redis is disabled via REDIS_ENABLED=false');
    redisAvailable = false;
    return;
  }

  try {
    redisInstance = new Redis(redisUrl, {
      maxRetriesPerRequest: 0,
      lazyConnect: true,
      connectTimeout: 2000
    });

    redisInstance.on('connect', () => {
      console.log('[redis] connected successfully');
      redisAvailable = true;
    });

    redisInstance.on('error', (error: Error) => {
      console.warn('[redis] connection failed, Redis will be disabled:', error.message);
      redisAvailable = false;
      redisInstance = null;
    });

    // Try to connect immediately
    await redisInstance.connect();
  } catch (error) {
    console.warn('[redis] creating client failed, Redis will be disabled');
    redisAvailable = false;
    redisInstance = null;
  }
}

// Initialize Redis asynchronously
initializeRedis();

// Create a Redis interface that falls back to no-op
const redisFallback = {
  async setex(key: string, ttl: number, value: string): Promise<void> {
    if (redisAvailable && redisInstance) {
      await redisInstance.setex(key, ttl, value);
    }
  },

  async get(key: string): Promise<string | null> {
    return redisAvailable && redisInstance ? await redisInstance.get(key) : null;
  },

  async del(key: string): Promise<void> {
    if (redisAvailable && redisInstance) {
      await redisInstance.del(key);
    }
  },

  async lpush(key: string, value: string): Promise<void> {
    if (redisAvailable && redisInstance) {
      await redisInstance.lpush(key, value);
    }
  },

  async rpop(key: string): Promise<string | null> {
    return redisAvailable && redisInstance ? await redisInstance.rpop(key) : null;
  },

  async llen(key: string): Promise<number> {
    return redisAvailable && redisInstance ? await redisInstance.llen(key) : 0;
  },

  on(event: string, callback: (...args: any[]) => void): void {
    if (redisInstance) {
      redisInstance.on(event, callback);
    }
  },

  removeAllListeners(): void {
    if (redisInstance) {
      redisInstance.removeAllListeners();
    }
  },

  quit(): void {
    if (redisInstance) {
      redisInstance.quit();
    }
  }
};

export const redis = redisFallback;
export { redisAvailable }; // Export availability status

export async function setCache(key: string, value: any, ttl = 3600): Promise<void> {
  await redis.setex(key, ttl, JSON.stringify(value));
}

export async function getCache<T = any>(key: string): Promise<T | null> {
  const value = await redis.get(key);
  return value ? (JSON.parse(value) as T) : null;
}

export async function delCache(key: string): Promise<void> {
  await redis.del(key);
}

export async function enqueue(queueName: string, payload: any): Promise<void> {
  await redis.lpush(queueName, JSON.stringify(payload));
}

export async function dequeue<T = any>(queueName: string): Promise<T | null> {
  const data = await redis.rpop(queueName);
  return data ? (JSON.parse(data) as T) : null;
}

export const closeRedis = () => redis.quit();
