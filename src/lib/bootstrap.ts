// src/lib/bootstrap.ts
import { redis } from "./redis";

const PREFIX = "bootstrap";

function key(name: string) {
  return `${PREFIX}:${name}`;
}

export const bootstrap = {
  async isDone(name: string): Promise<boolean> {
    const v = await redis.get(key(name));
    return v === "1";
  },

  async setDone(name: string): Promise<void> {
    await redis.set(key(name), "1");
  },

  async clearAll(): Promise<void> {
    const pattern = `${PREFIX}:*`;
    const keys = await redis.keys(pattern);
    if (keys.length) {
      for (const key of keys) {
        await redis.del(key);
      }
    }
  },
};
