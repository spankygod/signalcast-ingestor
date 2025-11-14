import { redis } from "./redis";

export const bootstrap = {
  async isDone(key: string) {
    return (await redis.get(`bootstrap:${key}`)) === "1";
  },

  async setDone(key: string) {
    await redis.set(`bootstrap:${key}`, "1");
  },

  async reset() {
    await redis.del("bootstrap:events_done");
    await redis.del("bootstrap:markets_done");
  }
};