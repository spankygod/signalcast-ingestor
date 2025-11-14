// src/lib/bootstrap.ts
import { redis } from "./redis";
import { BootstrapCoordinator } from "./bootstrap-coordinator";

const PREFIX = "bootstrap";

function key(name: string) {
  return `${PREFIX}:${name}`;
}

const STAGE_ALIAS: Record<string, "events" | "markets" | "outcomes"> = {
  events_done: "events",
  markets_done: "markets",
  outcomes_done: "outcomes",
};

function resolveStage(name: string): "events" | "markets" | "outcomes" | null {
  return STAGE_ALIAS[name] ?? null;
}

export const bootstrap = {
  async isDone(name: string): Promise<boolean> {
    const stage = resolveStage(name);
    if (stage) {
      return BootstrapCoordinator.isStageComplete(stage);
    }
    const v = await redis.get(key(name));
    return v === "1";
  },

  async setDone(name: string, processedCount = 0): Promise<void> {
    const stage = resolveStage(name);
    if (stage) {
      await BootstrapCoordinator.setStageBarrier(stage, processedCount);
      return;
    }
    await redis.set(key(name), "1");
  },

  async clearAll(): Promise<void> {
    const pattern = `${PREFIX}:*`;
    const keys = await redis.keys(pattern);
    if (keys.length) {
      for (const keyName of keys) {
        await redis.del(keyName);
      }
    }

    await Promise.all(
      Object.values(STAGE_ALIAS).map((stage) =>
        BootstrapCoordinator.clearStage(stage),
      ),
    );
  },
};
