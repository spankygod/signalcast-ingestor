import { enqueue, dequeue } from '../lib/redis';
import { QUEUES } from '../utils/constants';

export type UpdateKind = 'event' | 'market' | 'outcome' | 'tick';

export interface UpdateJob<TPayload = any> {
  kind: UpdateKind;
  payload: TPayload;
  queuedAt: string;
  attempts?: number;
}

const queueMap: Record<UpdateKind, string> = {
  event: QUEUES.events,
  market: QUEUES.markets,
  outcome: QUEUES.outcomes,
  tick: QUEUES.ticks
};

export async function pushUpdate<T>(
  kind: UpdateKind,
  payload: T,
  attempts = 0
): Promise<void> {
  const job: UpdateJob<T> = {
    kind,
    payload,
    queuedAt: new Date().toISOString(),
    attempts
  };
  await enqueue(queueMap[kind], job);
}

export async function pullNextUpdate<T>(kind: UpdateKind): Promise<UpdateJob<T> | null> {
  const job = await dequeue<UpdateJob<T>>(queueMap[kind]);
  if (!job) return null;
  return {
    ...job,
    attempts: job.attempts ?? 0
  };
}
