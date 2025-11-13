import { enqueue, dequeue } from '../lib/redis';
import { QUEUES } from '../utils/constants';

export type UpdateKind = 'event' | 'market' | 'outcome' | 'tick';

export interface UpdateJob<TPayload = any> {
  kind: UpdateKind;
  payload: TPayload;
  queuedAt: string;
}

export async function pushUpdate<T>(kind: UpdateKind, payload: T): Promise<void> {
  const job: UpdateJob<T> = {
    kind,
    payload,
    queuedAt: new Date().toISOString()
  };
  await enqueue(QUEUES.updates, job);
}

export async function pullNextUpdate<T>(): Promise<UpdateJob<T> | null> {
  return dequeue<UpdateJob<T>>(QUEUES.updates);
}
