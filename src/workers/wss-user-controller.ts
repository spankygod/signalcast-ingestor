import { createHttpUserChannelAuthProvider, UserChannelAuthProvider, UserChannelCredentials } from '../services/polymarket-user-auth';
import { UserChannelMessageHandler, UserChannelSubscription, WssUserChannelWorker } from './wss-user-channel';

export interface UserAccountConfig extends UserChannelCredentials {
  subscriptions?: UserChannelSubscription[];
  onMessage?: UserChannelMessageHandler;
}

export class WssUserChannelController {
  private readonly workers = new Map<string, WssUserChannelWorker>();
  private readonly authProvider: UserChannelAuthProvider;
  private readonly defaultSubscriptions: UserChannelSubscription[];
  private readonly defaultHandler?: UserChannelMessageHandler;

  constructor(options: {
    authProvider?: UserChannelAuthProvider;
    defaultSubscriptions?: UserChannelSubscription[];
    defaultHandler?: UserChannelMessageHandler;
  } = {}) {
    this.authProvider = options.authProvider || createHttpUserChannelAuthProvider();
    this.defaultSubscriptions = options.defaultSubscriptions || [];
    this.defaultHandler = options.defaultHandler;
  }

  ensureAccount(config: UserAccountConfig): WssUserChannelWorker {
    const { accountId } = config;
    const existing = this.workers.get(accountId);
    if (existing) {
      return existing;
    }

    const worker = new WssUserChannelWorker({
      credentials: config,
      authProvider: this.authProvider,
      subscriptions: config.subscriptions ?? this.defaultSubscriptions,
      onMessage: config.onMessage ?? this.defaultHandler
    });

    worker.start();
    this.workers.set(accountId, worker);
    return worker;
  }

  stopAccount(accountId: string): void {
    const worker = this.workers.get(accountId);
    if (!worker) return;
    worker.stop();
    this.workers.delete(accountId);
  }

  stopAll(): void {
    for (const worker of this.workers.values()) {
      worker.stop();
    }
    this.workers.clear();
  }

  listAccounts(): string[] {
    return [...this.workers.keys()];
  }
}

export const userChannelController = new WssUserChannelController();
