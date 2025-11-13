import WebSocket from 'ws';
import logger from '../lib/logger';
import { polymarketConfig } from '../config/polymarket';
import { sleep } from '../lib/retry';
import { heartbeatMonitor } from './heartbeat';
import { WORKERS } from '../utils/constants';
import {
  UserChannelAuthProvider,
  UserChannelAuthToken,
  UserChannelCredentials
} from '../services/polymarket-user-auth';

export interface UserChannelSubscription {
  type: string;
  payload?: Record<string, unknown>;
}

export interface UserChannelMessageHandler {
  (accountId: string, message: unknown): Promise<void> | void;
}

export interface WssUserChannelOptions {
  credentials: UserChannelCredentials;
  authProvider: UserChannelAuthProvider;
  subscriptions?: UserChannelSubscription[];
  onMessage?: UserChannelMessageHandler;
}

export class WssUserChannelWorker {
  private readonly workerId: string;
  private readonly subscriptions: UserChannelSubscription[];
  private readonly messageHandler?: UserChannelMessageHandler;
  private readonly authProvider: UserChannelAuthProvider;
  private readonly credentials: UserChannelCredentials;
  private readonly userPath: string;

  private socket: WebSocket | null = null;
  private reconnectDelay = 5_000;
  private shouldRun = false;
  private token: UserChannelAuthToken | null = null;
  private tokenTimer: NodeJS.Timeout | null = null;

  constructor(options: WssUserChannelOptions) {
    this.credentials = options.credentials;
    this.authProvider = options.authProvider;
    this.subscriptions = options.subscriptions || [];
    this.messageHandler = options.onMessage;
    this.workerId = `${WORKERS.userChannel}:${this.credentials.accountId}`;
    this.userPath = process.env.POLYMARKET_USER_WS_PATH || '/user';
  }

  start(): void {
    if (this.shouldRun) return;
    this.shouldRun = true;
    heartbeatMonitor.beat(this.workerId, { state: 'connecting' });
    void this.connect();
  }

  stop(): void {
    this.shouldRun = false;
    this.clearTokenTimer();
    if (this.socket) {
      this.socket.removeAllListeners();
      this.socket.close();
      this.socket = null;
    }
    heartbeatMonitor.markIdle(this.workerId, { state: 'stopped' });
  }

  private async connect(): Promise<void> {
    if (!this.shouldRun) return;

    try {
      const token = await this.ensureToken();
      const url = `${polymarketConfig.wsBaseUrl}${this.userPath}`;
      logger.info(`${this.workerId} connecting`, { url });

      this.socket = new WebSocket(url, {
        headers: {
          Authorization: `Bearer ${token.token}`
        }
      });

      this.socket.on('open', () => this.handleOpen());
      this.socket.on('message', (raw) => void this.handleMessage(raw));
      this.socket.on('error', (error) => this.handleError(error));
      this.socket.on('close', (code, reason) => void this.handleClose(code, reason));
    } catch (error) {
      logger.error(`${this.workerId} failed to establish connection`, { error });
      await this.scheduleReconnect();
    }
  }

  private handleOpen(): void {
    logger.info(`${this.workerId} connected`);
    this.reconnectDelay = 5_000;
    heartbeatMonitor.beat(this.workerId, { state: 'connected' });
    this.flushSubscriptions();
  }

  private async handleMessage(raw: WebSocket.RawData): Promise<void> {
    try {
      const payload = parseJson(raw.toString());
      heartbeatMonitor.beat(this.workerId, { state: 'message' });

      if (this.messageHandler) {
        await this.messageHandler(this.credentials.accountId, payload);
        return;
      }

      console.log(`[${this.workerId}]`, 'message', payload);
    } catch (error) {
      logger.warn(`${this.workerId} failed to handle message`, { error });
    }
  }

  private handleError(error: Error): void {
    logger.error(`${this.workerId} socket error`, { error });
  }

  private async handleClose(code: number, reason: Buffer): Promise<void> {
    logger.warn(`${this.workerId} connection closed`, {
      code,
      reason: reason.toString()
    });
    heartbeatMonitor.markIdle(this.workerId, { state: 'disconnected' });
    this.socket = null;
    if (!this.shouldRun) return;
    await this.scheduleReconnect();
  }

  private async scheduleReconnect(): Promise<void> {
    await sleep(this.reconnectDelay);
    this.reconnectDelay = Math.min(this.reconnectDelay * 2, 60_000);
    heartbeatMonitor.beat(this.workerId, { state: 'reconnecting' });
    await this.connect();
  }

  private async ensureToken(force = false): Promise<UserChannelAuthToken> {
    if (!force && this.token && this.token.expiresAt - Date.now() > 5_000) {
      return this.token;
    }

    this.token = await this.authProvider.fetchToken(this.credentials);
    this.scheduleTokenRefresh();
    return this.token;
  }

  private scheduleTokenRefresh(): void {
    this.clearTokenTimer();
    if (!this.token) return;

    const refreshIn = Math.max(this.token.expiresAt - Date.now() - 5_000, 5_000);
    this.tokenTimer = setTimeout(() => {
      this.token = null;
      if (this.socket) {
        logger.info(`${this.workerId} refreshing token, restarting socket`);
        this.socket.terminate();
      } else if (this.shouldRun) {
        void this.connect();
      }
    }, refreshIn);
  }

  private flushSubscriptions(): void {
    if (!this.socket || this.socket.readyState !== WebSocket.OPEN) return;
    for (const subscription of this.subscriptions) {
      const message = JSON.stringify(subscription);
      this.socket.send(message);
    }
  }

  private clearTokenTimer(): void {
    if (this.tokenTimer) {
      clearTimeout(this.tokenTimer);
      this.tokenTimer = null;
    }
  }
}

function parseJson(value: string): unknown {
  try {
    return JSON.parse(value);
  } catch {
    return value;
  }
}
