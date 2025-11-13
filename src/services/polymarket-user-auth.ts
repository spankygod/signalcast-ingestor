import axios, { AxiosInstance } from 'axios';

export interface UserChannelCredentials {
  accountId: string;
  apiKey: string;
  apiSecret: string;
}

export interface UserChannelAuthToken {
  token: string;
  expiresAt: number;
}

export interface UserChannelAuthProvider {
  fetchToken(credentials: UserChannelCredentials): Promise<UserChannelAuthToken>;
}

export interface HttpAuthProviderOptions {
  baseUrl?: string;
  endpoint?: string;
  timeoutMs?: number;
}

/**
 * Basic HTTP-based auth provider that calls the Polymarket `/wss-auth` endpoint (or a proxy)
 * to retrieve a short-lived websocket token for a given account.
 */
export function createHttpUserChannelAuthProvider(options: HttpAuthProviderOptions = {}): UserChannelAuthProvider {
  const {
    baseUrl = process.env.POLYMARKET_WSS_AUTH_BASE_URL || 'https://clob.polymarket.com',
    endpoint = process.env.POLYMARKET_WSS_AUTH_PATH || '/wss-auth',
    timeoutMs = Number(process.env.POLYMARKET_WSS_AUTH_TIMEOUT_MS || 10_000)
  } = options;

  const http: AxiosInstance = axios.create({
    baseURL: baseUrl,
    timeout: timeoutMs
  });

  return {
    async fetchToken(credentials: UserChannelCredentials): Promise<UserChannelAuthToken> {
      const response = await http.post(endpoint, {
        accountId: credentials.accountId,
        apiKey: credentials.apiKey,
        apiSecret: credentials.apiSecret
      });

      const data = response.data ?? {};
      const token: string | undefined = data.token || data.wssAuthToken || data.authToken;

      if (!token) {
        throw new Error('User-channel auth response did not contain a token');
      }

      const expiresAt = resolveExpiry(data);

      return { token, expiresAt };
    }
  };
}

function resolveExpiry(data: any): number {
  if (typeof data.expiresAt === 'string') {
    const parsed = Date.parse(data.expiresAt);
    if (!Number.isNaN(parsed)) return parsed;
  }

  if (typeof data.expiresAt === 'number') {
    return data.expiresAt;
  }

  const expiresInSeconds: number | undefined = data.expiresIn ?? data.ttl ?? 55;
  const ttlMs = Number.isFinite(expiresInSeconds) ? Number(expiresInSeconds) * 1000 : 55_000;
  return Date.now() + ttlMs;
}
