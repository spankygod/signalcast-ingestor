/**
 * Polymarket Authentication Service
 * Handles user authentication, token management, and API access credentials
 */

import crypto from 'crypto';
import { Logger, LoggerFactory } from '../lib/logger';
import { retryUtils } from '../lib/retry';
import { POLYMARKET_CONFIG, AUTH_CONFIG, REQUEST_CONFIG, ERROR_HANDLING } from '../config/polymarket';

/**
 * Authentication token interface
 */
export interface AuthToken {
  accessToken: string;
  refreshToken?: string;
  tokenType: string;
  expiresAt: number;
  scope?: string[];
}

/**
 * API credentials interface
 */
export interface ApiCredentials {
  apiKey: string;
  secret: string;
  passphrase?: string;
}

/**
 * Signature data for HMAC authentication
 */
export interface SignatureData {
  timestamp: string;
  nonce: string;
  signature: string;
}

/**
 * User authentication response
 */
export interface AuthResponse {
  success: boolean;
  token?: AuthToken;
  error?: string;
  errorCode?: string;
}

/**
 * Authentication result interface
 */
export interface AuthResult {
  valid: boolean;
  authenticated?: boolean;
  token?: AuthToken;
  credentials?: ApiCredentials;
  expiresAt?: number;
  userId?: string;
  error?: string;
}

/**
 * Authentication service class
 */
export class PolymarketAuthService {
  private logger: Logger;
  private currentToken: AuthToken | null = null;
  private credentials: ApiCredentials;
  private tokenRefreshTimer: NodeJS.Timeout | null = null;

  constructor() {
    this.logger = LoggerFactory.getLogger('polymarket-auth', {
      category: 'API',
    });

    this.credentials = {
      apiKey: POLYMARKET_CONFIG.API_KEY,
      secret: POLYMARKET_CONFIG.SECRET,
      passphrase: POLYMARKET_CONFIG.PASSPHRASE,
    };

    this.logger.info('Polymarket authentication service initialized', {
      hasApiKey: !!this.credentials.apiKey,
      hasSecret: !!this.credentials.secret,
      hasPassphrase: !!this.credentials.passphrase,
    });
  }

  /**
   * Initialize authentication service
   */
  async initialize(): Promise<void> {
    this.logger.info('Initializing authentication service');

    try {
      // Validate credentials
      if (!this.validateCredentials()) {
        throw new Error('Invalid API credentials configuration');
      }

      // Attempt to authenticate if API key is available
      if (this.credentials.apiKey) {
        await this.authenticate();
      }

      this.logger.info('Authentication service initialized successfully');
    } catch (error) {
      this.logger.error('Failed to initialize authentication service', {
        error: error as Error,
      });
      throw error;
    }
  }

  /**
   * Authenticate with Polymarket API
   */
  async authenticate(): Promise<AuthResult> {
    return this.logger.timed('authenticate', this.logger.getLevel(), async () => {
      return await retryUtils.withExponentialBackoff(
        async () => {
          try {
            this.logger.debug('Attempting authentication');

            // For now, simulate authentication with API key
            // In real implementation, this would make API calls to get tokens
            const token: AuthToken = {
              accessToken: this.credentials.apiKey,
              tokenType: 'Bearer',
              expiresAt: Date.now() + (AUTH_CONFIG.JWT.EXPIRES_IN_SECONDS * 1000),
              scope: ['read', 'write'],
            };

            this.currentToken = token;
            this.scheduleTokenRefresh();

            this.logger.info('Authentication successful', {
              tokenType: token.tokenType,
              expiresAt: new Date(token.expiresAt).toISOString(),
              scope: token.scope,
            });

            return {
              authenticated: true,
              token,
              credentials: this.credentials,
              expiresAt: token.expiresAt,
            };
          } catch (error) {
            this.logger.error('Authentication failed', {
              error: error as Error,
            });

            return {
              authenticated: false,
              error: error instanceof Error ? error.message : 'Unknown authentication error',
            };
          }
        },
        ERROR_HANDLING.MAX_RETRIES.AUTHENTICATION,
        AUTH_CONFIG.JWT.REFRESH_THRESHOLD_SECONDS * 1000,
        30000, // maxDelay
        this.logger
      );
    });
  }

  /**
   * Get current authentication token
   */
  getCurrentToken(): AuthToken | null {
    return this.currentToken;
  }

  /**
   * Check if currently authenticated
   */
  isAuthenticated(): boolean {
    if (!this.currentToken) {
      return false;
    }

    // Check if token is expired
    return Date.now() < this.currentToken.expiresAt;
  }

  /**
   * Get authentication headers for API requests
   */
  getAuthHeaders(): Record<string, string> {
    const headers: Record<string, string> = {
      ...REQUEST_CONFIG.HEADERS,
    };

    if (this.currentToken) {
      headers['Authorization'] = `${this.currentToken.tokenType} ${this.currentToken.accessToken}`;
    }

    // Add signature if credentials are available
    if (this.credentials.secret) {
      const signature = this.generateSignature();
      headers[AUTH_CONFIG.SIGNATURE.TIMESTAMP_HEADER] = signature.timestamp;
      headers[AUTH_CONFIG.SIGNATURE.SIGNATURE_HEADER] = signature.signature;
      headers[AUTH_CONFIG.SIGNATURE.NONCE_HEADER] = signature.nonce;
    }

    return headers;
  }

  /**
   * Refresh authentication token
   */
  async refreshToken(): Promise<AuthResult> {
    this.logger.info('Refreshing authentication token');

    try {
      // In a real implementation, this would use the refresh token
      // For now, we'll re-authenticate
      return await this.authenticate();
    } catch (error) {
      this.logger.error('Failed to refresh token', {
        error: error as Error,
      });
      throw error;
    }
  }

  /**
   * Generate HMAC signature for API requests
   */
  generateSignature(method: string = 'GET', path: string = '/', body: string = ''): SignatureData {
    const timestamp = Date.now().toString();
    const nonce = crypto.randomBytes(16).toString('hex');

    // Create message to sign
    const message = `${timestamp}${method}${path}${body}${nonce}`;

    // Generate signature
    const signature = crypto
      .createHmac(AUTH_CONFIG.SIGNATURE.ALGORITHM, this.credentials.secret)
      .update(message)
      .digest('hex');

    return {
      timestamp,
      nonce,
      signature,
    };
  }

  /**
   * Validate API credentials
   */
  private validateCredentials(): boolean {
    if (!this.credentials.apiKey) {
      this.logger.error('API key is missing');
      return false;
    }

    if (!this.credentials.secret) {
      this.logger.error('API secret is missing');
      return false;
    }

    return true;
  }

  /**
   * Schedule automatic token refresh
   */
  private scheduleTokenRefresh(): void {
    if (this.tokenRefreshTimer) {
      clearTimeout(this.tokenRefreshTimer);
    }

    if (!this.currentToken) {
      return;
    }

    const refreshTime = this.currentToken.expiresAt - (AUTH_CONFIG.JWT.REFRESH_THRESHOLD_SECONDS * 1000);
    const delayMs = Math.max(0, refreshTime - Date.now());

    this.logger.debug('Scheduling token refresh', {
      refreshAt: new Date(refreshTime).toISOString(),
      delayMs,
    });

    this.tokenRefreshTimer = setTimeout(async () => {
      try {
        await this.refreshToken();
      } catch (error) {
        this.logger.error('Automatic token refresh failed', {
          error: error as Error,
        });
      }
    }, delayMs);
  }

  /**
   * Handle authentication errors
   */
  handleAuthError(error: any): boolean {
    const errorMessage = error?.message?.toLowerCase() || '';
    const errorCode = error?.code || '';

    // Check if error is authentication-related
    const isAuthError =
      errorMessage.includes('unauthorized') ||
      errorMessage.includes('forbidden') ||
      errorMessage.includes('invalid token') ||
      errorMessage.includes('expired') ||
      errorCode === '401' ||
      errorCode === '403';

    if (isAuthError) {
      this.logger.warn('Authentication error detected', {
        error,
        errorCode,
        errorMessage,
      });

      // Clear current token and attempt re-authentication
      this.currentToken = null;

      // Schedule re-authentication
      setTimeout(() => {
        this.authenticate().catch(authError => {
          this.logger.error('Re-authentication failed', {
            error: authError,
          });
        });
      }, 5000);

      return true;
    }

    return false;
  }

  /**
   * Verify JWT token
   */
  async verifyToken(token: string): Promise<AuthResult> {
    try {
      this.logger.debug('Verifying token', { tokenLength: token.length });

      // In a real implementation, this would verify the JWT signature and claims
      // For now, we'll simulate token verification
      if (!token || token.length < 10) {
        return {
          valid: false,
          error: 'Invalid token format',
        };
      }

      // Simulate successful verification
      return {
        valid: true,
        authenticated: true,
        userId: 'user_' + Math.random().toString(36).substr(2, 9),
        expiresAt: Date.now() + (AUTH_CONFIG.JWT.EXPIRES_IN_SECONDS * 1000),
      };
    } catch (error) {
      this.logger.error('Token verification failed', {
        error: error as Error,
      });
      return {
        valid: false,
        error: error instanceof Error ? error.message : 'Token verification failed',
      };
    }
  }

  /**
   * Verify API key
   */
  async verifyApiKey(apiKey: string): Promise<AuthResult> {
    try {
      this.logger.debug('Verifying API key', { apiKeyLength: apiKey.length });

      if (!apiKey || apiKey.length < 10) {
        return {
          valid: false,
          error: 'Invalid API key format',
        };
      }

      // Check if API key matches our configured key
      if (apiKey === this.credentials.apiKey) {
        return {
          valid: true,
          authenticated: true,
          userId: 'api_user_' + Math.random().toString(36).substr(2, 9),
          credentials: this.credentials,
        };
      }

      return {
        valid: false,
        error: 'Invalid API key',
      };
    } catch (error) {
      this.logger.error('API key verification failed', {
        error: error as Error,
      });
      return {
        valid: false,
        error: error instanceof Error ? error.message : 'API key verification failed',
      };
    }
  }

  /**
   * Verify HMAC signature
   */
  async verifySignature(signature: string): Promise<AuthResult> {
    try {
      this.logger.debug('Verifying signature', { signatureLength: signature.length });

      if (!signature || signature.length < 10) {
        return {
          valid: false,
          error: 'Invalid signature format',
        };
      }

      // In a real implementation, this would verify the HMAC signature
      // For now, we'll simulate signature verification
      const expectedSignature = this.generateSignature();

      // Simple validation - in production this would be proper HMAC verification
      if (signature.length === expectedSignature.signature.length) {
        return {
          valid: true,
          authenticated: true,
          userId: 'sig_user_' + Math.random().toString(36).substr(2, 9),
          credentials: this.credentials,
        };
      }

      return {
        valid: false,
        error: 'Invalid signature',
      };
    } catch (error) {
      this.logger.error('Signature verification failed', {
        error: error as Error,
      });
      return {
        valid: false,
        error: error instanceof Error ? error.message : 'Signature verification failed',
      };
    }
  }

  /**
   * Get authentication status
   */
  getAuthStatus(): {
    authenticated: boolean;
    tokenExpiresAt?: string;
    timeUntilExpiry?: number;
    hasCredentials: boolean;
  } {
    const authenticated = this.isAuthenticated();

    return {
      authenticated,
      tokenExpiresAt: this.currentToken ? new Date(this.currentToken.expiresAt).toISOString() : undefined,
      timeUntilExpiry: this.currentToken ? this.currentToken.expiresAt - Date.now() : undefined,
      hasCredentials: !!(this.credentials.apiKey && this.credentials.secret),
    };
  }

  /**
   * Cleanup resources
   */
  cleanup(): void {
    if (this.tokenRefreshTimer) {
      clearTimeout(this.tokenRefreshTimer);
      this.tokenRefreshTimer = null;
    }

    this.currentToken = null;
    this.logger.info('Authentication service cleaned up');
  }
}

/**
 * Singleton instance for the authentication service
 */
let authInstance: PolymarketAuthService | null = null;

/**
 * Get or create authentication service instance
 */
export function getAuthService(): PolymarketAuthService {
  if (!authInstance) {
    authInstance = new PolymarketAuthService();
  }
  return authInstance;
}

/**
 * Initialize authentication service
 */
export async function initializeAuth(): Promise<PolymarketAuthService> {
  const service = getAuthService();
  await service.initialize();
  return service;
}

/**
 * Authentication middleware for API requests
 */
export function createAuthMiddleware(authService: PolymarketAuthService) {
  return async (req: any, res: any, next: any) => {
    try {
      if (!authService.isAuthenticated()) {
        await authService.authenticate();
      }

      // Add auth headers to request
      req.headers = {
        ...req.headers,
        ...authService.getAuthHeaders(),
      };

      next();
    } catch (error) {
      res.status(401).json({
        error: 'Authentication failed',
        message: error instanceof Error ? error.message : 'Unknown error',
      });
    }
  };
}

/**
 * Export types and utilities
 */
export type {
  AuthToken,
  ApiCredentials,
  SignatureData,
  AuthResponse,
  AuthResult,
};

export default PolymarketAuthService;