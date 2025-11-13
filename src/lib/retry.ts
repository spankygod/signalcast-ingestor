export interface RetryOptions {
  maxAttempts?: number;
  baseDelay?: number;
  maxDelay?: number;
  factor?: number;
}

export async function retryWithBackoff<T>(
  operation: () => Promise<T>,
  options: RetryOptions = {}
): Promise<T> {
  const {
    maxAttempts = 3,
    baseDelay = 1000,
    maxDelay = 30_000,
    factor = 2
  } = options;

  let lastError: Error | null = null;

  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      return await operation();
    } catch (error) {
      lastError = error as Error;

      if (attempt === maxAttempts) {
        throw lastError;
      }

      const delay = Math.min(baseDelay * Math.pow(factor, attempt - 1), maxDelay);
      console.warn(`[retry] attempt ${attempt} failed, retrying in ${delay}ms`, lastError.message);
      await sleep(delay);
    }
  }

  throw lastError ?? new Error('retryWithBackoff failed without capturing error');
}

export function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}
