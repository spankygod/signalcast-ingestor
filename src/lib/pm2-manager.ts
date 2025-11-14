import pm2 from "pm2";

export class PM2Manager {
  private static instance: PM2Manager;
  private isConnected = false;
  private connectionPromise: Promise<void> | null = null;
  private operationQueue: Array<() => Promise<any>> = [];
  private isProcessingQueue = false;

  private constructor() {}

  static getInstance(): PM2Manager {
    if (!PM2Manager.instance) {
      PM2Manager.instance = new PM2Manager();
    }
    return PM2Manager.instance;
  }

  async ensureConnection(): Promise<void> {
    if (this.isConnected) {
      return;
    }

    if (this.connectionPromise) {
      return this.connectionPromise;
    }

    this.connectionPromise = new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        reject(new Error("PM2 connection timeout after 10 seconds"));
      }, 10000);

      pm2.connect((err) => {
        clearTimeout(timeout);
        if (err) {
          this.connectionPromise = null;
          reject(err);
        } else {
          this.isConnected = true;
          resolve();
        }
      });
    });

    return this.connectionPromise;
  }

  async disconnect(): Promise<void> {
    if (this.isConnected) {
      return new Promise((resolve) => {
        pm2.disconnect();
        this.isConnected = false;
        this.connectionPromise = null;
        resolve();
      });
    }
  }

  async executeWithTimeout<T>(
    operation: () => Promise<T>,
    timeoutMs: number = 8000,
    operationName: string = "PM2 operation"
  ): Promise<T> {
    return new Promise(async (resolve, reject) => {
      const timeout = setTimeout(() => {
        reject(new Error(`${operationName} timed out after ${timeoutMs}ms`));
      }, timeoutMs);

      try {
        const result = await operation();
        clearTimeout(timeout);
        resolve(result);
      } catch (error) {
        clearTimeout(timeout);
        reject(error);
      }
    });
  }

  async listProcesses(): Promise<any[]> {
    return this.executeWithTimeout(async () => {
      await this.ensureConnection();

      return new Promise((resolve, reject) => {
        pm2.list((err, list) => {
          if (err) {
            reject(err);
          } else {
            resolve(list);
          }
        });
      });
    }, 8000, "PM2 list operation");
  }

  async startProcess(config: any): Promise<void> {
    return this.executeWithTimeout(async () => {
      await this.ensureConnection();

      return new Promise((resolve, reject) => {
        pm2.start(config, (err) => {
          if (err) {
            reject(err);
          } else {
            resolve();
          }
        });
      });
    }, 10000, "PM2 start operation");
  }

  async deleteProcess(processName: string): Promise<void> {
    return this.executeWithTimeout(async () => {
      await this.ensureConnection();

      return new Promise((resolve, reject) => {
        pm2.delete(processName, (err) => {
          if (err) {
            reject(err);
          } else {
            resolve();
          }
        });
      });
    }, 8000, "PM2 delete operation");
  }

  async describeProcess(processName: string): Promise<any> {
    return this.executeWithTimeout(async () => {
      await this.ensureConnection();

      return new Promise((resolve, reject) => {
        pm2.describe(processName, (err, processDescription) => {
          if (err) {
            reject(err);
          } else {
            resolve(processDescription);
          }
        });
      });
    }, 8000, "PM2 describe operation");
  }

  async restartProcess(processName: string): Promise<void> {
    return this.executeWithTimeout(async () => {
      await this.ensureConnection();

      return new Promise((resolve, reject) => {
        pm2.restart(processName, (err) => {
          if (err) {
            reject(err);
          } else {
            resolve();
          }
        });
      });
    }, 10000, "PM2 restart operation");
  }

  // Health check method
  async healthCheck(): Promise<{ healthy: boolean; details: any }> {
    try {
      const processes = await this.listProcesses();
      return {
        healthy: true,
        details: {
          processCount: processes.length,
          connected: this.isConnected
        }
      };
    } catch (error) {
      return {
        healthy: false,
        details: {
          error: (error as Error).message,
          connected: this.isConnected
        }
      };
    }
  }
}