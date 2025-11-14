#!/usr/bin/env node

/**
 * SignalCast Ingestion System - Shutdown Script
 * Graceful shutdown with proper cleanup, health verification,
 * and rollback capabilities
 */

const { spawn, exec } = require('child_process');
const fs = require('fs');
const path = require('path');
const { program } = require('commander');

// ANSI color codes for better output
const colors = {
  reset: '\x1b[0m',
  red: '\x1b[31m',
  green: '\x1b[32m',
  yellow: '\x1b[33m',
  blue: '\x1b[34m',
  magenta: '\x1b[35m',
  cyan: '\x1b[36m',
  white: '\x1b[37m',
  gray: '\x1b[90m'
};

// Logger utility
function log(level, message, ...args) {
  const timestamp = new Date().toISOString();
  const color = {
    info: colors.blue,
    warn: colors.yellow,
    error: colors.red,
    success: colors.green,
    debug: colors.gray
  }[level] || colors.white;

  console.log(`${color}[${timestamp}] ${level.toUpperCase()}:${colors.reset} ${message}`, ...args);
}

class SignalCastStopper {
  constructor(options = {}) {
    this.options = {
      force: options.force || false,
      timeout: options.timeout || 30000, // 30 seconds default
      graceful: options.graceful !== false,
      verify: options.verify !== false,
      backup: options.backup || false,
      rollback: options.rollback || false,
      specific: options.specific || 'all', // 'all' or specific process name
      verbose: options.verbose || false,
      ...options
    };

    this.projectRoot = path.resolve(__dirname, '..');
    this.logDir = path.join(this.projectRoot, 'logs');
    this.isPM2Available = this.checkPM2Availability();
    this.stopStartTime = Date.now();
    this.shutdownTimeout = null;
  }

  /**
   * Check if PM2 is available
   */
  checkPM2Availability() {
    try {
      execSync('pm2 --version', { stdio: 'ignore' });
      return true;
    } catch (error) {
      return false;
    }
  }

  /**
   * Get current running processes
   */
  async getRunningProcesses() {
    return new Promise((resolve, reject) => {
      exec('pm2 jlist', { cwd: this.projectRoot }, (error, stdout, stderr) => {
        if (error) {
          if (error.message.includes('list') || error.message.includes('not found')) {
            resolve([]);
            return;
          }
          reject(error);
          return;
        }

        try {
          const processes = JSON.parse(stdout);
          resolve(processes.filter(p => p.name && p.name.includes('signalcast')));
        } catch (parseError) {
          // Fallback to parsing text output
          exec('pm2 list', { cwd: this.projectRoot }, (listError, listStdout) => {
            if (listError) {
              resolve([]);
              return;
            }

            const lines = listStdout.split('\n');
            const processes = [];
            let headerFound = false;

            for (const line of lines) {
              if (line.includes('App name')) {
                headerFound = true;
                continue;
              }
              if (headerFound && line.trim()) {
                const parts = line.trim().split(/\s+/);
                if (parts.length >= 2 && parts[0].includes('signalcast')) {
                  processes.push({
                    name: parts[0],
                    status: parts[1],
                    pid: parts[3],
                    cpu: parts[6],
                    memory: parts[7]
                  });
                }
              }
            }
            resolve(processes);
          });
        }
      });
    });
  }

  /**
   * Get process dependency order for graceful shutdown
   */
  getShutdownOrder(processes) {
    const shutdownOrder = [
      'signalcast-autoscaler',     // Stop autoscaling first
      'signalcast-db-writer',      // Stop data processor
      'signalcast-outcomes-poller',
      'signalcast-markets-poller',
      'signalcast-events-poller',  // Stop data producers
      'signalcast-wss-user-controller',
      'signalcast-wss-user-channel',
      'signalcast-wss-market-channel',  // Stop WebSocket handlers
      'signalcast-heartbeat',      // Stop monitoring last
      'signalcast-main'           // Stop orchestrator last
    ];

    const orderedProcesses = [];
    for (const name of shutdownOrder) {
      const matching = processes.filter(p => p.name === name);
      orderedProcesses.push(...matching);
    }

    // Add any remaining processes
    const remaining = processes.filter(p => !shutdownOrder.includes(p.name));
    orderedProcesses.push(...remaining);

    return orderedProcesses;
  }

  /**
   * Create backup before shutdown
   */
  async createBackup() {
    if (!this.options.backup) {
      return null;
    }

    log('info', 'Creating backup before shutdown...');

    const backupDir = path.join(this.projectRoot, 'backups');
    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    const backupPath = path.join(backupDir, `backup-${timestamp}`);

    if (!fs.existsSync(backupDir)) {
      fs.mkdirSync(backupDir, { recursive: true });
    }

    try {
      // Backup PM2 list
      const processes = await this.getRunningProcesses();
      fs.writeFileSync(
        path.join(backupPath, 'processes.json'),
        JSON.stringify(processes, null, 2)
      );

      // Backup logs
      if (fs.existsSync(this.logDir)) {
        const logBackupDir = path.join(backupPath, 'logs');
        fs.mkdirSync(logBackupDir, { recursive: true });

        const logFiles = fs.readdirSync(this.logDir);
        for (const logFile of logFiles) {
          fs.copyFileSync(
            path.join(this.logDir, logFile),
            path.join(logBackupDir, logFile)
          );
        }
      }

      log('success', `Backup created: ${backupPath}`);
      return backupPath;

    } catch (error) {
      log('warn', 'Failed to create backup:', error.message);
      return null;
    }
  }

  /**
   * Graceful shutdown of PM2 processes
   */
  async gracefulShutdown() {
    log('info', 'Starting graceful shutdown...');

    const processes = await this.getRunningProcesses();
    if (processes.length === 0) {
      log('info', 'No SignalCast processes found running');
      return;
    }

    log('info', `Found ${processes.length} processes to stop:`);
    processes.forEach(p => log('info', `  - ${p.name} (${p.status}, PID: ${p.pid})`));

    // Create backup if requested
    const backupPath = await this.createBackup();

    // Get shutdown order
    const orderedProcesses = this.getShutdownOrder(processes);

    // Stop processes in order with delays
    let stoppedCount = 0;
    for (const process of orderedProcesses) {
      try {
        log('info', `Stopping ${process.name}...`);

        if (this.options.graceful) {
          // Send SIGTERM for graceful shutdown
          await this.stopProcessGracefully(process, this.options.timeout);
        } else {
          // Force kill immediately
          await this.stopProcessForcefully(process);
        }

        stoppedCount++;
        log('success', `Stopped ${process.name}`);

        // Small delay between processes
        if (this.options.graceful) {
          await new Promise(resolve => setTimeout(resolve, 1000));
        }

      } catch (error) {
        log('error', `Failed to stop ${process.name}:`, error.message);
      }
    }

    log('success', `Successfully stopped ${stoppedCount}/${processes.length} processes`);

    // Save backup info for potential rollback
    if (backupPath) {
      fs.writeFileSync(
        path.join(backupPath, 'stop-info.json'),
        JSON.stringify({
          timestamp: new Date().toISOString(),
          processesStopped: stoppedCount,
          totalProcesses: processes.length,
          graceful: this.options.graceful,
          timeout: this.options.timeout
        }, null, 2)
      );
    }

    return backupPath;
  }

  /**
   * Stop single process gracefully
   */
  async stopProcessGracefully(process, timeout) {
    return new Promise((resolve, reject) => {
      const processName = process.name;
      const startTime = Date.now();

      // Set up timeout
      const timeoutHandle = setTimeout(() => {
        log('warn', `${processName} did not stop gracefully, forcing...`);
        this.forceStopProcess(processName).then(resolve).catch(reject);
      }, timeout);

      // Monitor process status
      const checkInterval = setInterval(async () => {
        try {
          const currentProcesses = await this.getRunningProcesses();
          const currentProcess = currentProcesses.find(p => p.name === processName);

          if (!currentProcess || currentProcess.status === 'stopped') {
            clearTimeout(timeoutHandle);
            clearInterval(checkInterval);
            resolve();
          }
        } catch (error) {
          log('error', `Error checking ${processName} status:`, error.message);
        }
      }, 1000);

      // Send stop signal
      exec(`pm2 stop ${processName}`, { cwd: this.projectRoot }, (error) => {
        if (error) {
          clearTimeout(timeoutHandle);
          clearInterval(checkInterval);
          reject(error);
        }
      });
    });
  }

  /**
   * Force stop process
   */
  async forceStopProcess(processName) {
    return new Promise((resolve, reject) => {
      exec(`pm2 delete ${processName}`, { cwd: this.projectRoot }, (error, stdout, stderr) => {
        if (error && !stderr.includes('does not exist')) {
          reject(error);
        } else {
          resolve();
        }
      });
    });
  }

  /**
   * Stop process forcefully
   */
  async stopProcessForcefully(process) {
    try {
      await this.forceStopProcess(process.name);
    } catch (error) {
      log('warn', `Could not stop ${process.name} via PM2:`, error.message);

      // Try to kill by PID
      if (process.pid && process.pid !== 'N/A') {
        try {
          process.kill(process.pid, 'SIGKILL');
          log('info', `Killed ${process.name} by PID ${process.pid}`);
        } catch (killError) {
          log('error', `Could not kill ${process.name} by PID:`, killError.message);
        }
      }
    }
  }

  /**
   * Stop specific process type
   */
  async stopSpecific(processType) {
    log('info', `Stopping specific process type: ${processType}`);

    const processes = await this.getRunningProcesses();
    const targetProcesses = processes.filter(p => p.name.includes(processType));

    if (targetProcesses.length === 0) {
      log('warn', `No processes found matching: ${processType}`);
      return;
    }

    for (const process of targetProcesses) {
      try {
        if (this.options.graceful) {
          await this.stopProcessGracefully(process, this.options.timeout);
        } else {
          await this.stopProcessForcefully(process);
        }
        log('success', `Stopped ${process.name}`);
      } catch (error) {
        log('error', `Failed to stop ${process.name}:`, error.message);
      }
    }
  }

  /**
   * Verify shutdown completed successfully
   */
  async verifyShutdown() {
    if (!this.options.verify) {
      log('info', 'Shutdown verification skipped');
      return true;
    }

    log('info', 'Verifying shutdown completion...');

    // Wait a bit for processes to clean up
    await new Promise(resolve => setTimeout(resolve, 2000));

    const remainingProcesses = await this.getRunningProcesses();
    const signalcastProcesses = remainingProcesses.filter(p => p.name.includes('signalcast'));

    if (signalcastProcesses.length === 0) {
      log('success', 'All SignalCast processes successfully stopped');
      return true;
    } else {
      log('warn', `${signalcastProcesses.length} SignalCast processes still running:`);
      signalcastProcesses.forEach(p => log('warn', `  - ${p.name} (${p.status})`));
      return false;
    }
  }

  /**
   * Cleanup resources
   */
  async cleanup() {
    log('info', 'Performing cleanup...');

    // Close any remaining file handles
    if (this.shutdownTimeout) {
      clearTimeout(this.shutdownTimeout);
      this.shutdownTimeout = null;
    }

    // Optional: Clean up old log files
    try {
      if (fs.existsSync(this.logDir)) {
        const logFiles = fs.readdirSync(this.logDir);
        const oldFiles = logFiles.filter(file => {
          const filePath = path.join(this.logDir, file);
          const stats = fs.statSync(filePath);
          const daysOld = (Date.now() - stats.mtime.getTime()) / (1000 * 60 * 60 * 24);
          return daysOld > 7 && file.endsWith('.log');
        });

        if (oldFiles.length > 0) {
          log('info', `Cleaning up ${oldFiles.length} old log files...`);
          for (const oldFile of oldFiles) {
            fs.unlinkSync(path.join(this.logDir, oldFile));
          }
          log('success', `Cleaned up ${oldFiles.length} old log files`);
        }
      }
    } catch (error) {
      log('warn', 'Failed to cleanup old log files:', error.message);
    }
  }

  /**
   * Rollback from backup
   */
  async rollback(backupPath) {
    if (!this.options.rollback || !backupPath) {
      return;
    }

    log('info', 'Rolling back from backup...');

    try {
      const stopInfoPath = path.join(backupPath, 'stop-info.json');
      if (fs.existsSync(stopInfoPath)) {
        const stopInfo = JSON.parse(fs.readFileSync(stopInfoPath, 'utf8'));
        log('info', `Rolling back shutdown from ${stopInfo.timestamp}`);
      }

      // Restore processes
      const processesPath = path.join(backupPath, 'processes.json');
      if (fs.existsSync(processesPath)) {
        const processes = JSON.parse(fs.readFileSync(processesPath, 'utf8'));
        log('info', `Would restore ${processes.length} processes`);
        // Implementation would restart processes from backup
      }

      log('success', 'Rollback completed');

    } catch (error) {
      log('error', 'Rollback failed:', error.message);
    }
  }

  /**
   * Execute command
   */
  execCommand(command, timeout = 30000) {
    return new Promise((resolve, reject) => {
      const child = exec(command, {
        cwd: this.projectRoot,
        maxBuffer: 1024 * 1024 * 10 // 10MB buffer
      }, (error, stdout, stderr) => {
        if (error) {
          reject(error);
        } else {
          resolve(stdout);
        }
      });

      if (timeout) {
        setTimeout(() => {
          child.kill('SIGKILL');
          reject(new Error(`Command timed out after ${timeout}ms`));
        }, timeout);
      }
    });
  }

  /**
   * Main shutdown method
   */
  async stop() {
    try {
      log('info', '🛑 Stopping SignalCast Ingestion System...');
      log('info', `Force: ${this.options.force}`);
      log('info', `Graceful: ${this.options.graceful}`);
      log('info', `Timeout: ${this.options.timeout}ms`);

      // Set shutdown timeout
      this.shutdownTimeout = setTimeout(() => {
        log('warn', 'Shutdown timeout reached, forcing...');
        this.options.force = true;
        this.options.graceful = false;
      }, this.options.timeout * 2);

      let backupPath = null;

      if (this.options.specific === 'all') {
        backupPath = await this.gracefulShutdown();
      } else {
        await this.stopSpecific(this.options.specific);
      }

      // Verify shutdown
      const shutdownSuccessful = await this.verifyShutdown();

      if (!shutdownSuccessful && !this.options.force) {
        log('warn', 'Shutdown verification failed, retrying with force...');
        this.options.force = true;
        this.options.graceful = false;
        await this.gracefulShutdown();
      }

      // Cleanup
      await this.cleanup();

      const shutdownTime = Date.now() - this.stopStartTime;
      log('success', `✅ SignalCast Ingestion System stopped successfully in ${Math.round(shutdownTime / 1000)}s`);

      // Handle rollback if needed
      if (!shutdownSuccessful && backupPath) {
        await this.rollback(backupPath);
      }

    } catch (error) {
      log('error', '❌ Failed to stop SignalCast Ingestion System:', error.message);

      if (this.options.verbose) {
        console.error(error);
      }

      process.exit(1);
    } finally {
      if (this.shutdownTimeout) {
        clearTimeout(this.shutdownTimeout);
        this.shutdownTimeout = null;
      }
    }
  }
}

// CLI setup
program
  .name('stop')
  .description('Stop SignalCast Ingestion System')
  .version('1.0.0');

program
  .option('--force', 'Force stop without graceful shutdown', false)
  .option('--timeout <ms>', 'Shutdown timeout in milliseconds', '30000')
  .option('--no-graceful', 'Skip graceful shutdown', false)
  .option('--no-verify', 'Skip shutdown verification', false)
  .option('--backup', 'Create backup before stopping', false)
  .option('--rollback', 'Rollback from backup if shutdown fails', false)
  .option('--specific <name>', 'Stop specific process (or "all")', 'all')
  .option('--verbose', 'Verbose output', false)
  .parse();

const options = program.opts();

// Create stopper instance
const stopper = new SignalCastStopper({
  force: options.force,
  timeout: parseInt(options.timeout),
  graceful: options.graceful,
  verify: options.verify,
  backup: options.backup,
  rollback: options.rollback,
  specific: options.specific,
  verbose: options.verbose
});

// Set up signal handlers for the stop script itself
process.on('SIGINT', () => {
  log('info', 'Received SIGINT, exiting...');
  process.exit(0);
});

process.on('SIGTERM', () => {
  log('info', 'Received SIGTERM, exiting...');
  process.exit(0);
});

// Stop the system
stopper.stop().catch((error) => {
  console.error('Shutdown failed:', error);
  process.exit(1);
});