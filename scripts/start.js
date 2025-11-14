#!/usr/bin/env node

/**
 * SignalCast Ingestion System - Startup Script
 * Production-ready startup with environment detection, health checks,
 * and graceful error handling
 */

const { spawn, exec } = require('child_process');
const fs = require('fs');
const path = require('path');
const os = require('os');
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

class SignalCastStarter {
  constructor(options = {}) {
    this.options = {
      mode: options.mode || 'production',
      environment: options.environment || process.env.NODE_ENV || 'development',
      workers: options.workers || 'all',
      watch: options.watch || false,
      verbose: options.verbose || false,
      healthCheck: options.healthCheck !== false,
      timeout: options.timeout || 60000, // 60 seconds default timeout
      maxRetries: options.maxRetries || 3,
      ...options
    };

    this.projectRoot = path.resolve(__dirname, '..');
    this.pm2ConfigPath = path.join(this.projectRoot, 'ecosystem.config.js');
    this.logDir = path.join(this.projectRoot, 'logs');
    this.isPM2Available = this.checkPM2Availability();
    this.startupStartTime = Date.now();
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
   * Ensure required directories exist
   */
  ensureDirectories() {
    const dirs = [this.logDir];
    dirs.forEach(dir => {
      if (!fs.existsSync(dir)) {
        fs.mkdirSync(dir, { recursive: true });
        log('info', `Created directory: ${dir}`);
      }
    });
  }

  /**
   * Validate environment and configuration
   */
  async validateEnvironment() {
    log('info', 'Validating environment and configuration...');

    // Check required files
    const requiredFiles = [
      this.pm2ConfigPath,
      path.join(this.projectRoot, 'package.json'),
      path.join(this.projectRoot, 'src/index.ts'),
      path.join(this.projectRoot, 'tsconfig.json')
    ];

    for (const file of requiredFiles) {
      if (!fs.existsSync(file)) {
        throw new Error(`Required file not found: ${file}`);
      }
    }

    // Check environment variables
    const requiredEnvVars = ['NODE_ENV'];
    if (this.options.environment === 'production') {
      requiredEnvVars.push('DATABASE_URL', 'REDIS_URL');
    }

    const missingVars = requiredEnvVars.filter(varName => !process.env[varName]);
    if (missingVars.length > 0) {
      log('warn', `Missing environment variables: ${missingVars.join(', ')}`);
      if (this.options.environment === 'production') {
        throw new Error(`Missing required environment variables: ${missingVars.join(', ')}`);
      }
    }

    log('success', 'Environment validation completed');
  }

  /**
   * Check if application is already running
   */
  async checkExistingProcesses() {
    return new Promise((resolve, reject) => {
      exec('pm2 list', { cwd: this.projectRoot }, (error, stdout, stderr) => {
        if (error) {
          // PM2 not running, which is fine
          resolve([]);
          return;
        }

        const lines = stdout.split('\n');
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
                pid: parts[3]
              });
            }
          }
        }

        resolve(processes);
      });
    });
  }

  /**
   * Install dependencies if needed
   */
  async installDependencies() {
    if (!fs.existsSync(path.join(this.projectRoot, 'node_modules'))) {
      log('info', 'Installing dependencies...');
      await this.runCommand('npm install', this.options.timeout * 2);
      log('success', 'Dependencies installed');
    }
  }

  /**
   * Build TypeScript project
   */
  async buildProject() {
    if (this.options.environment === 'production' || !this.options.watch) {
      log('info', 'Building TypeScript project...');
      await this.runCommand('npm run build', this.options.timeout);
      log('success', 'Project built successfully');
    }
  }

  /**
   * Start PM2 processes
   */
  async startPM2() {
    if (!this.isPM2Available) {
      throw new Error('PM2 is not available. Please install it with: npm install -g pm2');
    }

    const env = this.options.environment === 'development' ? 'development' :
                this.options.environment === 'staging' ? 'staging' : 'production';

    const pm2Command = `pm2 start ${this.pm2ConfigPath} --env ${env}`;

    if (this.options.watch) {
      log('info', 'Starting PM2 in development mode with watch...');
    } else {
      log('info', `Starting PM2 in ${env} mode...`);
    }

    try {
      await this.runCommand(pm2Command, this.options.timeout);
      log('success', 'PM2 processes started successfully');
    } catch (error) {
      log('error', 'Failed to start PM2 processes:', error.message);
      throw error;
    }
  }

  /**
   * Start specific worker or application mode
   */
  async startSpecificMode() {
    const nodeCommand = 'node';
    const scriptPath = path.join(this.projectRoot, 'src/index.ts');
    const args = [scriptPath];

    // Add mode-specific arguments
    switch (this.options.mode) {
      case 'orchestrator':
        args.push('--mode', 'orchestrator');
        break;
      case 'standalone':
        args.push('--standalone');
        break;
      case 'development':
        args.push('--development');
        break;
      case 'worker':
        if (!this.options.workerType) {
          throw new Error('Worker type required when starting in worker mode');
        }
        args.push('--worker', this.options.workerType);
        break;
      default:
        throw new Error(`Unknown mode: ${this.options.mode}`);
    }

    const command = `${nodeCommand} ${args.join(' ')}`;
    log('info', `Starting in ${this.options.mode} mode...`);
    log('info', `Command: ${command}`);

    // Start the process
    const process = spawn(nodeCommand, args.slice(1), {
      stdio: 'inherit',
      cwd: this.projectRoot,
      env: {
        ...process.env,
        NODE_ENV: this.options.environment
      }
    });

    return new Promise((resolve, reject) => {
      process.on('error', (error) => {
        log('error', 'Process error:', error.message);
        reject(error);
      });

      process.on('exit', (code, signal) => {
        if (code === 0) {
          log('success', 'Process exited successfully');
          resolve(0);
        } else {
          log('error', `Process exited with code ${code} and signal ${signal}`);
          reject(new Error(`Process failed with exit code ${code}`));
        }
      });

      // Handle parent process signals
      process.on('SIGINT', () => {
        log('info', 'Received SIGINT, shutting down gracefully...');
        process.kill('SIGINT');
      });

      process.on('SIGTERM', () => {
        log('info', 'Received SIGTERM, shutting down gracefully...');
        process.kill('SIGTERM');
      });
    });
  }

  /**
   * Perform health check after startup
   */
  async performHealthCheck() {
    if (!this.options.healthCheck) {
      log('info', 'Health check disabled');
      return;
    }

    log('info', 'Performing post-startup health check...');

    const healthCheckEndpoints = [
      { name: 'Main Orchestrator', url: 'http://localhost:3000/health' },
      { name: 'WebSocket Controller', url: 'http://localhost:8082/health' }
    ];

    const healthResults = [];

    for (const endpoint of healthCheckEndpoints) {
      try {
        const response = await fetch(endpoint.url, {
          timeout: 5000,
          signal: AbortSignal.timeout(5000)
        });

        if (response.ok) {
          healthResults.push({ name: endpoint.name, status: 'healthy' });
          log('success', `${endpoint.name}: Healthy`);
        } else {
          healthResults.push({ name: endpoint.name, status: 'unhealthy', error: `HTTP ${response.status}` });
          log('warn', `${endpoint.name}: HTTP ${response.status}`);
        }
      } catch (error) {
        healthResults.push({ name: endpoint.name, status: 'error', error: error.message });
        log('warn', `${endpoint.name}: ${error.message}`);
      }
    }

    const healthyCount = healthResults.filter(r => r.status === 'healthy').length;
    const totalChecks = healthResults.length;

    if (healthyCount === totalChecks) {
      log('success', `All ${totalChecks} health checks passed`);
    } else {
      log('warn', `${healthyCount}/${totalChecks} health checks passed`);
    }

    return healthResults;
  }

  /**
   * Show status after startup
   */
  async showStatus() {
    log('info', 'Fetching current status...');

    try {
      const stdout = await this.execCommand('pm2 status');
      log('info', 'Current PM2 status:');
      console.log(stdout);
    } catch (error) {
      log('warn', 'Could not fetch PM2 status:', error.message);
    }

    const startupTime = Date.now() - this.startupStartTime;
    log('success', `Startup completed in ${Math.round(startupTime / 1000)}s`);
  }

  /**
   * Execute command and return promise
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
   * Run command with retries
   */
  async runCommand(command, timeout = 30000) {
    let lastError;

    for (let attempt = 1; attempt <= this.options.maxRetries; attempt++) {
      try {
        log('info', `Running command (attempt ${attempt}/${this.options.maxRetries}): ${command}`);
        const result = await this.execCommand(command, timeout);
        if (this.options.verbose) {
          console.log(result);
        }
        return result;
      } catch (error) {
        lastError = error;
        log('warn', `Command failed (attempt ${attempt}/${this.options.maxRetries}): ${error.message}`);

        if (attempt < this.options.maxRetries) {
          const delay = attempt * 1000; // Exponential backoff
          log('info', `Retrying in ${delay}ms...`);
          await new Promise(resolve => setTimeout(resolve, delay));
        }
      }
    }

    throw lastError;
  }

  /**
   * Main startup method
   */
  async start() {
    try {
      log('info', '🚀 Starting SignalCast Ingestion System...');
      log('info', `Mode: ${this.options.mode}`);
      log('info', `Environment: ${this.options.environment}`);

      // Pre-startup checks
      this.ensureDirectories();
      await this.validateEnvironment();
      await this.installDependencies();

      // Check existing processes
      if (this.options.mode !== 'worker') {
        const existingProcesses = await this.checkExistingProcesses();
        if (existingProcesses.length > 0) {
          log('warn', `Found existing processes: ${existingProcesses.map(p => p.name).join(', ')}`);
          if (this.options.environment === 'production') {
            log('warn', 'Stopping existing processes before starting new ones...');
            await this.runCommand('pm2 delete all', 10000);
          }
        }
      }

      // Build project
      await this.buildProject();

      // Start based on mode
      if (this.options.mode === 'worker') {
        await this.startSpecificMode();
      } else {
        await this.startPM2();

        // Wait a bit for processes to start
        await new Promise(resolve => setTimeout(resolve, 3000));

        await this.performHealthCheck();
        await this.showStatus();
      }

      log('success', '✅ SignalCast Ingestion System started successfully!');

      if (this.options.mode !== 'worker') {
        log('info', '💡 Use "npm run status" to check running processes');
        log('info', '💡 Use "npm run logs" to view logs');
        log('info', '💡 Use "npm run stop" to stop the system');
      }

    } catch (error) {
      log('error', '❌ Failed to start SignalCast Ingestion System:', error.message);

      if (this.options.verbose) {
        console.error(error);
      }

      process.exit(1);
    }
  }
}

// CLI setup
program
  .name('start')
  .description('Start SignalCast Ingestion System')
  .version('1.0.0');

program
  .option('--mode <mode>', 'Startup mode', 'orchestrator')
  .option('--environment <env>', 'Environment (development|staging|production)', 'development')
  .option('--worker-type <type>', 'Worker type (when mode=worker)')
  .option('--watch', 'Enable watch mode (development)', false)
  .option('--no-health-check', 'Disable post-startup health check')
  .option('--timeout <ms>', 'Command timeout in milliseconds', '60000')
  .option('--max-retries <count>', 'Maximum retry attempts', '3')
  .option('--verbose', 'Verbose output', false)
  .option('--force', 'Force start even if processes are running', false)
  .parse();

const options = program.opts();

// Create starter instance
const starter = new SignalCastStarter({
  mode: options.mode,
  environment: options.environment,
  workerType: options.workerType,
  watch: options.watch,
  healthCheck: options.healthCheck,
  timeout: parseInt(options.timeout),
  maxRetries: parseInt(options.maxRetries),
  verbose: options.verbose,
  force: options.force
});

// Start the application
starter.start().catch((error) => {
  console.error('Startup failed:', error);
  process.exit(1);
});