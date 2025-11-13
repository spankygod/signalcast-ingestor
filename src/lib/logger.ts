import winston from 'winston';
import path from 'path';
import fs from 'fs';

const logsDir = path.join(process.cwd(), 'logs');
if (!fs.existsSync(logsDir)) {
  fs.mkdirSync(logsDir, { recursive: true });
}

const logger = winston.createLogger({
  level: process.env.LOG_LEVEL || 'info',
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.errors({ stack: true }),
    winston.format.json()
  ),
  defaultMeta: { service: 'signalcast-ingestor' },
  transports: [
    new winston.transports.File({
      filename: path.join(logsDir, 'error.log'),
      level: 'error'
    }),
    new winston.transports.File({
      filename: path.join(logsDir, 'ingestor.log')
    })
  ]
});

if (process.env.NODE_ENV !== 'production') {
  logger.add(new winston.transports.Console({
    format: winston.format.combine(
      winston.format.colorize(),
      winston.format.simple()
    )
  }));
}

export default logger;

export function formatError(error: unknown): Record<string, unknown> {
  if (!error) {
    return {};
  }

  if (error instanceof Error) {
    const serialized: Record<string, unknown> = {
      name: error.name,
      message: error.message,
      stack: error.stack
    };

    const anyError = error as unknown as Record<string, unknown> & { [key: string]: unknown };
    for (const key of Object.keys(anyError)) {
      const value = anyError[key];
      if (value !== undefined && serialized[key] === undefined) {
        serialized[key] = value;
      }
    }

    return serialized;
  }

  if (typeof error === 'object') {
    return { ...(error as Record<string, unknown>) };
  }

  return { message: String(error) };
}
