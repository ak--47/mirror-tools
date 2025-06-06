import pino from 'pino';

// Optional: Configure based on environment (dev vs prod)
const isDev = process.env.NODE_ENV !== 'production';

const logger = pino({
  level: process.env.LOG_LEVEL || 'info', // Supports 'fatal', 'error', 'warn', 'info', 'debug', 'trace'
  transport: isDev
    ? {
        target: 'pino-pretty', // Prettified output for local dev
        options: { colorize: true, translateTime: true }
      }
    : undefined // In prod/cloud, keep as JSON for cloud logging
});

export default logger;