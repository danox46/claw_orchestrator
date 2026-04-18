import pino, { type LoggerOptions } from "pino";
import { env } from "./env";

const baseOptions: LoggerOptions = {
  name: env.app.name,
  level: env.logging.level,
  base: {
    service: env.app.name,
    environment: env.app.nodeEnv,
    version: env.app.version,
  },
  timestamp: pino.stdTimeFunctions.isoTime,
};

const prettyOptions: LoggerOptions = env.logging.pretty
  ? {
      ...baseOptions,
      transport: {
        target: "pino-pretty",
        options: {
          colorize: true,
          translateTime: "SYS:standard",
          ignore: "pid,hostname",
          singleLine: false,
        },
      },
    }
  : baseOptions;

export const logger = pino(prettyOptions);

export function createLogger(bindings?: Record<string, unknown>) {
  return bindings ? logger.child(bindings) : logger;
}

export default logger;
