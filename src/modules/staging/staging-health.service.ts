import axios, { type AxiosInstance } from "axios";
import { env } from "../../config/env";
import { createLogger } from "../../config/logger";

const logger = createLogger({
  module: "staging",
  component: "staging-health-service",
});

type ServiceError = Error & {
  statusCode?: number;
  code?: string;
  details?: unknown;
};

function createServiceError(input: {
  message: string;
  code: string;
  statusCode: number;
  details?: unknown;
}): ServiceError {
  return Object.assign(new Error(input.message), {
    code: input.code,
    statusCode: input.statusCode,
    details: input.details,
  });
}

export type WaitForHealthyInput = {
  url: string;
  timeoutMs?: number;
  intervalMs?: number;
  readyPath?: string;
  acceptedStatusCodes?: number[];
};

export class StagingHealthService {
  private readonly http: AxiosInstance;

  constructor(httpClient?: AxiosInstance) {
    this.http =
      httpClient ??
      axios.create({
        timeout: 10_000,
        validateStatus: () => true,
      });
  }

  /**
   * Polls a staging deployment until it becomes healthy.
   *
   * Strategy:
   * - try `${url}/ready` first
   * - if needed, callers can override the path
   * - accept 2xx by default
   */
  async waitForHealthy(input: WaitForHealthyInput): Promise<void> {
    const startedAt = Date.now();
    const timeoutMs = input.timeoutMs ?? env.staging.healthcheckTimeoutMs;
    const intervalMs = input.intervalMs ?? 2_000;
    const acceptedStatusCodes = input.acceptedStatusCodes ?? [
      200, 201, 202, 204,
    ];

    const baseUrl = input.url.replace(/\/+$/, "");
    const readyPath = input.readyPath ?? "/ready";
    const targetUrl = this.buildHealthUrl(baseUrl, readyPath);

    let lastStatusCode: number | undefined;
    let lastBody: unknown;

    logger.info(
      {
        url: targetUrl,
        timeoutMs,
        intervalMs,
      },
      "Waiting for staging health check to pass.",
    );

    while (Date.now() - startedAt < timeoutMs) {
      try {
        const response = await this.http.get(targetUrl);

        lastStatusCode = response.status;
        lastBody = response.data;

        if (acceptedStatusCodes.includes(response.status)) {
          logger.info(
            {
              url: targetUrl,
              statusCode: response.status,
            },
            "Staging health check passed.",
          );
          return;
        }

        logger.debug(
          {
            url: targetUrl,
            statusCode: response.status,
            responseBody: response.data,
          },
          "Staging health check has not passed yet.",
        );
      } catch (error) {
        logger.debug(
          {
            url: targetUrl,
            err: error,
          },
          "Staging health check request failed. Retrying.",
        );
      }

      await this.sleep(intervalMs);
    }

    throw createServiceError({
      message: `Staging deployment did not become healthy within ${timeoutMs}ms.`,
      code: "STAGING_HEALTHCHECK_TIMEOUT",
      statusCode: 504,
      details: {
        url: targetUrl,
        timeoutMs,
        lastStatusCode,
        lastBody,
      },
    });
  }

  private buildHealthUrl(baseUrl: string, readyPath: string): string {
    const normalizedPath = readyPath.startsWith("/")
      ? readyPath
      : `/${readyPath}`;
    return `${baseUrl}${normalizedPath}`;
  }

  private async sleep(ms: number): Promise<void> {
    await new Promise<void>((resolve) => {
      setTimeout(resolve, ms);
    });
  }
}

export default StagingHealthService;
