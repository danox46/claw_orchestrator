import mongoose from "mongoose";
import express, {
  type Express,
  type NextFunction,
  type Request,
  type Response,
} from "express";
import { createApiRouter, type ApiRouterDependencies } from "./http/api.router";
import httpErrorMapper from "./http/http-error.mapper";
import requestContext, {
  type RequestContextLocals,
} from "./http/middleware/request-context";
import { NotFoundError } from "./shared/errors/not-found-error";

export type AppDependencies = ApiRouterDependencies;

function getRequestId(
  res: Response<any, Partial<RequestContextLocals>>,
): string | undefined {
  const requestId = res.locals.requestId;

  return typeof requestId === "string" && requestId.length > 0
    ? requestId
    : undefined;
}

function getMongoHealth(): {
  status: "up" | "down";
  readyState: number;
} {
  const readyState = mongoose.connection.readyState;

  return {
    status: readyState === 1 ? "up" : "down",
    readyState,
  };
}

export function createApp(dependencies: AppDependencies): Express {
  const app = express();

  app.set("trust proxy", 1);

  app.use(express.json({ limit: "1mb" }));
  app.use(express.urlencoded({ extended: true, limit: "1mb" }));

  app.use(requestContext);

  app.use(
    (
      req: Request,
      res: Response<any, Partial<RequestContextLocals>>,
      next: NextFunction,
    ) => {
      res.on("finish", () => {
        const startedAtMs =
          res.locals.requestContext?.startedAtMs ?? Date.now();
        const durationMs = Date.now() - startedAtMs;
        const requestId = getRequestId(res);

        console.log(
          JSON.stringify({
            level: "info",
            event: "http_request_completed",
            method: req.method,
            path: req.originalUrl,
            statusCode: res.statusCode,
            durationMs,
            ...(requestId ? { requestId } : {}),
          }),
        );
      });

      next();
    },
  );

  app.get("/health", (_req: Request, res: Response) => {
    res.status(200).json({
      ok: true,
      service: "app-factory-orchestrator",
      status: "healthy",
      timestamp: new Date().toISOString(),
    });
  });

  app.get("/ready", (_req: Request, res: Response) => {
    const mongo = getMongoHealth();
    const isReady = mongo.status === "up";

    res.status(isReady ? 200 : 503).json({
      ok: isReady,
      service: "app-factory-orchestrator",
      status: isReady ? "ready" : "not-ready",
      checks: {
        api: "up",
        mongo,
      },
      timestamp: new Date().toISOString(),
    });
  });

  app.get("/", (_req: Request, res: Response) => {
    res.status(200).json({
      ok: true,
      name: "App Factory Orchestrator",
      version: "0.1.0",
    });
  });

  app.use("/api", createApiRouter(dependencies));

  app.use((req: Request, _res: Response, next: NextFunction) => {
    next(
      new NotFoundError({
        code: "ROUTE_NOT_FOUND",
        message: `Route not found: ${req.method} ${req.originalUrl}`,
        details: {
          method: req.method,
          path: req.originalUrl,
        },
      }),
    );
  });

  app.use(httpErrorMapper);

  return app;
}

export default createApp;
