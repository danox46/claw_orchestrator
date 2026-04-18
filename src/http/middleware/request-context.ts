import { randomUUID } from "node:crypto";
import type { NextFunction, Request, RequestHandler, Response } from "express";

export type RequestContext = {
  requestId: string;
  startedAt: string;
  startedAtMs: number;
};

export type RequestContextLocals = {
  requestId: string;
  requestContext: RequestContext;
};

const REQUEST_ID_HEADER = "x-request-id";

function getIncomingRequestId(req: Request): string | undefined {
  const headerValue = req.header(REQUEST_ID_HEADER);

  if (typeof headerValue !== "string") {
    return undefined;
  }

  const trimmed = headerValue.trim();

  return trimmed.length > 0 ? trimmed : undefined;
}

function buildRequestContext(req: Request): RequestContext {
  const requestId = getIncomingRequestId(req) ?? randomUUID();
  const startedAtMs = Date.now();

  return {
    requestId,
    startedAt: new Date(startedAtMs).toISOString(),
    startedAtMs,
  };
}

export const requestContext: RequestHandler = (
  req: Request,
  res: Response<any, Partial<RequestContextLocals>>,
  next: NextFunction,
): void => {
  const context = buildRequestContext(req);

  res.locals.requestId = context.requestId;
  res.locals.requestContext = context;

  res.setHeader(REQUEST_ID_HEADER, context.requestId);

  next();
};

export function createRequestContextMiddleware(): RequestHandler {
  return requestContext;
}

export default requestContext;
