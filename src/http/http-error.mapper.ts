import type {
  ErrorRequestHandler,
  NextFunction,
  Request,
  Response,
} from "express";
import { ZodError } from "zod";

type ErrorLike = {
  name?: string;
  message?: string;
  code?: string;
  status?: number;
  statusCode?: number;
  details?: unknown;
  expose?: boolean;
  cause?: unknown;
  stack?: string;
};

type ErrorResponseBody = {
  ok: false;
  error: {
    code: string;
    message: string;
    details?: unknown;
    requestId?: string;
  };
};

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function isErrorLike(value: unknown): value is ErrorLike {
  return isObject(value);
}

function getErrorStatus(error: ErrorLike): number {
  if (typeof error.statusCode === "number") {
    return error.statusCode;
  }

  if (typeof error.status === "number") {
    return error.status;
  }

  return 500;
}

function getErrorCode(error: ErrorLike, fallbackStatus: number): string {
  if (typeof error.code === "string" && error.code.trim().length > 0) {
    return error.code.trim();
  }

  switch (fallbackStatus) {
    case 400:
      return "BAD_REQUEST";
    case 401:
      return "UNAUTHORIZED";
    case 403:
      return "FORBIDDEN";
    case 404:
      return "NOT_FOUND";
    case 409:
      return "CONFLICT";
    case 422:
      return "UNPROCESSABLE_ENTITY";
    default:
      return "INTERNAL_SERVER_ERROR";
  }
}

function shouldExposeMessage(error: ErrorLike, status: number): boolean {
  if (typeof error.expose === "boolean") {
    return error.expose;
  }

  return status >= 400 && status < 500;
}

function normalizeUnknownError(error: unknown): ErrorLike {
  if (error instanceof Error) {
    const normalized: ErrorLike = {
      name: error.name,
      message: error.message,
      ...("cause" in error ? { cause: error.cause } : {}),
      ...(typeof error.stack === "string" ? { stack: error.stack } : {}),
    };

    return normalized;
  }

  if (isErrorLike(error)) {
    return error;
  }

  return {
    message: "An unknown error was thrown.",
    details: {
      thrownValue: error,
    },
  };
}

function formatZodError(error: ZodError) {
  const flattened = error.flatten();

  return {
    fieldErrors: flattened.fieldErrors,
    formErrors: flattened.formErrors,
    issues: error.issues.map((issue) => ({
      code: issue.code,
      path: issue.path.join("."),
      message: issue.message,
    })),
  };
}

function buildErrorResponse(
  error: unknown,
  requestId?: string,
): {
  status: number;
  body: ErrorResponseBody;
  logDetails?: unknown;
} {
  if (error instanceof ZodError) {
    return {
      status: 400,
      body: {
        ok: false,
        error: {
          code: "VALIDATION_ERROR",
          message: "Request validation failed.",
          details: formatZodError(error),
          ...(requestId ? { requestId } : {}),
        },
      },
      logDetails: error.issues,
    };
  }

  const normalized = normalizeUnknownError(error);
  const status = getErrorStatus(normalized);
  const safeStatus =
    Number.isInteger(status) && status >= 400 && status <= 599 ? status : 500;
  const code = getErrorCode(normalized, safeStatus);
  const exposeMessage = shouldExposeMessage(normalized, safeStatus);

  const message =
    exposeMessage &&
    typeof normalized.message === "string" &&
    normalized.message.length > 0
      ? normalized.message
      : "An unexpected error occurred.";

  const errorBody: ErrorResponseBody["error"] = {
    code,
    message,
    ...(normalized.details !== undefined
      ? { details: normalized.details }
      : {}),
    ...(requestId ? { requestId } : {}),
  };

  return {
    status: safeStatus,
    body: {
      ok: false,
      error: errorBody,
    },
    logDetails: {
      ...(typeof normalized.name === "string" ? { name: normalized.name } : {}),
      ...(typeof normalized.code === "string" ? { code: normalized.code } : {}),
      ...(normalized.details !== undefined
        ? { details: normalized.details }
        : {}),
      ...(normalized.cause !== undefined ? { cause: normalized.cause } : {}),
      ...(typeof normalized.stack === "string"
        ? { stack: normalized.stack }
        : {}),
    },
  };
}

function getRequestId(res: Response): string | undefined {
  const maybeRequestId = res.locals?.requestId;

  return typeof maybeRequestId === "string" && maybeRequestId.length > 0
    ? maybeRequestId
    : undefined;
}

export const httpErrorMapper: ErrorRequestHandler = (
  error: unknown,
  req: Request,
  res: Response,
  _next: NextFunction,
): void => {
  const requestId = getRequestId(res);
  const { status, body, logDetails } = buildErrorResponse(error, requestId);

  if (res.headersSent) {
    return;
  }

  if (status >= 500) {
    console.error("Unhandled HTTP error", {
      method: req.method,
      path: req.originalUrl,
      requestId,
      error: logDetails,
    });
  } else {
    console.warn("Handled HTTP error", {
      method: req.method,
      path: req.originalUrl,
      requestId,
      error: logDetails,
    });
  }

  res.status(status).json(body);
};

export default httpErrorMapper;
