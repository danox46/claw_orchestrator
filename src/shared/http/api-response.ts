export type ApiSuccessResponse<TData> = {
  ok: true;
  data: TData;
  meta?: Record<string, unknown>;
};

export type ApiErrorResponse = {
  ok: false;
  error: {
    code: string;
    message: string;
    details?: unknown;
    requestId?: string;
  };
};

export type ApiResponse<TData> = ApiSuccessResponse<TData> | ApiErrorResponse;

export function successResponse<TData>(
  data: TData,
  meta?: Record<string, unknown>,
): ApiSuccessResponse<TData> {
  return {
    ok: true,
    data,
    ...(meta !== undefined ? { meta } : {}),
  };
}

export function errorResponse(
  code: string,
  message: string,
  options?: {
    details?: unknown;
    requestId?: string;
  },
): ApiErrorResponse {
  return {
    ok: false,
    error: {
      code,
      message,
      ...(options?.details !== undefined ? { details: options.details } : {}),
      ...(typeof options?.requestId === "string" && options.requestId.length > 0
        ? { requestId: options.requestId }
        : {}),
    },
  };
}
