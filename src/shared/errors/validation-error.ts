import { ServiceError } from "./service-error";

export type ValidationErrorOptions = {
  message?: string;
  code?: string;
  details?: unknown;
  cause?: unknown;
  statusCode?: 400 | 422;
};

export class ValidationError extends ServiceError {
  constructor(options: ValidationErrorOptions = {}) {
    const statusCode = options.statusCode ?? 400;

    super({
      code: options.code ?? "VALIDATION_ERROR",
      message: options.message ?? "The provided data is invalid.",
      statusCode,
      details: options.details,
      expose: true,
      cause: options.cause,
    });

    this.name = "ValidationError";
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

export default ValidationError;
