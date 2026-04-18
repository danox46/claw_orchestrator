import { ServiceError } from "./service-error";

export type NotFoundErrorOptions = {
  message?: string;
  code?: string;
  details?: unknown;
  cause?: unknown;
};

export class NotFoundError extends ServiceError {
  constructor(options: NotFoundErrorOptions = {}) {
    super({
      code: options.code ?? "NOT_FOUND",
      message: options.message ?? "The requested resource was not found.",
      statusCode: 404,
      details: options.details,
      expose: true,
      cause: options.cause,
    });

    this.name = "NotFoundError";
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

export default NotFoundError;
