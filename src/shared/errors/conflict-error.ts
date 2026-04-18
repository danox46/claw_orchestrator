import { ServiceError } from "./service-error";

export type ConflictErrorOptions = {
  message?: string;
  code?: string;
  details?: unknown;
  cause?: unknown;
};

export class ConflictError extends ServiceError {
  constructor(options: ConflictErrorOptions = {}) {
    super({
      code: options.code ?? "CONFLICT",
      message:
        options.message ??
        "The request conflicts with the current state of the resource.",
      statusCode: 409,
      details: options.details,
      expose: true,
      cause: options.cause,
    });

    this.name = "ConflictError";
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

export default ConflictError;
