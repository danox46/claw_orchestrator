export type ServiceErrorOptions = {
  code: string;
  message: string;
  statusCode?: number;
  details?: unknown;
  expose?: boolean;
  cause?: unknown;
};

export class ServiceError extends Error {
  public readonly code: string;
  public readonly statusCode: number;
  public readonly details?: unknown;
  public readonly expose: boolean;
  public override readonly cause?: unknown;

  constructor(options: ServiceErrorOptions) {
    super(options.message);

    this.name = "ServiceError";
    this.code = options.code;
    this.statusCode = options.statusCode ?? 500;
    this.expose =
      options.expose ?? (this.statusCode >= 400 && this.statusCode < 500);

    if (options.details !== undefined) {
      this.details = options.details;
    }

    if (options.cause !== undefined) {
      this.cause = options.cause;
    }

    Object.setPrototypeOf(this, new.target.prototype);
  }
}

export default ServiceError;
