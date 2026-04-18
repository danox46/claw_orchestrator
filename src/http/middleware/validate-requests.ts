import type { NextFunction, Request, RequestHandler, Response } from "express";
import { ZodError, type ZodTypeAny } from "zod";

export type RequestSchemas = {
  body?: ZodTypeAny;
  params?: ZodTypeAny;
  query?: ZodTypeAny;
};

export type ValidatedRequestData = {
  body?: unknown;
  params?: unknown;
  query?: unknown;
};

export type ResponseLocalsWithValidated = {
  validated: ValidatedRequestData;
};

function formatZodError(error: ZodError) {
  return {
    fieldErrors: error.flatten().fieldErrors,
    formErrors: error.flatten().formErrors,
    issues: error.issues.map((issue) => ({
      code: issue.code,
      path: issue.path.join("."),
      message: issue.message,
    })),
  };
}

export function validateRequest(schemas: RequestSchemas): RequestHandler {
  return (req: Request, res: Response, next: NextFunction): void => {
    try {
      const validated: ValidatedRequestData = {};

      if (schemas.body) {
        validated.body = schemas.body.parse(req.body);
      }

      if (schemas.params) {
        validated.params = schemas.params.parse(req.params);
      }

      if (schemas.query) {
        validated.query = schemas.query.parse(req.query);
      }

      res.locals.validated = validated;

      next();
    } catch (error) {
      if (error instanceof ZodError) {
        res.status(400).json({
          ok: false,
          error: {
            code: "VALIDATION_ERROR",
            message: "Request validation failed.",
            details: formatZodError(error),
          },
        });
        return;
      }

      next(error);
    }
  };
}

export default validateRequest;
