import type { Request, Response } from "express";
import { successResponse } from "../../shared/http/api-response";
import { ServiceError } from "../../shared/errors/service-error";
import type { CreateIntakeRequest } from "./intake.schemas";
import type { IntakeServicePort } from "./intake.service";

type IntakeValidatedLocals = {
  validated?: {
    body?: CreateIntakeRequest;
  };
};

type IntakeCreateInput = Parameters<IntakeServicePort["createIntakeJob"]>[0];

type RawUpdateIntakeFields = {
  mode?: unknown;
  projectId?: unknown;
  requestType?: unknown;
  request?: unknown;
  canonicalProjectRoot?: unknown;
};

export type IntakeController = {
  health: (_req: Request, res: Response) => void;
  create: (
    req: Request,
    res: Response<any, IntakeValidatedLocals>,
  ) => Promise<void>;
};

function requireValidatedBody(
  res: Response<any, IntakeValidatedLocals>,
): CreateIntakeRequest {
  const body = res.locals.validated?.body;

  if (body === undefined) {
    throw new ServiceError({
      code: "MISSING_VALIDATED_BODY",
      message: "Validated request body was not found.",
      statusCode: 500,
      expose: false,
    });
  }

  return body;
}

function mergeUpdateFields(
  validatedBody: CreateIntakeRequest,
  rawBody: unknown,
): IntakeCreateInput {
  const raw =
    rawBody !== null && typeof rawBody === "object"
      ? (rawBody as RawUpdateIntakeFields)
      : undefined;

  return {
    ...validatedBody,
    ...(typeof raw?.mode === "string" ? { mode: raw.mode } : {}),
    ...(typeof raw?.projectId === "string" ? { projectId: raw.projectId } : {}),
    ...(typeof raw?.requestType === "string"
      ? { requestType: raw.requestType }
      : {}),
    ...(typeof raw?.request === "string" ? { request: raw.request } : {}),
    ...(typeof raw?.canonicalProjectRoot === "string"
      ? { canonicalProjectRoot: raw.canonicalProjectRoot }
      : {}),
  } as IntakeCreateInput;
}

export function createIntakeController(
  intakeService: IntakeServicePort,
): IntakeController {
  return {
    health: (_req: Request, res: Response): void => {
      res.status(200).json(
        successResponse({
          module: "intake",
          status: "healthy",
          timestamp: new Date().toISOString(),
        }),
      );
    },

    create: async (
      req: Request,
      res: Response<any, IntakeValidatedLocals>,
    ): Promise<void> => {
      const validatedBody = requireValidatedBody(res);
      const input = mergeUpdateFields(validatedBody, req.body);
      const result = await intakeService.createIntakeJob(input);

      res.status(202).json(
        successResponse({
          projectId: result.projectId,
          jobId: result.jobId,
          milestoneId: result.milestoneId,
          taskId: result.taskId,
          status: result.status,
          message:
            result.message ??
            "Intake request accepted and queued for orchestration.",
        }),
      );
    },
  };
}

export default createIntakeController;
