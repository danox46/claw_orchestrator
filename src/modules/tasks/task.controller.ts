import type { Request, Response } from "express";
import { successResponse } from "../../shared/http/api-response";
import {
  buildPaginatedResult,
  normalizePagination,
} from "../../shared/http/pagination";
import { ServiceError } from "../../shared/errors/service-error";
import type {
  CreateTaskInput as CreateTaskServiceInput,
  TasksServicePort,
  UpdateTaskInput as UpdateTaskServiceInput,
} from "./task.service";
import type {
  CreateTaskInput as CreateTaskRequest,
  UpdateTaskInput as UpdateTaskRequest,
} from "./task.schemas";
import type { ListTasksQuery, TaskIdParams } from "./task.query.schemas";

type SessionRetryFields = {
  sessionName?: string;
  sessionCount?: number;
  maxSessions?: number;
};

type CreateTaskLocals = {
  validated?: {
    body?: CreateTaskRequest;
  };
};

type GetTaskByIdLocals = {
  validated?: {
    params?: TaskIdParams;
  };
};

type UpdateTaskLocals = {
  validated?: {
    params?: TaskIdParams;
    body?: UpdateTaskRequest & SessionRetryFields;
  };
};

type ListTasksLocals = {
  validated?: {
    query?: ListTasksQuery;
  };
};

export type TasksController = {
  create: (
    _req: Request,
    res: Response<any, CreateTaskLocals>,
  ) => Promise<void>;
  getById: (
    _req: Request,
    res: Response<any, GetTaskByIdLocals>,
  ) => Promise<void>;
  update: (
    _req: Request,
    res: Response<any, UpdateTaskLocals>,
  ) => Promise<void>;
  list: (_req: Request, res: Response<any, ListTasksLocals>) => Promise<void>;
};

function requireValidatedBody<T>(
  res: Response<any, { validated?: { body?: T } }>,
): T {
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

function requireValidatedParams<T>(
  res: Response<any, { validated?: { params?: T } }>,
): T {
  const params = res.locals.validated?.params;

  if (params === undefined) {
    throw new ServiceError({
      code: "MISSING_VALIDATED_PARAMS",
      message: "Validated route params were not found.",
      statusCode: 500,
      expose: false,
    });
  }

  return params;
}

function requireValidatedQuery<T>(
  res: Response<any, { validated?: { query?: T } }>,
): T {
  const query = res.locals.validated?.query;

  if (query === undefined) {
    throw new ServiceError({
      code: "MISSING_VALIDATED_QUERY",
      message: "Validated query params were not found.",
      statusCode: 500,
      expose: false,
    });
  }

  return query;
}

function extractSessionRetryFields(
  input: Partial<SessionRetryFields>,
): SessionRetryFields {
  return {
    ...(typeof input.sessionName === "string"
      ? { sessionName: input.sessionName }
      : {}),
    ...(typeof input.sessionCount === "number"
      ? { sessionCount: input.sessionCount }
      : {}),
    ...(typeof input.maxSessions === "number"
      ? { maxSessions: input.maxSessions }
      : {}),
  };
}

function toCreateTaskServiceInput(
  input: CreateTaskRequest,
): CreateTaskServiceInput {
  const sessionFields = extractSessionRetryFields(
    input as CreateTaskRequest & SessionRetryFields,
  );

  return {
    jobId: input.jobId,
    projectId: input.projectId,
    milestoneId: input.milestoneId,
    ...(typeof input.parentTaskId === "string"
      ? { parentTaskId: input.parentTaskId }
      : {}),
    dependencies: [...input.dependencies],
    issuer: {
      kind: input.issuer.kind,
      id: input.issuer.id,
      ...(typeof input.issuer.sessionId === "string"
        ? { sessionId: input.issuer.sessionId }
        : {}),
      ...(typeof input.issuer.role === "string"
        ? { role: input.issuer.role }
        : {}),
    },
    target: {
      agentId: input.target.agentId,
    },
    intent: input.intent,
    inputs: input.inputs,
    constraints: {
      toolProfile: input.constraints.toolProfile,
      sandbox: input.constraints.sandbox,
      ...(typeof input.constraints.maxTokens === "number"
        ? { maxTokens: input.constraints.maxTokens }
        : {}),
      ...(typeof input.constraints.maxCost === "number"
        ? { maxCost: input.constraints.maxCost }
        : {}),
    },
    ...(input.requiredArtifacts.length > 0
      ? { requiredArtifacts: input.requiredArtifacts }
      : {}),
    ...(input.acceptanceCriteria.length > 0
      ? { acceptanceCriteria: input.acceptanceCriteria }
      : {}),
    idempotencyKey: input.idempotencyKey,
    ...(typeof input.status === "string" ? { status: input.status } : {}),
    ...(typeof input.attemptCount === "number"
      ? { attemptCount: input.attemptCount }
      : {}),
    ...(typeof input.maxAttempts === "number"
      ? { maxAttempts: input.maxAttempts }
      : {}),
    ...sessionFields,
    ...(input.nextRetryAt instanceof Date
      ? { nextRetryAt: input.nextRetryAt }
      : {}),
    ...(typeof input.lastError === "string"
      ? { lastError: input.lastError }
      : {}),
    ...(typeof input.retryable === "boolean"
      ? { retryable: input.retryable }
      : {}),
    ...(typeof input.sequence === "number" ? { sequence: input.sequence } : {}),
    ...(input.outputs !== undefined ? { outputs: input.outputs } : {}),
    ...(input.artifacts.length > 0 ? { artifacts: input.artifacts } : {}),
    ...(input.errors.length > 0 ? { errors: input.errors } : {}),
  };
}

function toUpdateTaskServiceInput(
  input: UpdateTaskRequest & SessionRetryFields,
): UpdateTaskServiceInput {
  const sessionFields = extractSessionRetryFields(input);

  return {
    ...(typeof input.milestoneId === "string"
      ? { milestoneId: input.milestoneId }
      : {}),
    ...(typeof input.parentTaskId === "string"
      ? { parentTaskId: input.parentTaskId }
      : {}),
    ...(Array.isArray(input.dependencies)
      ? { dependencies: [...input.dependencies] }
      : {}),
    ...(typeof input.status === "string" ? { status: input.status } : {}),
    ...(typeof input.attemptCount === "number"
      ? { attemptCount: input.attemptCount }
      : {}),
    ...(typeof input.maxAttempts === "number"
      ? { maxAttempts: input.maxAttempts }
      : {}),
    ...sessionFields,
    ...(input.nextRetryAt instanceof Date
      ? { nextRetryAt: input.nextRetryAt }
      : {}),
    ...(typeof input.lastError === "string"
      ? { lastError: input.lastError }
      : {}),
    ...(typeof input.retryable === "boolean"
      ? { retryable: input.retryable }
      : {}),
    ...(typeof input.sequence === "number" ? { sequence: input.sequence } : {}),
    ...(input.outputs !== undefined ? { outputs: input.outputs } : {}),
    ...(Array.isArray(input.artifacts)
      ? { artifacts: [...input.artifacts] }
      : {}),
    ...(Array.isArray(input.errors) ? { errors: [...input.errors] } : {}),
    ...(input.target !== undefined
      ? {
          target: {
            agentId: input.target.agentId,
          },
        }
      : {}),
  };
}

export function createTasksController(
  tasksService: TasksServicePort,
): TasksController {
  return {
    create: async (
      _req: Request,
      res: Response<any, CreateTaskLocals>,
    ): Promise<void> => {
      const input = requireValidatedBody(res);
      const task = await tasksService.createTask(
        toCreateTaskServiceInput(input),
      );

      res.status(201).json(successResponse(task));
    },

    getById: async (
      _req: Request,
      res: Response<any, GetTaskByIdLocals>,
    ): Promise<void> => {
      const { taskId } = requireValidatedParams(res);
      const task = await tasksService.requireTaskById(taskId);

      res.status(200).json(successResponse(task));
    },

    update: async (
      _req: Request,
      res: Response<any, UpdateTaskLocals>,
    ): Promise<void> => {
      const { taskId } = requireValidatedParams(res);
      const updates = requireValidatedBody(res);
      const task = await tasksService.updateTask(
        taskId,
        toUpdateTaskServiceInput(updates),
      );

      res.status(200).json(successResponse(task));
    },

    list: async (
      _req: Request,
      res: Response<any, ListTasksLocals>,
    ): Promise<void> => {
      const query = requireValidatedQuery(res);
      const { page, pageSize, skip, limit } = normalizePagination({
        page: query.page,
        pageSize: query.pageSize,
      });

      const filters = {
        ...(typeof query.jobId === "string" ? { jobId: query.jobId } : {}),
        ...(typeof query.projectId === "string"
          ? { projectId: query.projectId }
          : {}),
        ...(typeof query.milestoneId === "string"
          ? { milestoneId: query.milestoneId }
          : {}),
        ...(typeof query.parentTaskId === "string"
          ? { parentTaskId: query.parentTaskId }
          : {}),
        ...(query.status ? { status: query.status } : {}),
        ...(query.intent ? { intent: query.intent } : {}),
        ...(typeof query.agentId === "string"
          ? { agentId: query.agentId }
          : {}),
      };

      const [items, totalItems] = await Promise.all([
        tasksService.listTasks({
          ...filters,
          skip,
          limit,
        }),
        tasksService.countTasks(filters),
      ]);

      res.status(200).json(
        successResponse(
          buildPaginatedResult({
            items,
            page,
            pageSize,
            totalItems,
          }),
        ),
      );
    },
  };
}

export default createTasksController;
