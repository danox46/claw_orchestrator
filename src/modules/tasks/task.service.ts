import { type QueryFilter, Types } from "mongoose";
import { ConflictError } from "../../shared/errors/conflict-error";
import { NotFoundError } from "../../shared/errors/not-found-error";
import { ValidationError } from "../../shared/errors/validation-error";
import MilestoneModel from "../milestones/milestone.model";
import TaskModel, { type TaskModelType } from "./task.model";
import type {
  TaskConstraints,
  TaskIntent,
  TaskIssuer,
  TaskStatus,
  TaskTarget,
  UpdateTaskInput as UpdateTaskRequest,
} from "./task.schemas";

export type CreateTaskInput = {
  jobId: string;
  projectId: string;
  milestoneId: string;
  parentTaskId?: string;
  dependencies?: string[];
  issuer: TaskIssuer;
  target: TaskTarget;
  intent: TaskIntent;
  inputs: Record<string, unknown>;
  constraints: TaskConstraints;
  requiredArtifacts?: string[];
  acceptanceCriteria?: string[];
  idempotencyKey: string;
  status?: TaskStatus;
  attemptCount?: number;
  maxAttempts?: number;
  sessionName?: string;
  sessionCount?: number;
  maxSessions?: number;
  nextRetryAt?: Date;
  lastError?: string;
  retryable?: boolean;
  sequence?: number;
  outputs?: Record<string, unknown>;
  artifacts?: string[];
  errors?: string[];
};

export type UpdateTaskInput = UpdateTaskRequest & {
  sessionName?: string;
  sessionCount?: number;
  maxSessions?: number;
};

export type TaskRecord = {
  _id: string;
  jobId: string;
  projectId: string;
  milestoneId: string;
  parentTaskId?: string;
  dependencies: string[];
  issuer: TaskIssuer;
  target: TaskTarget;
  intent: TaskIntent;
  inputs: Record<string, unknown>;
  constraints: TaskConstraints;
  requiredArtifacts: string[];
  acceptanceCriteria: string[];
  idempotencyKey: string;
  status: TaskStatus;
  attemptCount: number;
  maxAttempts: number;
  sessionName?: string;
  sessionCount: number;
  maxSessions: number;
  nextRetryAt?: Date;
  lastError?: string;
  retryable: boolean;
  sequence: number;
  outputs?: Record<string, unknown>;
  artifacts: string[];
  errors: string[];
  createdAt: Date;
  updatedAt: Date;
};

export type ListTasksInput = {
  jobId?: string;
  projectId?: string;
  milestoneId?: string;
  parentTaskId?: string;
  status?: TaskStatus;
  intent?: TaskIntent;
  agentId?: string;
  sessionName?: string;
  sessionCount?: number;
  maxSessions?: number;
  limit?: number;
  skip?: number;
};

export type CountTasksInput = Pick<
  ListTasksInput,
  | "jobId"
  | "projectId"
  | "milestoneId"
  | "parentTaskId"
  | "status"
  | "intent"
  | "agentId"
  | "sessionName"
  | "sessionCount"
  | "maxSessions"
>;

export interface TasksServicePort {
  createTask(input: CreateTaskInput): Promise<TaskRecord>;
  getTaskById(taskId: string): Promise<TaskRecord | null>;
  requireTaskById(taskId: string): Promise<TaskRecord>;
  getTaskByIdempotencyKey(idempotencyKey: string): Promise<TaskRecord | null>;
  updateTask(taskId: string, updates: UpdateTaskInput): Promise<TaskRecord>;
  setStatus(taskId: string, status: TaskStatus): Promise<TaskRecord>;
  markRunning(taskId: string): Promise<TaskRecord>;
  markSucceeded(input: {
    taskId: string;
    outputs?: Record<string, unknown>;
    artifacts?: string[];
  }): Promise<TaskRecord>;
  markFailed(input: {
    taskId: string;
    errors: string[];
    outputs?: Record<string, unknown>;
    artifacts?: string[];
  }): Promise<TaskRecord>;
  markFailedExhausted(input: {
    taskId: string;
    errors: string[];
    outputs?: Record<string, unknown>;
    artifacts?: string[];
  }): Promise<TaskRecord>;
  requeueTask(input: {
    taskId: string;
    error: string;
    nextRetryAt?: Date;
    outputs?: Record<string, unknown>;
    artifacts?: string[];
  }): Promise<TaskRecord>;
  cancelTask(taskId: string, reason?: string): Promise<TaskRecord>;
  listTasks(input?: ListTasksInput): Promise<TaskRecord[]>;
  countTasks(input?: CountTasksInput): Promise<number>;
  listRunnableTasks(input?: {
    jobId?: string;
    agentId?: string;
    milestoneId?: string;
    limit?: number;
    ignoreRetryAt?: boolean;
  }): Promise<TaskRecord[]>;
  listNextRunnableTask(input?: {
    jobId?: string;
    agentId?: string;
    milestoneId?: string;
    ignoreRetryAt?: boolean;
  }): Promise<TaskRecord | null>;
}

type TaskDocumentLike = {
  id: string;
  jobId: Types.ObjectId;
  projectId: Types.ObjectId;
  milestoneId: Types.ObjectId;
  parentTaskId?: Types.ObjectId | null;
  dependencies?: Types.ObjectId[] | null;
  issuer: {
    kind: TaskIssuer["kind"];
    id: string;
    sessionId?: string | null;
    role?: string | null;
  };
  target: {
    agentId: string;
  };
  intent: TaskIntent;
  inputs?: Record<string, unknown> | null;
  constraints: {
    toolProfile: string;
    sandbox: TaskConstraints["sandbox"];
    maxTokens?: number | null;
    maxCost?: number | null;
  };
  requiredArtifacts: string[];
  acceptanceCriteria: string[];
  idempotencyKey: string;
  status: TaskStatus;
  attemptCount: number;
  maxAttempts: number;
  sessionName?: string | null;
  sessionCount: number;
  maxSessions: number;
  nextRetryAt?: Date | null;
  lastError?: string | null;
  retryable: boolean;
  sequence: number;
  outputs?: Record<string, unknown> | null;
  artifacts?: string[] | null;
  errors?: string[] | null;
  createdAt: Date;
  updatedAt: Date;
};

function toObjectId(value: string, fieldName: string): Types.ObjectId {
  if (!Types.ObjectId.isValid(value)) {
    throw new ValidationError({
      message: `Invalid ${fieldName}: ${value}`,
      code: "INVALID_OBJECT_ID",
      statusCode: 400,
      details: {
        fieldName,
        value,
      },
    });
  }

  return new Types.ObjectId(value);
}

function mapTask(document: TaskDocumentLike): TaskRecord {
  return {
    _id: document.id,
    jobId: document.jobId.toString(),
    projectId: document.projectId.toString(),
    milestoneId: document.milestoneId.toString(),
    ...(document.parentTaskId
      ? { parentTaskId: document.parentTaskId.toString() }
      : {}),
    dependencies: [...(document.dependencies ?? [])].map((dependencyId) =>
      dependencyId.toString(),
    ),
    issuer: {
      kind: document.issuer.kind,
      id: document.issuer.id,
      ...(document.issuer.sessionId
        ? { sessionId: document.issuer.sessionId }
        : {}),
      ...(document.issuer.role ? { role: document.issuer.role } : {}),
    },
    target: {
      agentId: document.target.agentId,
    },
    intent: document.intent,
    inputs: (document.inputs ?? {}) as Record<string, unknown>,
    constraints: {
      toolProfile: document.constraints.toolProfile,
      sandbox: document.constraints.sandbox,
      ...(typeof document.constraints.maxTokens === "number"
        ? { maxTokens: document.constraints.maxTokens }
        : {}),
      ...(typeof document.constraints.maxCost === "number"
        ? { maxCost: document.constraints.maxCost }
        : {}),
    },
    requiredArtifacts: [...document.requiredArtifacts],
    acceptanceCriteria: [...document.acceptanceCriteria],
    idempotencyKey: document.idempotencyKey,
    status: document.status,
    attemptCount: document.attemptCount,
    maxAttempts: document.maxAttempts,
    ...(typeof document.sessionName === "string" &&
    document.sessionName.length > 0
      ? { sessionName: document.sessionName }
      : {}),
    sessionCount:
      typeof document.sessionCount === "number" &&
      Number.isFinite(document.sessionCount)
        ? document.sessionCount
        : 1,
    maxSessions:
      typeof document.maxSessions === "number" &&
      Number.isFinite(document.maxSessions)
        ? document.maxSessions
        : 1,
    ...(document.nextRetryAt instanceof Date
      ? { nextRetryAt: document.nextRetryAt }
      : {}),
    ...(typeof document.lastError === "string" && document.lastError.length > 0
      ? { lastError: document.lastError }
      : {}),
    retryable: document.retryable,
    sequence: document.sequence,
    ...(document.outputs !== null && document.outputs !== undefined
      ? { outputs: document.outputs as Record<string, unknown> }
      : {}),
    artifacts: [...(document.artifacts ?? [])],
    errors: [...(document.errors ?? [])],
    createdAt: document.createdAt,
    updatedAt: document.updatedAt,
  };
}

function normalizeCreateTaskInput(input: CreateTaskInput): {
  requiredArtifacts: string[];
  acceptanceCriteria: string[];
  status: TaskStatus;
  attemptCount: number;
  maxAttempts: number;
  sessionName?: string;
  sessionCount: number;
  maxSessions: number;
  retryable: boolean;
  sequence: number;
  artifacts: string[];
  errors: string[];
  dependencies: string[];
} {
  const sessionName =
    typeof input.sessionName === "string" && input.sessionName.trim().length > 0
      ? input.sessionName.trim()
      : undefined;

  return {
    requiredArtifacts: [...(input.requiredArtifacts ?? [])],
    acceptanceCriteria: [...(input.acceptanceCriteria ?? [])],
    status: input.status ?? "queued",
    attemptCount: input.attemptCount ?? 0,
    maxAttempts: input.maxAttempts ?? 5,
    ...(sessionName ? { sessionName } : {}),
    sessionCount: input.sessionCount ?? 1,
    maxSessions: input.maxSessions ?? 4,
    retryable: input.retryable ?? true,
    sequence: input.sequence ?? 0,
    artifacts: [...(input.artifacts ?? [])],
    errors: [...(input.errors ?? [])],
    dependencies: dedupeStringIds(input.dependencies ?? []),
  };
}

function dedupeStringIds(values: string[]): string[] {
  return [...new Set(values.map((value) => value.trim()).filter(Boolean))];
}

export class TaskService implements TasksServicePort {
  async createTask(input: CreateTaskInput): Promise<TaskRecord> {
    try {
      const normalized = normalizeCreateTaskInput(input);

      await this.assertMilestoneBelongsToProject({
        projectId: input.projectId,
        milestoneId: input.milestoneId,
      });

      if (normalized.dependencies.length > 0) {
        await this.assertDependenciesAreValid({
          projectId: input.projectId,
          milestoneId: input.milestoneId,
          dependencyIds: normalized.dependencies,
        });
      }

      const created = await TaskModel.create({
        jobId: toObjectId(input.jobId, "jobId"),
        projectId: toObjectId(input.projectId, "projectId"),
        milestoneId: toObjectId(input.milestoneId, "milestoneId"),
        ...(typeof input.parentTaskId === "string"
          ? { parentTaskId: toObjectId(input.parentTaskId, "parentTaskId") }
          : {}),
        dependencies: normalized.dependencies.map((dependencyId) =>
          toObjectId(dependencyId, "dependencyId"),
        ),
        issuer: {
          kind: input.issuer.kind,
          id: input.issuer.id.trim(),
          ...(typeof input.issuer.sessionId === "string"
            ? { sessionId: input.issuer.sessionId.trim() }
            : {}),
          ...(typeof input.issuer.role === "string"
            ? { role: input.issuer.role.trim() }
            : {}),
        },
        target: {
          agentId: input.target.agentId.trim(),
        },
        intent: input.intent,
        inputs: input.inputs,
        constraints: {
          toolProfile: input.constraints.toolProfile.trim(),
          sandbox: input.constraints.sandbox,
          ...(typeof input.constraints.maxTokens === "number"
            ? { maxTokens: input.constraints.maxTokens }
            : {}),
          ...(typeof input.constraints.maxCost === "number"
            ? { maxCost: input.constraints.maxCost }
            : {}),
        },
        requiredArtifacts: normalized.requiredArtifacts,
        acceptanceCriteria: normalized.acceptanceCriteria,
        idempotencyKey: input.idempotencyKey.trim(),
        status: normalized.status,
        attemptCount: normalized.attemptCount,
        maxAttempts: normalized.maxAttempts,
        ...(normalized.sessionName
          ? { sessionName: normalized.sessionName }
          : {}),
        sessionCount: normalized.sessionCount,
        maxSessions: normalized.maxSessions,
        ...(input.nextRetryAt instanceof Date
          ? { nextRetryAt: input.nextRetryAt }
          : {}),
        ...(typeof input.lastError === "string" &&
        input.lastError.trim().length > 0
          ? { lastError: input.lastError.trim() }
          : {}),
        retryable: normalized.retryable,
        sequence: normalized.sequence,
        ...(input.outputs !== undefined ? { outputs: input.outputs } : {}),
        artifacts: normalized.artifacts,
        errors: normalized.errors,
      });

      return mapTask(created);
    } catch (error: unknown) {
      if (this.isDuplicateKeyError(error)) {
        throw new ConflictError({
          message: `A task with idempotency key "${input.idempotencyKey}" already exists.`,
          code: "TASK_IDEMPOTENCY_KEY_ALREADY_EXISTS",
          details: {
            idempotencyKey: input.idempotencyKey,
          },
          cause: error,
        });
      }

      throw error;
    }
  }

  async getTaskById(taskId: string): Promise<TaskRecord | null> {
    const task = await TaskModel.findById(toObjectId(taskId, "taskId")).exec();
    return task ? mapTask(task) : null;
  }

  async requireTaskById(taskId: string): Promise<TaskRecord> {
    const task = await this.getTaskById(taskId);

    if (!task) {
      throw new NotFoundError({
        message: `Task not found: ${taskId}`,
        code: "TASK_NOT_FOUND",
        details: { taskId },
      });
    }

    return task;
  }

  async getTaskByIdempotencyKey(
    idempotencyKey: string,
  ): Promise<TaskRecord | null> {
    const task = await TaskModel.findOne({
      idempotencyKey: idempotencyKey.trim(),
    }).exec();

    return task ? mapTask(task) : null;
  }

  async updateTask(
    taskId: string,
    updates: UpdateTaskInput,
  ): Promise<TaskRecord> {
    const needsMilestoneValidation =
      typeof updates.milestoneId === "string" ||
      updates.dependencies !== undefined;

    const current = needsMilestoneValidation
      ? await this.requireTaskById(taskId)
      : null;

    const updatePayload: Record<string, unknown> = {};

    if (typeof updates.milestoneId === "string" && current) {
      await this.assertMilestoneBelongsToProject({
        projectId: current.projectId,
        milestoneId: updates.milestoneId,
      });

      updatePayload.milestoneId = toObjectId(
        updates.milestoneId,
        "milestoneId",
      );
    }

    if (typeof updates.parentTaskId === "string") {
      updatePayload.parentTaskId = toObjectId(
        updates.parentTaskId,
        "parentTaskId",
      );
    }

    if (updates.dependencies !== undefined && current) {
      const nextMilestoneId =
        typeof updates.milestoneId === "string"
          ? updates.milestoneId
          : current.milestoneId;

      const nextDependencies = dedupeStringIds(updates.dependencies);

      await this.assertDependenciesAreValid({
        projectId: current.projectId,
        milestoneId: nextMilestoneId,
        dependencyIds: nextDependencies,
        currentTaskId: taskId,
      });

      updatePayload.dependencies = nextDependencies.map((dependencyId) =>
        toObjectId(dependencyId, "dependencyId"),
      );
    } else if (typeof updates.milestoneId === "string" && current) {
      await this.assertDependenciesAreValid({
        projectId: current.projectId,
        milestoneId: updates.milestoneId,
        dependencyIds: current.dependencies,
        currentTaskId: taskId,
      });
    }

    if (typeof updates.status === "string") {
      updatePayload.status = updates.status;
    }

    if (typeof updates.attemptCount === "number") {
      updatePayload.attemptCount = updates.attemptCount;
    }

    if (typeof updates.maxAttempts === "number") {
      updatePayload.maxAttempts = updates.maxAttempts;
    }

    if (typeof updates.sessionName === "string") {
      const trimmedSessionName = updates.sessionName.trim();
      if (trimmedSessionName.length > 0) {
        updatePayload.sessionName = trimmedSessionName;
      }
    }

    if (typeof updates.sessionCount === "number") {
      updatePayload.sessionCount = updates.sessionCount;
    }

    if (typeof updates.maxSessions === "number") {
      updatePayload.maxSessions = updates.maxSessions;
    }

    if (updates.nextRetryAt instanceof Date) {
      updatePayload.nextRetryAt = updates.nextRetryAt;
    }

    if (typeof updates.lastError === "string") {
      updatePayload.lastError = updates.lastError.trim();
    }

    if (typeof updates.retryable === "boolean") {
      updatePayload.retryable = updates.retryable;
    }

    if (typeof updates.sequence === "number") {
      updatePayload.sequence = updates.sequence;
    }

    if (updates.outputs !== undefined) {
      updatePayload.outputs = updates.outputs;
    }

    if (updates.artifacts !== undefined) {
      updatePayload.artifacts = updates.artifacts;
    }

    if (updates.errors !== undefined) {
      updatePayload.errors = updates.errors;
    }

    if (updates.target !== undefined) {
      updatePayload.target = {
        agentId: updates.target.agentId.trim(),
      };
    }

    const updated = await TaskModel.findByIdAndUpdate(
      toObjectId(taskId, "taskId"),
      updatePayload,
      {
        new: true,
        runValidators: true,
      },
    ).exec();

    if (!updated) {
      throw new NotFoundError({
        message: `Task not found: ${taskId}`,
        code: "TASK_NOT_FOUND",
        details: { taskId },
      });
    }

    return mapTask(updated);
  }

  async setStatus(taskId: string, status: TaskStatus): Promise<TaskRecord> {
    return this.updateTask(taskId, { status });
  }

  async markRunning(taskId: string): Promise<TaskRecord> {
    const updated = await TaskModel.findByIdAndUpdate(
      toObjectId(taskId, "taskId"),
      {
        $set: {
          status: "running",
        },
        $inc: {
          attemptCount: 1,
        },
        $unset: {
          nextRetryAt: 1,
        },
      },
      {
        new: true,
        runValidators: true,
      },
    ).exec();

    if (!updated) {
      throw new NotFoundError({
        message: `Task not found: ${taskId}`,
        code: "TASK_NOT_FOUND",
        details: { taskId },
      });
    }

    return mapTask(updated);
  }

  async markSucceeded(input: {
    taskId: string;
    outputs?: Record<string, unknown>;
    artifacts?: string[];
  }): Promise<TaskRecord> {
    const updated = await TaskModel.findByIdAndUpdate(
      toObjectId(input.taskId, "taskId"),
      {
        $set: {
          status: "succeeded",
          ...(input.outputs !== undefined ? { outputs: input.outputs } : {}),
          ...(input.artifacts !== undefined
            ? { artifacts: input.artifacts }
            : {}),
          errors: [],
        },
        $unset: {
          nextRetryAt: 1,
          lastError: 1,
        },
      },
      {
        new: true,
        runValidators: true,
      },
    ).exec();

    if (!updated) {
      throw new NotFoundError({
        message: `Task not found: ${input.taskId}`,
        code: "TASK_NOT_FOUND",
        details: { taskId: input.taskId },
      });
    }

    return mapTask(updated);
  }

  async markFailed(input: {
    taskId: string;
    errors: string[];
    outputs?: Record<string, unknown>;
    artifacts?: string[];
  }): Promise<TaskRecord> {
    return this.updateTask(input.taskId, {
      status: "failed",
      errors: input.errors,
      ...(input.outputs !== undefined ? { outputs: input.outputs } : {}),
      ...(input.artifacts !== undefined ? { artifacts: input.artifacts } : {}),
      ...(input.errors[0] ? { lastError: input.errors[0] } : {}),
    });
  }

  async markFailedExhausted(input: {
    taskId: string;
    errors: string[];
    outputs?: Record<string, unknown>;
    artifacts?: string[];
  }): Promise<TaskRecord> {
    const lastError = input.errors[input.errors.length - 1];

    const updated = await TaskModel.findByIdAndUpdate(
      toObjectId(input.taskId, "taskId"),
      {
        $set: {
          status: "failed",
          errors: input.errors,
          ...(typeof lastError === "string" && lastError.trim().length > 0
            ? { lastError: lastError.trim() }
            : {}),
          ...(input.outputs !== undefined ? { outputs: input.outputs } : {}),
          ...(input.artifacts !== undefined
            ? { artifacts: input.artifacts }
            : {}),
        },
        $unset: {
          nextRetryAt: 1,
        },
      },
      {
        new: true,
        runValidators: true,
      },
    ).exec();

    if (!updated) {
      throw new NotFoundError({
        message: `Task not found: ${input.taskId}`,
        code: "TASK_NOT_FOUND",
        details: { taskId: input.taskId },
      });
    }

    return mapTask(updated);
  }

  async requeueTask(input: {
    taskId: string;
    error: string;
    nextRetryAt?: Date;
    outputs?: Record<string, unknown>;
    artifacts?: string[];
  }): Promise<TaskRecord> {
    const current = await this.requireTaskById(input.taskId);

    const nextErrors = [...current.errors, input.error.trim()].filter(
      (value) => value.length > 0,
    );

    const updated = await TaskModel.findByIdAndUpdate(
      toObjectId(input.taskId, "taskId"),
      {
        $set: {
          status: "queued",
          errors: nextErrors,
          lastError: input.error.trim(),
          ...(input.nextRetryAt instanceof Date
            ? { nextRetryAt: input.nextRetryAt }
            : {}),
          ...(input.outputs !== undefined ? { outputs: input.outputs } : {}),
          ...(input.artifacts !== undefined
            ? { artifacts: input.artifacts }
            : {}),
        },
        ...(input.nextRetryAt instanceof Date
          ? {}
          : {
              $unset: {
                nextRetryAt: 1,
              },
            }),
      },
      {
        new: true,
        runValidators: true,
      },
    ).exec();

    if (!updated) {
      throw new NotFoundError({
        message: `Task not found: ${input.taskId}`,
        code: "TASK_NOT_FOUND",
        details: { taskId: input.taskId },
      });
    }

    return mapTask(updated);
  }

  async cancelTask(taskId: string, reason?: string): Promise<TaskRecord> {
    const updated = await TaskModel.findByIdAndUpdate(
      toObjectId(taskId, "taskId"),
      {
        $set: {
          status: "canceled",
          ...(typeof reason === "string" && reason.trim().length > 0
            ? {
                errors: [reason.trim()],
                lastError: reason.trim(),
              }
            : {}),
        },
        $unset: {
          nextRetryAt: 1,
        },
      },
      {
        new: true,
        runValidators: true,
      },
    ).exec();

    if (!updated) {
      throw new NotFoundError({
        message: `Task not found: ${taskId}`,
        code: "TASK_NOT_FOUND",
        details: { taskId },
      });
    }

    return mapTask(updated);
  }

  async listTasks(input: ListTasksInput = {}): Promise<TaskRecord[]> {
    const filter = this.buildFilter(input);
    const limit = Math.min(Math.max(input.limit ?? 25, 1), 100);
    const skip = Math.max(input.skip ?? 0, 0);

    const sort: [string, 1 | -1][] =
      input.status === "queued"
        ? [
            ["sequence", 1],
            ["createdAt", 1],
          ]
        : [["updatedAt", -1]];

    const tasks = await TaskModel.find(filter)
      .sort(sort)
      .skip(skip)
      .limit(limit)
      .exec();

    return tasks.map(mapTask);
  }

  async countTasks(input: CountTasksInput = {}): Promise<number> {
    const filter = this.buildFilter(input);
    return TaskModel.countDocuments(filter).exec();
  }

  async listRunnableTasks(input?: {
    jobId?: string;
    agentId?: string;
    milestoneId?: string;
    limit?: number;
    ignoreRetryAt?: boolean;
  }): Promise<TaskRecord[]> {
    const now = new Date();

    const filter: QueryFilter<TaskModelType> = {
      status: "queued",
      ...(!input?.ignoreRetryAt
        ? {
            $or: [
              { nextRetryAt: { $exists: false } },
              { nextRetryAt: { $lte: now } },
            ],
          }
        : {}),
    };

    if (typeof input?.jobId === "string") {
      filter.jobId = toObjectId(input.jobId, "jobId");
    }

    if (typeof input?.agentId === "string") {
      filter["target.agentId"] = input.agentId.trim();
    }

    if (typeof input?.milestoneId === "string") {
      filter.milestoneId = toObjectId(input.milestoneId, "milestoneId");
    }

    const limit = Math.min(Math.max(input?.limit ?? 25, 1), 100);
    const fetchLimit = Math.min(limit * 5, 200);

    const candidates = await TaskModel.find(filter)
      .sort({ sequence: 1, nextRetryAt: 1, createdAt: 1 })
      .limit(fetchLimit)
      .exec();

    const runnable: TaskRecord[] = [];

    for (const candidate of candidates) {
      const mapped = mapTask(candidate);

      if (await this.areDependenciesSatisfied(mapped)) {
        runnable.push(mapped);
      }

      if (runnable.length >= limit) {
        break;
      }
    }

    return runnable;
  }

  async listNextRunnableTask(input?: {
    jobId?: string;
    agentId?: string;
    milestoneId?: string;
    ignoreRetryAt?: boolean;
  }): Promise<TaskRecord | null> {
    const tasks = await this.listRunnableTasks({
      ...(typeof input?.jobId === "string" ? { jobId: input.jobId } : {}),
      ...(typeof input?.agentId === "string" ? { agentId: input.agentId } : {}),
      ...(typeof input?.milestoneId === "string"
        ? { milestoneId: input.milestoneId }
        : {}),
      ...(typeof input?.ignoreRetryAt === "boolean"
        ? { ignoreRetryAt: input.ignoreRetryAt }
        : {}),
      limit: 1,
    });

    return tasks[0] ?? null;
  }

  private buildFilter(
    input: CountTasksInput | ListTasksInput,
  ): QueryFilter<TaskModelType> {
    const filter: QueryFilter<TaskModelType> = {};

    if (typeof input.jobId === "string") {
      filter.jobId = toObjectId(input.jobId, "jobId");
    }

    if (typeof input.projectId === "string") {
      filter.projectId = toObjectId(input.projectId, "projectId");
    }

    if (typeof input.milestoneId === "string") {
      filter.milestoneId = toObjectId(input.milestoneId, "milestoneId");
    }

    if (typeof input.parentTaskId === "string") {
      filter.parentTaskId = toObjectId(input.parentTaskId, "parentTaskId");
    }

    if (input.status) {
      filter.status = input.status;
    }

    if (input.intent) {
      filter.intent = input.intent;
    }

    if (typeof input.agentId === "string") {
      filter["target.agentId"] = input.agentId.trim();
    }

    if (typeof input.sessionName === "string") {
      filter.sessionName = input.sessionName.trim();
    }

    if (typeof input.sessionCount === "number") {
      filter.sessionCount = input.sessionCount;
    }

    if (typeof input.maxSessions === "number") {
      filter.maxSessions = input.maxSessions;
    }

    return filter;
  }

  private async assertMilestoneBelongsToProject(input: {
    projectId: string;
    milestoneId: string;
  }): Promise<void> {
    const milestone = await MilestoneModel.findById(
      toObjectId(input.milestoneId, "milestoneId"),
    ).exec();

    if (!milestone) {
      throw new NotFoundError({
        message: `Milestone not found: ${input.milestoneId}`,
        code: "MILESTONE_NOT_FOUND",
        details: {
          milestoneId: input.milestoneId,
        },
      });
    }

    if (milestone.projectId.toString() !== input.projectId) {
      throw new ValidationError({
        message: "Task milestone must belong to the same project.",
        code: "TASK_MILESTONE_PROJECT_MISMATCH",
        statusCode: 400,
        details: {
          projectId: input.projectId,
          milestoneId: input.milestoneId,
        },
      });
    }
  }

  private async assertDependenciesAreValid(input: {
    projectId: string;
    milestoneId: string;
    dependencyIds: string[];
    currentTaskId?: string;
  }): Promise<void> {
    const dependencyIds = dedupeStringIds(input.dependencyIds);

    if (dependencyIds.length === 0) {
      return;
    }

    if (
      typeof input.currentTaskId === "string" &&
      dependencyIds.includes(input.currentTaskId)
    ) {
      throw new ValidationError({
        message: "A task cannot depend on itself.",
        code: "TASK_SELF_DEPENDENCY",
        statusCode: 400,
        details: {
          taskId: input.currentTaskId,
        },
      });
    }

    const dependencyObjectIds = dependencyIds.map((dependencyId) =>
      toObjectId(dependencyId, "dependencyId"),
    );

    const dependencyTasks = await TaskModel.find({
      _id: { $in: dependencyObjectIds },
    }).exec();

    if (dependencyTasks.length !== dependencyObjectIds.length) {
      const foundIds = new Set(dependencyTasks.map((task) => task.id));
      const missingDependencyId = dependencyIds.find(
        (dependencyId) => !foundIds.has(dependencyId),
      );

      throw new NotFoundError({
        message: `Dependency task not found: ${missingDependencyId ?? "unknown"}`,
        code: "TASK_DEPENDENCY_NOT_FOUND",
        details: {
          dependencyIds,
        },
      });
    }

    for (const dependencyTask of dependencyTasks) {
      if (dependencyTask.projectId.toString() !== input.projectId) {
        throw new ValidationError({
          message: "Task dependencies must belong to the same project.",
          code: "TASK_DEPENDENCY_PROJECT_MISMATCH",
          statusCode: 400,
          details: {
            projectId: input.projectId,
            dependencyTaskId: dependencyTask.id,
          },
        });
      }

      if (dependencyTask.milestoneId.toString() !== input.milestoneId) {
        throw new ValidationError({
          message: "Task dependencies must belong to the same milestone.",
          code: "TASK_DEPENDENCY_MILESTONE_MISMATCH",
          statusCode: 400,
          details: {
            milestoneId: input.milestoneId,
            dependencyTaskId: dependencyTask.id,
          },
        });
      }
    }
  }

  private async areDependenciesSatisfied(task: TaskRecord): Promise<boolean> {
    if (task.dependencies.length === 0) {
      return true;
    }

    const incompleteDependencyCount = await TaskModel.countDocuments({
      _id: {
        $in: task.dependencies.map((dependencyId) =>
          toObjectId(dependencyId, "dependencyId"),
        ),
      },
      status: { $ne: "succeeded" },
    }).exec();

    return incompleteDependencyCount === 0;
  }

  private isDuplicateKeyError(error: unknown): boolean {
    if (
      typeof error === "object" &&
      error !== null &&
      "name" in error &&
      "code" in error
    ) {
      const candidate = error as {
        name?: unknown;
        code?: unknown;
      };

      return candidate.name === "MongoServerError" && candidate.code === 11000;
    }

    return false;
  }
}

export default TaskService;
