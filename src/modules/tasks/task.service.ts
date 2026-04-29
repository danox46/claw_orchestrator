import { type QueryFilter } from "mongoose";
import { ConflictError } from "../../shared/errors/conflict-error";
import { NotFoundError } from "../../shared/errors/not-found-error";
import { ValidationError } from "../../shared/errors/validation-error";
import TaskModel, { type TaskModelType } from "./task.model";
import { TaskValidationService } from "./task-validation.service";
import {
  assertValidCreateTaskInput,
  asRecordValue,
  buildAtomicBatchUpdatePayload,
  buildFilter,
  dedupeStringIds,
  getBatchOwnerLabel,
  isDuplicateKeyError,
  mapTask,
  normalizeCreateTaskInput,
  resolveConcreteTaskExecutionUpdates,
  toObjectId,
  wrapMilestonePlanningBatchCommitError,
} from "./task.helpers";
import type {
  CommitMilestonePlanningBatchInput,
  CommitMilestonePlanningBatchResult,
  CommittedMilestonePlanningBatchTask,
  CountTasksInput,
  CreateTaskInput,
  ListTasksInput,
  TaskRecord,
  TaskStatus,
  TasksServicePort,
  TaskValidationServicePort,
  UpdateConcreteTaskExecutionInput,
  UpdateTaskInput,
  ValidateMilestonePlanningBatchResult,
} from "./task.types";

export type {
  AssertTaskDependenciesInput,
  AssertTaskMilestoneBelongsToProjectInput,
  CommitMilestonePlanningBatchInput,
  CommitMilestonePlanningBatchResult,
  CommittedMilestonePlanningBatchTask,
  CountTasksInput,
  CreateTaskInput,
  ListTasksInput,
  MilestonePlanningBatchCreate,
  MilestonePlanningBatchMutation,
  MilestonePlanningBatchOperationKind,
  MilestonePlanningBatchOperationStage,
  MilestonePlanningBatchUpdate,
  MilestonePlanningBatchValidationIssue,
  ResolveTaskBatchDependencyIdsInput,
  TaskRecord,
  TasksServicePort,
  TaskValidationServicePort,
  UpdateConcreteTaskExecutionInput,
  UpdateTaskInput,
  ValidateMilestonePlanningBatchResult,
  ValidateTaskBatchDependencyTargetsInput,
} from "./task.types";

export class TaskService implements TasksServicePort {
  constructor(
    private readonly validation: TaskValidationServicePort = new TaskValidationService(),
  ) {}

  async createTask(input: CreateTaskInput): Promise<TaskRecord> {
    try {
      assertValidCreateTaskInput(input);
      const normalized = normalizeCreateTaskInput(input);

      await this.validation.assertMilestoneBelongsToProject({
        projectId: input.projectId,
        milestoneId: input.milestoneId,
      });

      if (normalized.dependencies.length > 0) {
        await this.validation.assertDependenciesAreValid({
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
      if (isDuplicateKeyError(error)) {
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
    const needsCurrentTask =
      needsMilestoneValidation ||
      updates.inputs !== undefined ||
      updates.prompt !== undefined ||
      updates.testingCriteria !== undefined ||
      updates.inputAcceptanceCriteria !== undefined;

    const current = needsCurrentTask
      ? await this.requireTaskById(taskId)
      : null;

    const updatePayload: Record<string, unknown> = {};

    if (typeof updates.milestoneId === "string" && current) {
      await this.validation.assertMilestoneBelongsToProject({
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

      await this.validation.assertDependenciesAreValid({
        projectId: current.projectId,
        milestoneId: nextMilestoneId,
        dependencyIds: nextDependencies,
        currentTaskId: taskId,
      });

      updatePayload.dependencies = nextDependencies.map((dependencyId) =>
        toObjectId(dependencyId, "dependencyId"),
      );
    } else if (typeof updates.milestoneId === "string" && current) {
      await this.validation.assertDependenciesAreValid({
        projectId: current.projectId,
        milestoneId: updates.milestoneId,
        dependencyIds: current.dependencies,
        currentTaskId: taskId,
      });
    }

    if (typeof updates.intent === "string") {
      updatePayload.intent = updates.intent;
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

    if (current) {
      let nextInputs: Record<string, unknown> | null =
        updates.inputs !== undefined ? asRecordValue(updates.inputs) : null;

      const ensureNextInputs = (): Record<string, unknown> => {
        if (!nextInputs) {
          nextInputs = { ...asRecordValue(current.inputs) };
        }

        return nextInputs;
      };

      if (updates.prompt !== undefined) {
        ensureNextInputs().prompt = updates.prompt;
      }

      if (updates.testingCriteria !== undefined) {
        ensureNextInputs().testingCriteria = [...updates.testingCriteria];
      }

      if (updates.inputAcceptanceCriteria !== undefined) {
        ensureNextInputs().acceptanceCriteria = [
          ...updates.inputAcceptanceCriteria,
        ];
      }

      if (nextInputs) {
        updatePayload.inputs = nextInputs;
      }
    } else if (updates.inputs !== undefined) {
      updatePayload.inputs = asRecordValue(updates.inputs);
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

  async updateConcreteTaskExecution(
    taskId: string,
    updates: UpdateConcreteTaskExecutionInput,
  ): Promise<TaskRecord> {
    const resolved = resolveConcreteTaskExecutionUpdates(updates);

    if (
      resolved.prompt === undefined &&
      resolved.testingCriteria === undefined &&
      resolved.acceptanceCriteria === undefined &&
      resolved.requiredArtifacts === undefined
    ) {
      return this.requireTaskById(taskId);
    }

    return this.updateTask(taskId, {
      ...(resolved.prompt !== undefined ? { prompt: resolved.prompt } : {}),
      ...(resolved.testingCriteria !== undefined
        ? { testingCriteria: resolved.testingCriteria }
        : {}),
      ...(resolved.acceptanceCriteria !== undefined
        ? { inputAcceptanceCriteria: resolved.acceptanceCriteria }
        : {}),
      ...(resolved.acceptanceCriteria !== undefined
        ? { acceptanceCriteria: resolved.acceptanceCriteria }
        : {}),
      ...(resolved.requiredArtifacts !== undefined
        ? { requiredArtifacts: resolved.requiredArtifacts }
        : {}),
    });
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
    const filter = buildFilter(input);
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
    const filter = buildFilter(input);
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

      if (await this.validation.areDependenciesSatisfied(mapped)) {
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

  async validateMilestonePlanningBatch(
    batch: CommitMilestonePlanningBatchInput,
  ): Promise<ValidateMilestonePlanningBatchResult> {
    return this.validation.validateMilestonePlanningBatch(batch);
  }

  async commitMilestonePlanningBatch(
    batch: CommitMilestonePlanningBatchInput,
  ): Promise<CommitMilestonePlanningBatchResult> {
    const validation = await this.validateMilestonePlanningBatch(batch);

    if (!validation.ok) {
      throw new ValidationError({
        message: `Milestone planning batch failed validation with ${validation.issues.length} issue${validation.issues.length === 1 ? "" : "s"}.`,
        code: "TASK_BATCH_VALIDATION_FAILED",
        statusCode: 422,
        details: {
          plannerTaskId: batch.plannerTaskId,
          milestoneId: batch.milestoneId,
          issues: validation.issues,
        },
      });
    }

    const referenceToTaskId = new Map<string, string>();
    const createdTaskIds: string[] = [];
    const updatedTaskIds: string[] = [];
    const createdTasks: CommittedMilestonePlanningBatchTask[] = [];
    const updatedTasks: CommittedMilestonePlanningBatchTask[] = [];
    let reviewTaskId: string | null = null;
    let reviewTaskCreated = false;
    let reviewTaskUpdated = false;

    try {
      for (const [updateIndex, update] of batch.updates.entries()) {
        const existing = await TaskModel.findById(
          toObjectId(update.taskId, "taskId"),
        ).exec();

        if (!existing) {
          throw new NotFoundError({
            message: `Task not found: ${update.taskId}`,
            code: "TASK_NOT_FOUND",
            details: { taskId: update.taskId },
          });
        }

        for (const referenceKey of dedupeStringIds(update.referenceKeys)) {
          referenceToTaskId.set(referenceKey, existing.id);
        }

        if (existing.intent === "review_milestone") {
          reviewTaskId = existing.id;
        }
      }
    } catch (error: unknown) {
      throw wrapMilestonePlanningBatchCommitError(error, {
        batch,
        stage: "seed_updates",
        operationKind: "update",
        operationIndex: -1,
        ownerLabel: "seed_updates",
        createdTaskIds,
        updatedTaskIds,
        createdTasks,
        updatedTasks,
        reviewTaskId,
      });
    }

    for (const [createIndex, create] of batch.creates.entries()) {
      const ownerLabel = getBatchOwnerLabel({
        referenceKeys: create.referenceKeys,
        fallback: create.task.idempotencyKey,
      });

      try {
        assertValidCreateTaskInput(create.task);
        const normalized = normalizeCreateTaskInput(create.task);
        const dependencyIds = await this.validation.resolveBatchDependencyIds(
          {
            explicitTaskIds: create.dependencyTaskIds,
            dependencyRefs: create.dependencyRefs,
            projectId: batch.projectId,
            milestoneId: batch.milestoneId,
          },
          referenceToTaskId,
        );

        await this.validation.assertMilestoneBelongsToProject({
          projectId: create.task.projectId,
          milestoneId: create.task.milestoneId,
        });

        const created = await TaskModel.create({
          jobId: toObjectId(create.task.jobId, "jobId"),
          projectId: toObjectId(create.task.projectId, "projectId"),
          milestoneId: toObjectId(create.task.milestoneId, "milestoneId"),
          ...(typeof create.task.parentTaskId === "string"
            ? {
                parentTaskId: toObjectId(
                  create.task.parentTaskId,
                  "parentTaskId",
                ),
              }
            : {}),
          dependencies: dependencyIds.map((dependencyId) =>
            toObjectId(dependencyId, "dependencyId"),
          ),
          issuer: {
            kind: create.task.issuer.kind,
            id: create.task.issuer.id.trim(),
            ...(typeof create.task.issuer.sessionId === "string"
              ? { sessionId: create.task.issuer.sessionId.trim() }
              : {}),
            ...(typeof create.task.issuer.role === "string"
              ? { role: create.task.issuer.role.trim() }
              : {}),
          },
          target: {
            agentId: create.task.target.agentId.trim(),
          },
          intent: create.task.intent,
          inputs: create.task.inputs,
          constraints: {
            toolProfile: create.task.constraints.toolProfile.trim(),
            sandbox: create.task.constraints.sandbox,
            ...(typeof create.task.constraints.maxTokens === "number"
              ? { maxTokens: create.task.constraints.maxTokens }
              : {}),
            ...(typeof create.task.constraints.maxCost === "number"
              ? { maxCost: create.task.constraints.maxCost }
              : {}),
          },
          requiredArtifacts: normalized.requiredArtifacts,
          acceptanceCriteria: normalized.acceptanceCriteria,
          idempotencyKey: create.task.idempotencyKey.trim(),
          status: normalized.status,
          attemptCount: normalized.attemptCount,
          maxAttempts: normalized.maxAttempts,
          ...(normalized.sessionName
            ? { sessionName: normalized.sessionName }
            : {}),
          sessionCount: normalized.sessionCount,
          maxSessions: normalized.maxSessions,
          ...(create.task.nextRetryAt instanceof Date
            ? { nextRetryAt: create.task.nextRetryAt }
            : {}),
          ...(typeof create.task.lastError === "string" &&
          create.task.lastError.trim().length > 0
            ? { lastError: create.task.lastError.trim() }
            : {}),
          retryable: normalized.retryable,
          sequence: normalized.sequence,
          ...(create.task.outputs !== undefined
            ? { outputs: create.task.outputs }
            : {}),
          artifacts: normalized.artifacts,
          errors: normalized.errors,
        });

        createdTaskIds.push(created.id);
        createdTasks.push({
          stage: "create",
          operationKind: "create",
          operationIndex: createIndex,
          taskId: created.id,
          ownerLabel,
          referenceKeys: dedupeStringIds(create.referenceKeys),
          dependencyTaskIds: dedupeStringIds(create.dependencyTaskIds),
          dependencyRefs: dedupeStringIds(create.dependencyRefs),
          taskIntent: created.intent,
          idempotencyKey: created.idempotencyKey,
        });

        for (const referenceKey of dedupeStringIds(create.referenceKeys)) {
          referenceToTaskId.set(referenceKey, created.id);
        }

        if (created.intent === "review_milestone") {
          reviewTaskId = created.id;
          reviewTaskCreated = true;
        }
      } catch (error: unknown) {
        throw wrapMilestonePlanningBatchCommitError(error, {
          batch,
          stage: "create",
          operationKind: "create",
          operationIndex: createIndex,
          ownerLabel,
          taskIntent: create.task.intent,
          idempotencyKey: create.task.idempotencyKey,
          referenceKeys: create.referenceKeys,
          dependencyTaskIds: create.dependencyTaskIds,
          dependencyRefs: create.dependencyRefs,
          createdTaskIds,
          updatedTaskIds,
          createdTasks,
          updatedTasks,
          reviewTaskId,
        });
      }
    }

    for (const [updateIndex, update] of batch.updates.entries()) {
      const ownerLabel = getBatchOwnerLabel({
        referenceKeys: update.referenceKeys,
        fallback: update.taskId,
      });
      const existingTask = await this.getTaskById(update.taskId);

      try {
        const dependencyIds = await this.validation.resolveBatchDependencyIds(
          {
            explicitTaskIds: update.dependencyTaskIds,
            dependencyRefs: update.dependencyRefs,
            projectId: batch.projectId,
            milestoneId: batch.milestoneId,
            currentTaskId: update.taskId,
          },
          referenceToTaskId,
        );

        const updatePayload = buildAtomicBatchUpdatePayload({
          patch: update.patch,
          dependencyIds,
        });

        const updated = await TaskModel.findByIdAndUpdate(
          toObjectId(update.taskId, "taskId"),
          updatePayload,
          {
            new: true,
            runValidators: true,
          },
        ).exec();

        if (!updated) {
          throw new NotFoundError({
            message: `Task not found: ${update.taskId}`,
            code: "TASK_NOT_FOUND",
            details: { taskId: update.taskId },
          });
        }

        updatedTaskIds.push(updated.id);
        updatedTasks.push({
          stage: "update",
          operationKind: "update",
          operationIndex: updateIndex,
          taskId: updated.id,
          ownerLabel,
          referenceKeys: dedupeStringIds(update.referenceKeys),
          dependencyTaskIds: dedupeStringIds(update.dependencyTaskIds),
          dependencyRefs: dedupeStringIds(update.dependencyRefs),
          taskIntent: updated.intent,
          idempotencyKey: updated.idempotencyKey,
        });

        for (const referenceKey of dedupeStringIds(update.referenceKeys)) {
          referenceToTaskId.set(referenceKey, updated.id);
        }

        if (updated.intent === "review_milestone") {
          reviewTaskId = updated.id;
          reviewTaskUpdated = true;
        }
      } catch (error: unknown) {
        throw wrapMilestonePlanningBatchCommitError(error, {
          batch,
          stage: "update",
          operationKind: "update",
          operationIndex: updateIndex,
          ownerLabel,
          taskId: update.taskId,
          ...(existingTask?.intent ? { taskIntent: existingTask.intent } : {}),
          ...(typeof update.patch.idempotencyKey === "string" &&
          update.patch.idempotencyKey.trim().length > 0
            ? { idempotencyKey: update.patch.idempotencyKey.trim() }
            : existingTask?.idempotencyKey
              ? { idempotencyKey: existingTask.idempotencyKey }
              : {}),
          referenceKeys: update.referenceKeys,
          dependencyTaskIds: update.dependencyTaskIds,
          dependencyRefs: update.dependencyRefs,
          createdTaskIds,
          updatedTaskIds,
          createdTasks,
          updatedTasks,
          reviewTaskId,
        });
      }
    }

    if (!reviewTaskId) {
      throw wrapMilestonePlanningBatchCommitError(
        new ValidationError({
          message:
            "Milestone planning batch must include a review_milestone task.",
          code: "TASK_BATCH_REVIEW_TASK_MISSING",
          statusCode: 400,
          details: {
            plannerTaskId: batch.plannerTaskId,
            milestoneId: batch.milestoneId,
          },
        }),
        {
          batch,
          stage: "finalize",
          operationKind: "update",
          operationIndex: -1,
          ownerLabel: "review_milestone",
          createdTaskIds,
          updatedTaskIds,
          createdTasks,
          updatedTasks,
          reviewTaskId,
        },
      );
    }

    return {
      createdTaskIds,
      updatedTaskIds,
      reviewTaskId,
      reviewTaskCreated,
      reviewTaskUpdated,
      createdTasks,
      updatedTasks,
    };
  }
}

export default TaskService;
