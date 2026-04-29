import { type ClientSession } from "mongoose";
import { NotFoundError } from "../../shared/errors/not-found-error";
import { ValidationError } from "../../shared/errors/validation-error";
import MilestoneModel from "../milestones/milestone.model";
import TaskModel from "./task.model";
import {
  dedupeStringIds,
  extractIdempotencyKeyFromBatchReference,
  getBatchOwnerLabel,
  getBatchShapeValidationIssues,
  getCreateTaskValidationIssues,
  mapTask,
  toMilestoneBatchValidationIssue,
  toObjectId,
} from "./task.helpers";
import type {
  AssertTaskDependenciesInput,
  AssertTaskMilestoneBelongsToProjectInput,
  CommitMilestonePlanningBatchInput,
  MilestonePlanningBatchValidationIssue,
  ResolveTaskBatchDependencyIdsInput,
  TaskRecord,
  TaskValidationServicePort,
  ValidateMilestonePlanningBatchResult,
  ValidateTaskBatchDependencyTargetsInput,
} from "./task.types";

export class TaskValidationService implements TaskValidationServicePort {
  async validateMilestonePlanningBatch(
    batch: CommitMilestonePlanningBatchInput,
  ): Promise<ValidateMilestonePlanningBatchResult> {
    const issues: MilestonePlanningBatchValidationIssue[] = [];
    const batchShapeIssues = getBatchShapeValidationIssues(batch);

    if (batchShapeIssues.length > 0) {
      return {
        ok: false,
        issues: batchShapeIssues,
      };
    }

    try {
      await this.assertMilestoneBelongsToProject({
        projectId: batch.projectId,
        milestoneId: batch.milestoneId,
      });
    } catch (error: unknown) {
      issues.push(
        toMilestoneBatchValidationIssue(error, {
          code: "TASK_BATCH_MILESTONE_INVALID",
          message:
            "Milestone planning batch references an invalid milestone or project.",
          stage: "validation",
          details: {
            plannerTaskId: batch.plannerTaskId,
            projectId: batch.projectId,
            milestoneId: batch.milestoneId,
          },
        }),
      );

      return {
        ok: false,
        issues,
      };
    }

    const referenceOwner = new Map<string, string>();
    const createIdempotencyKeyOwner = new Map<string, string>();
    const updateTaskIds = dedupeStringIds(
      batch.updates.map((update) => update.taskId),
    );
    const existingUpdatedTasks = new Map<string, TaskRecord>();
    let reviewTaskPresent = false;

    for (const [createIndex, create] of batch.creates.entries()) {
      const ownerLabel = getBatchOwnerLabel({
        referenceKeys: create.referenceKeys,
        fallback: create.task.idempotencyKey,
      });

      const createTaskIssues = getCreateTaskValidationIssues(create.task);
      issues.push(
        ...createTaskIssues.map((issue) => ({
          code: issue.code,
          message: issue.message,
          field: issue.field,
          stage: "validation" as const,
          operationKind: "create" as const,
          operationIndex: createIndex,
          ownerLabel,
          taskIntent: create.task.intent,
          idempotencyKey: create.task.idempotencyKey.trim(),
          referenceKey: ownerLabel,
          ...(issue.details ? { details: issue.details } : {}),
        })),
      );
    }

    for (const [createIndex, create] of batch.creates.entries()) {
      const ownerLabel = getBatchOwnerLabel({
        referenceKeys: create.referenceKeys,
        fallback: create.task.idempotencyKey,
      });
      const normalizedReferenceKeys = dedupeStringIds(create.referenceKeys);

      for (const referenceKey of normalizedReferenceKeys) {
        const existingOwner = referenceOwner.get(referenceKey);
        if (existingOwner) {
          issues.push({
            code: "TASK_BATCH_REFERENCE_KEY_DUPLICATE",
            message: `Duplicate batch reference key "${referenceKey}".`,
            referenceKey,
            stage: "validation",
            operationKind: "create",
            operationIndex: createIndex,
            ownerLabel,
            taskIntent: create.task.intent,
            idempotencyKey: create.task.idempotencyKey,
            details: {
              firstOwner: existingOwner,
              secondOwner: ownerLabel,
            },
          });
          continue;
        }

        referenceOwner.set(referenceKey, ownerLabel);
      }

      const idempotencyKey = create.task.idempotencyKey.trim();
      const existingCreateOwner = createIdempotencyKeyOwner.get(idempotencyKey);
      if (existingCreateOwner) {
        issues.push({
          code: "TASK_BATCH_CREATE_IDEMPOTENCY_KEY_DUPLICATE",
          message: `Duplicate create idempotency key "${idempotencyKey}" in milestone planning batch.`,
          referenceKey: ownerLabel,
          field: "task.idempotencyKey",
          stage: "validation",
          operationKind: "create",
          operationIndex: createIndex,
          ownerLabel,
          taskIntent: create.task.intent,
          idempotencyKey,
          details: {
            firstOwner: existingCreateOwner,
            secondOwner: ownerLabel,
            idempotencyKey,
          },
        });
      } else {
        createIdempotencyKeyOwner.set(idempotencyKey, ownerLabel);
      }

      if (create.task.intent === "review_milestone") {
        reviewTaskPresent = true;
      }
    }

    for (const updateTaskId of updateTaskIds) {
      const existing = await this.getTaskById(updateTaskId);
      if (!existing) {
        issues.push({
          code: "TASK_BATCH_UPDATE_TARGET_NOT_FOUND",
          message: `Task not found: ${updateTaskId}`,
          taskId: updateTaskId,
          stage: "validation",
          operationKind: "update",
          details: { taskId: updateTaskId },
        });
        continue;
      }

      existingUpdatedTasks.set(updateTaskId, existing);
    }

    for (const [updateIndex, update] of batch.updates.entries()) {
      const existing = existingUpdatedTasks.get(update.taskId);
      const ownerLabel = getBatchOwnerLabel({
        referenceKeys: update.referenceKeys,
        fallback: update.taskId,
      });
      const normalizedReferenceKeys = dedupeStringIds(update.referenceKeys);

      for (const referenceKey of normalizedReferenceKeys) {
        const existingOwner = referenceOwner.get(referenceKey);
        if (existingOwner && existingOwner !== ownerLabel) {
          issues.push({
            code: "TASK_BATCH_REFERENCE_KEY_DUPLICATE",
            message: `Duplicate batch reference key "${referenceKey}".`,
            referenceKey,
            stage: "validation",
            operationKind: "update",
            operationIndex: updateIndex,
            ownerLabel,
            ...(existing?.intent ? { taskIntent: existing.intent } : {}),
            details: {
              firstOwner: existingOwner,
              secondOwner: ownerLabel,
            },
          });
          continue;
        }

        referenceOwner.set(referenceKey, ownerLabel);
      }

      if (existing && existing.intent === "review_milestone") {
        reviewTaskPresent = true;
      }
    }

    const referenceToTaskId = new Map<string, string>();
    for (const [updateIndex, update] of batch.updates.entries()) {
      const existing = existingUpdatedTasks.get(update.taskId);
      if (!existing) {
        continue;
      }

      for (const referenceKey of dedupeStringIds(update.referenceKeys)) {
        referenceToTaskId.set(referenceKey, existing._id);
      }

      if (!reviewTaskPresent && existing.intent === "review_milestone") {
        reviewTaskPresent = true;
      }

      if (
        typeof update.patch.idempotencyKey === "string" &&
        update.patch.idempotencyKey.trim().length > 0 &&
        update.patch.idempotencyKey.trim() !== existing.idempotencyKey
      ) {
        const existingByIdempotencyKey = await this.getTaskByIdempotencyKey(
          update.patch.idempotencyKey.trim(),
        );
        if (
          existingByIdempotencyKey &&
          existingByIdempotencyKey._id !== existing._id
        ) {
          issues.push({
            code: "TASK_BATCH_UPDATE_IDEMPOTENCY_KEY_ALREADY_EXISTS",
            message: `A task with idempotency key "${update.patch.idempotencyKey.trim()}" already exists.`,
            taskId: update.taskId,
            field: "patch.idempotencyKey",
            stage: "validation",
            operationKind: "update",
            operationIndex: updateIndex,
            ownerLabel: getBatchOwnerLabel({
              referenceKeys: update.referenceKeys,
              fallback: update.taskId,
            }),
            ...(existing.intent ? { taskIntent: existing.intent } : {}),
            idempotencyKey: update.patch.idempotencyKey.trim(),
            details: {
              idempotencyKey: update.patch.idempotencyKey.trim(),
              existingTaskId: existingByIdempotencyKey._id,
            },
          });
        }
      }
    }

    for (const [createIndex, create] of batch.creates.entries()) {
      const ownerLabel = getBatchOwnerLabel({
        referenceKeys: create.referenceKeys,
        fallback: create.task.idempotencyKey,
      });
      const trimmedIdempotencyKey = create.task.idempotencyKey.trim();
      const existingByIdempotencyKey = await this.getTaskByIdempotencyKey(
        trimmedIdempotencyKey,
      );
      if (existingByIdempotencyKey) {
        issues.push({
          code: "TASK_BATCH_IDEMPOTENCY_KEY_ALREADY_EXISTS",
          message: `A task with idempotency key "${trimmedIdempotencyKey}" already exists.`,
          referenceKey: ownerLabel,
          field: "task.idempotencyKey",
          stage: "validation",
          operationKind: "create",
          operationIndex: createIndex,
          ownerLabel,
          taskIntent: create.task.intent,
          idempotencyKey: trimmedIdempotencyKey,
          details: {
            idempotencyKey: trimmedIdempotencyKey,
            existingTaskId: existingByIdempotencyKey._id,
          },
        });
      }
    }

    for (const [createIndex, create] of batch.creates.entries()) {
      const ownerLabel = getBatchOwnerLabel({
        referenceKeys: create.referenceKeys,
        fallback: create.task.idempotencyKey,
      });
      const dependencyIssues = await this.validateBatchDependencyTargets({
        explicitTaskIds: create.dependencyTaskIds,
        dependencyRefs: create.dependencyRefs,
        projectId: batch.projectId,
        milestoneId: batch.milestoneId,
        ownerLabel,
        referenceToTaskId,
        operationIndex: createIndex,
        operationKind: "create",
        taskIntent: create.task.intent,
        idempotencyKey: create.task.idempotencyKey,
      });
      issues.push(...dependencyIssues);

      for (const referenceKey of dedupeStringIds(create.referenceKeys)) {
        referenceToTaskId.set(referenceKey, `pending:${ownerLabel}`);
      }
    }

    for (const [updateIndex, update] of batch.updates.entries()) {
      const ownerLabel = getBatchOwnerLabel({
        referenceKeys: update.referenceKeys,
        fallback: update.taskId,
      });
      const existingUpdatedIntent = existingUpdatedTasks.get(
        update.taskId,
      )?.intent;
      const dependencyIssues = await this.validateBatchDependencyTargets({
        explicitTaskIds: update.dependencyTaskIds,
        dependencyRefs: update.dependencyRefs,
        projectId: batch.projectId,
        milestoneId: batch.milestoneId,
        ownerLabel,
        referenceToTaskId,
        currentTaskId: update.taskId,
        operationIndex: updateIndex,
        operationKind: "update",
        ...(existingUpdatedIntent ? { taskIntent: existingUpdatedIntent } : {}),
        ...(typeof update.patch.idempotencyKey === "string"
          ? { idempotencyKey: update.patch.idempotencyKey.trim() }
          : {}),
      });
      issues.push(...dependencyIssues);
    }

    const actionableCreateCount = batch.creates.filter(
      (create) => create.task.intent !== "review_milestone",
    ).length;
    const actionableUpdateCount = batch.updates.filter((update) => {
      const existing = existingUpdatedTasks.get(update.taskId);
      return existing ? existing.intent !== "review_milestone" : false;
    }).length;

    if (actionableCreateCount + actionableUpdateCount === 0) {
      issues.push({
        code: "TASK_BATCH_ACTIONABLE_TASKS_EMPTY",
        message:
          "Milestone planning batch does not contain any actionable non-review tasks.",
        stage: "validation",
        details: {
          plannerTaskId: batch.plannerTaskId,
          milestoneId: batch.milestoneId,
          createCount: batch.creates.length,
          updateCount: batch.updates.length,
        },
      });
    }

    if (!reviewTaskPresent) {
      issues.push({
        code: "TASK_BATCH_REVIEW_TASK_MISSING",
        message:
          "Milestone planning batch must include a review_milestone task.",
        stage: "validation",
        details: {
          plannerTaskId: batch.plannerTaskId,
          milestoneId: batch.milestoneId,
        },
      });
    }

    return {
      ok: issues.length === 0,
      issues,
    };
  }

  private async getTaskById(taskId: string): Promise<TaskRecord | null> {
    const task = await TaskModel.findById(toObjectId(taskId, "taskId")).exec();
    return task ? mapTask(task) : null;
  }

  private async getTaskByIdempotencyKey(
    idempotencyKey: string,
  ): Promise<TaskRecord | null> {
    const task = await TaskModel.findOne({
      idempotencyKey: idempotencyKey.trim(),
    }).exec();

    return task ? mapTask(task) : null;
  }

  async assertMilestoneBelongsToProject(
    input: AssertTaskMilestoneBelongsToProjectInput,
    session?: ClientSession,
  ): Promise<void> {
    const milestone = await MilestoneModel.findById(
      toObjectId(input.milestoneId, "milestoneId"),
    )
      .session(session ?? null)
      .exec();

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

  async assertDependenciesAreValid(
    input: AssertTaskDependenciesInput,
    session?: ClientSession,
  ): Promise<void> {
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
    })
      .session(session ?? null)
      .exec();

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

  async resolveBatchDependencyIds(
    input: ResolveTaskBatchDependencyIdsInput,
    referenceToTaskId: Map<string, string>,
    session?: ClientSession,
  ): Promise<string[]> {
    const resolvedIds = new Set<string>(dedupeStringIds(input.explicitTaskIds));

    for (const dependencyRef of dedupeStringIds(input.dependencyRefs)) {
      let resolvedTaskId = referenceToTaskId.get(dependencyRef);

      if (!resolvedTaskId) {
        const resolvedByRef = await this.resolveTaskIdByBatchReference(
          dependencyRef,
          input.projectId,
          input.milestoneId,
          session,
        );

        if (resolvedByRef) {
          resolvedTaskId = resolvedByRef;
          referenceToTaskId.set(dependencyRef, resolvedByRef);
        }
      }

      if (!resolvedTaskId) {
        throw new ValidationError({
          message: `Unable to resolve task dependency reference: ${dependencyRef}`,
          code: "TASK_BATCH_DEPENDENCY_REFERENCE_UNRESOLVED",
          statusCode: 400,
          details: {
            dependencyRef,
            plannerProjectId: input.projectId,
            plannerMilestoneId: input.milestoneId,
          },
        });
      }

      resolvedIds.add(resolvedTaskId);
    }

    const dependencyIds = Array.from(resolvedIds);

    if (dependencyIds.length > 0) {
      await this.assertDependenciesAreValid(
        {
          projectId: input.projectId,
          milestoneId: input.milestoneId,
          dependencyIds,
          ...(typeof input.currentTaskId === "string"
            ? { currentTaskId: input.currentTaskId }
            : {}),
        },
        session,
      );
    }

    return dependencyIds;
  }

  private async resolveTaskIdByBatchReference(
    reference: string,
    projectId: string,
    milestoneId: string,
    session?: ClientSession,
  ): Promise<string | null> {
    const candidateIdempotencyKey =
      extractIdempotencyKeyFromBatchReference(reference);

    if (!candidateIdempotencyKey) {
      return null;
    }

    const task = await TaskModel.findOne({
      idempotencyKey: candidateIdempotencyKey,
      projectId: toObjectId(projectId, "projectId"),
      milestoneId: toObjectId(milestoneId, "milestoneId"),
    })
      .session(session ?? null)
      .exec();

    return task ? task.id : null;
  }

  private async validateBatchDependencyTargets(
    input: ValidateTaskBatchDependencyTargetsInput,
  ): Promise<MilestonePlanningBatchValidationIssue[]> {
    const issues: MilestonePlanningBatchValidationIssue[] = [];

    for (const dependencyRef of dedupeStringIds(input.dependencyRefs)) {
      const resolvedTaskId = input.referenceToTaskId.get(dependencyRef);
      if (resolvedTaskId) {
        continue;
      }

      const resolvedByRef = await this.resolveTaskIdByBatchReference(
        dependencyRef,
        input.projectId,
        input.milestoneId,
      );

      if (!resolvedByRef) {
        issues.push({
          code: "TASK_BATCH_DEPENDENCY_REFERENCE_UNRESOLVED",
          message: `Unable to resolve task dependency reference: ${dependencyRef}`,
          referenceKey: input.ownerLabel,
          field: "dependencyRefs",
          stage: "validation",
          ...(input.operationKind
            ? { operationKind: input.operationKind }
            : {}),
          ...(typeof input.operationIndex === "number"
            ? { operationIndex: input.operationIndex }
            : {}),
          ownerLabel: input.ownerLabel,
          ...(input.taskIntent ? { taskIntent: input.taskIntent } : {}),
          ...(input.idempotencyKey
            ? { idempotencyKey: input.idempotencyKey }
            : {}),
          details: {
            dependencyRef,
            projectId: input.projectId,
            milestoneId: input.milestoneId,
          },
        });
        continue;
      }

      input.referenceToTaskId.set(dependencyRef, resolvedByRef);
    }

    const explicitTaskIds = dedupeStringIds(input.explicitTaskIds);
    if (explicitTaskIds.length > 0) {
      try {
        await this.assertDependenciesAreValid({
          projectId: input.projectId,
          milestoneId: input.milestoneId,
          dependencyIds: explicitTaskIds,
          ...(typeof input.currentTaskId === "string"
            ? { currentTaskId: input.currentTaskId }
            : {}),
        });
      } catch (error: unknown) {
        issues.push(
          toMilestoneBatchValidationIssue(error, {
            code: "TASK_BATCH_DEPENDENCY_INVALID",
            message: `Invalid dependency ids for ${input.ownerLabel}.`,
            referenceKey: input.ownerLabel,
            field: "dependencyTaskIds",
            stage: "validation",
            ...(input.operationKind
              ? { operationKind: input.operationKind }
              : {}),
            ...(typeof input.operationIndex === "number"
              ? { operationIndex: input.operationIndex }
              : {}),
            ownerLabel: input.ownerLabel,
            ...(input.taskIntent ? { taskIntent: input.taskIntent } : {}),
            ...(input.idempotencyKey
              ? { idempotencyKey: input.idempotencyKey }
              : {}),
            details: {
              dependencyIds: explicitTaskIds,
            },
          }),
        );
      }
    }

    return issues;
  }

  async areDependenciesSatisfied(task: TaskRecord): Promise<boolean> {
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

}

export default TaskValidationService;
