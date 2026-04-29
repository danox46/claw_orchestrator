import type { JobRecord } from "../jobs/job.service";
import type { MilestonesServicePort } from "../milestones/milestone.service";
import type { TaskRecord, TasksServicePort } from "../tasks/task.service";
import type { OpenClawTaskStatusResponse } from "./openclaw.client";
import type {
  AgentPlanningBatchLogger,
  AgentPlanningBatchServiceDependencies,
  AtomicMilestonePlanningBatch,
  AtomicMilestonePlanningBatchCommitter,
  AtomicMilestonePlanningBatchResult,
  PlanningValidationIssue,
  PreparedConcreteTask,
} from "./agent.types";
import {
  assertMilestonePlanningBatchCommitResult,
  prepareAtomicMilestonePlanningBatch,
  prepareConcreteMilestoneTasks,
  validatePlannedTaskList,
  validatePreparedConcreteMilestoneTasks,
} from "./agent-planning-batch.helpers";
import {
  asRecord,
  createAggregatePlannedTaskValidationError,
  createServiceError,
  describeMilestonePlanningOperation,
  describeMilestonePlanningProgress,
  extractErrorDetailsRecord,
  extractPlannedTasks,
  isRecord,
  normalizeBatchValidationIssues,
  normalizeTaskArtifactRefs,
  readOptionalString,
  serializeUnknownError,
} from "./agent.helpers";

export class AgentPlanningBatchService {
  private readonly tasksService: TasksServicePort;
  private readonly milestonesService: MilestonesServicePort;
  private readonly projectOwnerAgentId: string;
  private readonly projectManagerAgentId: string;
  private readonly implementerAgentId: string;
  private readonly qaAgentId: string;

  constructor(dependencies: AgentPlanningBatchServiceDependencies) {
    this.tasksService = dependencies.tasksService;
    this.milestonesService = dependencies.milestonesService;
    this.projectOwnerAgentId = dependencies.projectOwnerAgentId;
    this.projectManagerAgentId = dependencies.projectManagerAgentId;
    this.implementerAgentId = dependencies.implementerAgentId;
    this.qaAgentId = dependencies.qaAgentId;
  }

  async enqueueConcreteMilestoneTasks(input: {
    job: JobRecord;
    task: TaskRecord;
    finalResult: OpenClawTaskStatusResponse;
    dispatchLogger: AgentPlanningBatchLogger;
  }): Promise<void> {
    const plannedTasks = extractPlannedTasks(input.finalResult.outputs).filter(
      (plannedTask) => plannedTask.intent !== "review_milestone",
    );

    const plannerValidationIssues: PlanningValidationIssue[] = [];

    if (plannedTasks.length === 0) {
      plannerValidationIssues.push({
        code: "PLANNED_TASKS_OUTPUT_MISSING",
        message:
          "Phase task planner succeeded but returned no usable tasks in outputs.tasks.",
        stage: "planned-task-validation",
        details: {
          jobId: input.task.jobId,
          taskId: input.task._id,
          outputs: input.finalResult.outputs,
        },
      });
    }

    const milestone = await this.milestonesService.requireMilestoneById(
      input.task.milestoneId,
    );

    const existingTasks = await this.tasksService.listTasks({
      jobId: input.task.jobId,
      milestoneId: input.task.milestoneId,
      limit: 500,
    });

    const existingByIdempotencyKey = new Map<string, TaskRecord>();
    for (const existingTask of existingTasks) {
      existingByIdempotencyKey.set(existingTask.idempotencyKey, existingTask);
    }

    const parentInputs = asRecord(input.task.inputs);
    const phaseId = readOptionalString(parentInputs, ["phaseId"]) ?? "phase-1";
    const phaseName =
      readOptionalString(parentInputs, ["phaseName"]) ?? "Current phase";
    const phaseGoal = readOptionalString(parentInputs, ["phaseGoal"]);

    const plannedTaskValidation = validatePlannedTaskList({
      plannerTask: input.task,
      plannedTasks,
    });
    plannerValidationIssues.push(...plannedTaskValidation.issues);

    let preparedTasks: PreparedConcreteTask[] = [];

    if (plannerValidationIssues.length === 0) {
      preparedTasks = prepareConcreteMilestoneTasks({
        plannerTask: input.task,
        plannedTasks,
        plannedTaskMetadata: plannedTaskValidation.plannedTaskMetadata,
        rawReferenceOwnerByKey: plannedTaskValidation.rawReferenceOwnerByKey,
        existingByIdempotencyKey,
        phaseId,
        phaseName,
        projectOwnerAgentId: this.projectOwnerAgentId,
        projectManagerAgentId: this.projectManagerAgentId,
        implementerAgentId: this.implementerAgentId,
        qaAgentId: this.qaAgentId,
        ...(phaseGoal ? { phaseGoal } : {}),
      });

      plannerValidationIssues.push(
        ...validatePreparedConcreteMilestoneTasks({
          plannerTask: input.task,
          preparedTasks,
        }),
      );
    }

    if (plannerValidationIssues.length > 0) {
      throw createAggregatePlannedTaskValidationError({
        plannerTask: input.task,
        issues: plannerValidationIssues,
      });
    }

    const batch = prepareAtomicMilestonePlanningBatch({
      plannerTask: input.task,
      milestone,
      preparedTasks,
      existingByIdempotencyKey,
      phaseId,
      phaseName,
      projectOwnerAgentId: this.projectOwnerAgentId,
      ...(phaseGoal ? { phaseGoal } : {}),
    });

    const validateMilestonePlanningBatch =
      this.getValidateMilestonePlanningBatchOrThrow();
    const preflightResult = await validateMilestonePlanningBatch(batch);
    const batchIssues = normalizeBatchValidationIssues(preflightResult);
    if (batchIssues.length > 0) {
      throw createAggregatePlannedTaskValidationError({
        plannerTask: input.task,
        issues: batchIssues,
      });
    }

    const batchResult = await this.commitMilestonePlanningBatchSafely({
      plannerTask: input.task,
      batch,
    });

    input.dispatchLogger.info(
      {
        phaseId,
        createdTaskCount: batchResult.createdTaskIds.length,
        updatedTaskCount: batchResult.updatedTaskIds.length,
        createdTaskIds: batchResult.createdTaskIds,
        updatedTaskIds: batchResult.updatedTaskIds,
        reviewTaskId: batchResult.reviewTaskId,
        reviewTaskCreated: batchResult.reviewTaskCreated,
        reviewTaskUpdated: batchResult.reviewTaskUpdated,
      },
      "Committed concrete milestone execution and enrichment tasks after validation-first planning checks.",
    );

    await this.finalizeMilestonePlanningPlannerTask({
      plannerTask: input.task,
      finalResult: input.finalResult,
      batch,
      batchResult,
    });
  }

  private async commitMilestonePlanningBatchSafely(input: {
    plannerTask: TaskRecord;
    batch: AtomicMilestonePlanningBatch;
  }): Promise<AtomicMilestonePlanningBatchResult> {
    const commitMilestonePlanningBatch =
      this.getCommitMilestonePlanningBatchOrThrow();

    try {
      const batchResult = await commitMilestonePlanningBatch(input.batch);
      assertMilestonePlanningBatchCommitResult({
        plannerTask: input.plannerTask,
        batch: input.batch,
        batchResult,
      });
      return batchResult;
    } catch (error) {
      const observedState = await this.inspectMilestonePlanningBatchState(
        input.batch,
      );
      const createdCount = observedState.createdTasks.length;
      const originalError = serializeUnknownError(error);
      const detailRecord = extractErrorDetailsRecord(error);
      const operationSummary = describeMilestonePlanningOperation(detailRecord);
      const progressSummary = describeMilestonePlanningProgress(detailRecord, {
        createdCount,
      });
      const causeMessage = readOptionalString(detailRecord, [
        "message",
        "causeMessage",
      ]);
      const partialMessage = [
        "Milestone planning batch commit partially applied",
        operationSummary ? `during ${operationSummary}` : undefined,
        progressSummary,
        causeMessage ? `cause: ${causeMessage}` : undefined,
      ]
        .filter(
          (part): part is string =>
            typeof part === "string" && part.trim().length > 0,
        )
        .join(". ");
      const failedMessage = [
        "Milestone planning batch commit failed before all task mutations completed",
        operationSummary ? `during ${operationSummary}` : undefined,
        causeMessage ? `cause: ${causeMessage}` : undefined,
      ]
        .filter(
          (part): part is string =>
            typeof part === "string" && part.trim().length > 0,
        )
        .join(". ");

      throw createServiceError({
        message: createdCount > 0 ? `${partialMessage}.` : `${failedMessage}.`,
        code:
          createdCount > 0
            ? "TASK_BATCH_COMMIT_PARTIAL"
            : "TASK_BATCH_COMMIT_FAILED",
        statusCode: 500,
        retryable: true,
        details: {
          plannerTaskId: input.plannerTask._id,
          milestoneId: input.plannerTask.milestoneId,
          createdCount,
          observedState,
          ...(isRecord(detailRecord) && Object.keys(detailRecord).length > 0
            ? { commitError: detailRecord }
            : {}),
          originalError,
        },
      });
    }
  }

  private async finalizeMilestonePlanningPlannerTask(input: {
    plannerTask: TaskRecord;
    finalResult: OpenClawTaskStatusResponse;
    batch: AtomicMilestonePlanningBatch;
    batchResult: AtomicMilestonePlanningBatchResult;
  }): Promise<void> {
    const normalizedArtifacts = normalizeTaskArtifactRefs(
      input.finalResult.artifacts,
    );

    try {
      await this.tasksService.markSucceeded({
        taskId: input.plannerTask._id,
        ...(input.finalResult.outputs
          ? { outputs: input.finalResult.outputs }
          : {}),
        ...(normalizedArtifacts.length > 0
          ? { artifacts: normalizedArtifacts }
          : {}),
      });
    } catch (error) {
      const observedState = await this.inspectMilestonePlanningBatchState(
        input.batch,
      );

      throw createServiceError({
        message:
          "Milestone planning child tasks were committed, but the planner task finalization failed. Review the persisted child-task state before retrying.",
        code: "PLANNER_FINALIZATION_FAILED_AFTER_BATCH_COMMIT",
        statusCode: 500,
        retryable: true,
        details: {
          plannerTaskId: input.plannerTask._id,
          milestoneId: input.plannerTask.milestoneId,
          batchResult: input.batchResult,
          observedState,
          originalError: serializeUnknownError(error),
        },
      });
    }
  }

  private async inspectMilestonePlanningBatchState(
    batch: AtomicMilestonePlanningBatch,
  ): Promise<{
    createdTasks: Array<{ idempotencyKey: string; taskId: string }>;
    reviewTaskId?: string;
    plannerTaskExists: boolean;
  }> {
    const createdTasks: Array<{ idempotencyKey: string; taskId: string }> = [];
    let reviewTaskId: string | undefined;

    for (const create of batch.creates) {
      const existingTask = await this.tasksService.getTaskByIdempotencyKey(
        create.task.idempotencyKey,
      );

      if (!existingTask) {
        continue;
      }

      createdTasks.push({
        idempotencyKey: create.task.idempotencyKey,
        taskId: existingTask._id,
      });

      if (existingTask.intent === "review_milestone") {
        reviewTaskId = existingTask._id;
      }
    }

    if (!reviewTaskId) {
      const reviewUpdate = batch.updates.find((update) =>
        update.referenceKeys.some((referenceKey) =>
          referenceKey.startsWith("review:"),
        ),
      );
      if (reviewUpdate) {
        reviewTaskId = reviewUpdate.taskId;
      }
    }

    const plannerTask = await this.tasksService.getTaskById(
      batch.plannerTaskId,
    );

    return {
      createdTasks,
      ...(reviewTaskId ? { reviewTaskId } : {}),
      plannerTaskExists: Boolean(plannerTask),
    };
  }

  private getValidateMilestonePlanningBatchOrThrow(): (
    batch: AtomicMilestonePlanningBatch,
  ) => Promise<unknown> {
    const validateMilestonePlanningBatch = (
      this.tasksService as TasksServicePort & {
        validateMilestonePlanningBatch?: unknown;
      }
    ).validateMilestonePlanningBatch;

    if (typeof validateMilestonePlanningBatch !== "function") {
      throw createServiceError({
        message:
          "Tasks service does not support validateMilestonePlanningBatch, so milestone planning cannot be safely validated before commit.",
        code: "TASK_BATCH_VALIDATE_NOT_SUPPORTED",
        statusCode: 500,
        retryable: false,
      });
    }

    return validateMilestonePlanningBatch.bind(this.tasksService) as (
      batch: AtomicMilestonePlanningBatch,
    ) => Promise<unknown>;
  }

  private getCommitMilestonePlanningBatchOrThrow(): AtomicMilestonePlanningBatchCommitter {
    const commitMilestonePlanningBatch = (
      this.tasksService as TasksServicePort & {
        commitMilestonePlanningBatch?: unknown;
      }
    ).commitMilestonePlanningBatch;

    if (typeof commitMilestonePlanningBatch !== "function") {
      throw createServiceError({
        message:
          "Tasks service does not support commitMilestonePlanningBatch, so milestone planning cannot be committed.",
        code: "TASK_BATCH_COMMIT_NOT_SUPPORTED",
        statusCode: 500,
        retryable: false,
      });
    }

    return commitMilestonePlanningBatch.bind(
      this.tasksService,
    ) as AtomicMilestonePlanningBatchCommitter;
  }
}

export default AgentPlanningBatchService;
