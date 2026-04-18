import { createLogger } from "../../config/logger";
import type { JobRecord, JobsServicePort } from "../jobs/job.service";
import type {
  MilestoneRecord,
  MilestonesServicePort,
} from "../milestones/milestone.service";
import type {
  CreateTaskInput,
  TaskRecord,
  TasksServicePort,
} from "../tasks/task.service";
import type {
  OpenClawClient,
  OpenClawTaskStatusResponse,
} from "./openclaw.client";

const logger = createLogger({
  module: "agents",
  component: "agent-dispatch-service",
});

type ServiceError = Error & {
  statusCode?: number;
  code?: string;
  details?: unknown;
};

function createServiceError(input: {
  message: string;
  code: string;
  statusCode: number;
  details?: unknown;
}): ServiceError {
  return Object.assign(new Error(input.message), {
    code: input.code,
    statusCode: input.statusCode,
    details: input.details,
  });
}

type PlannedPhase = {
  phaseId: string;
  name: string;
  goal?: string;
  description?: string;
  dependsOn: string[];
  inputs: Record<string, unknown>;
  deliverables: string[];
  exitCriteria: string[];
  raw: Record<string, unknown>;
};

type PlannedTaskDefinition = {
  localId?: string;
  intent: string;
  targetAgentId?: string;
  inputs: Record<string, unknown>;
  constraints?: Record<string, unknown>;
  requiredArtifacts: string[];
  acceptanceCriteria: string[];
  idempotencyKey?: string;
  dependsOn: string[];
};

export type AgentDispatchServiceDependencies = {
  openClawClient: OpenClawClient;
  tasksService: TasksServicePort;
  jobsService: JobsServicePort;
  milestonesService: MilestonesServicePort;
  taskTimeoutMs?: number;
  projectOwnerAgentId?: string;
  projectManagerAgentId?: string;
  implementerAgentId?: string;
  qaAgentId?: string;
};

export class AgentDispatchService {
  private readonly openClawClient: OpenClawClient;
  private readonly tasksService: TasksServicePort;
  private readonly jobsService: JobsServicePort;
  private readonly milestonesService: MilestonesServicePort;
  private readonly taskTimeoutMs: number;

  private readonly projectOwnerAgentId: string;
  private readonly projectManagerAgentId: string;
  private readonly implementerAgentId: string;
  private readonly qaAgentId: string;

  constructor(dependencies: AgentDispatchServiceDependencies) {
    this.openClawClient = dependencies.openClawClient;
    this.tasksService = dependencies.tasksService;
    this.jobsService = dependencies.jobsService;
    this.milestonesService = dependencies.milestonesService;
    this.taskTimeoutMs = dependencies.taskTimeoutMs ?? 120_000;

    this.projectOwnerAgentId =
      dependencies.projectOwnerAgentId ?? "project-owner";
    this.projectManagerAgentId =
      dependencies.projectManagerAgentId ?? "project-manager";
    this.implementerAgentId = dependencies.implementerAgentId ?? "implementer";
    this.qaAgentId = dependencies.qaAgentId ?? "qa";
  }

  async dispatchNextTask(
    job: JobRecord,
    input?: { taskId?: string },
  ): Promise<void> {
    const task = input?.taskId
      ? await this.tasksService.requireTaskById(input.taskId)
      : await this.findNextDispatchableTaskForJob(job._id);

    if (!task) {
      logger.warn(
        {
          jobId: job._id,
          projectId: job.projectId,
          state: job.state,
        },
        "No runnable task found for job dispatch.",
      );
      return;
    }

    if (task.status !== "queued" && task.status !== "qa") {
      logger.debug(
        {
          jobId: job._id,
          taskId: task._id,
          taskStatus: task.status,
        },
        "Task is not in a dispatchable state; skipping dispatch.",
      );
      return;
    }

    const attemptNumber = task.attemptCount + 1;

    const dispatchLogger = logger.child({
      jobId: job._id,
      projectId: job.projectId,
      milestoneId: task.milestoneId,
      taskId: task._id,
      intent: task.intent,
      agentId: task.target.agentId,
      attemptNumber,
      maxAttempts: task.maxAttempts,
      sequence: task.sequence,
    });

    dispatchLogger.info("Dispatching task to OpenClaw.");

    try {
      await this.tasksService.markRunning(task._id);

      const sendResult = await this.openClawClient.sendTask({
        agentId: task.target.agentId,
        payload: {
          projectId: task.projectId,
          jobId: task.jobId,
          milestoneId: task.milestoneId,
          taskId: task._id,
          intent: String(task.intent),
          inputs: task.inputs,
          constraints: {
            toolProfile: task.constraints.toolProfile,
            sandbox: task.constraints.sandbox,
            ...(typeof task.constraints.maxTokens === "number"
              ? { maxTokens: task.constraints.maxTokens }
              : {}),
            ...(typeof task.constraints.maxCost === "number"
              ? { maxCost: task.constraints.maxCost }
              : {}),
          },
          requiredArtifacts: task.requiredArtifacts,
          acceptanceCriteria: task.acceptanceCriteria,
          idempotencyKey: task.idempotencyKey,
          attemptNumber,
          maxAttempts: task.maxAttempts,
          ...(task.errors.length > 0 ? { errors: task.errors } : {}),
          ...(typeof task.lastError === "string" &&
          task.lastError.trim().length > 0
            ? { lastError: task.lastError }
            : {}),
          ...(task.outputs ? { outputs: task.outputs } : {}),
          ...(task.artifacts.length > 0 ? { artifacts: task.artifacts } : {}),
        },
      });

      const finalResult = await this.resolveFinalResult(sendResult);

      if (finalResult.status !== "succeeded") {
        const failureMessage = this.buildFailureMessage(finalResult);

        if (task.status === "qa") {
          await this.handleQaReviewFailure({
            job,
            task,
            attemptNumber,
            dispatchLogger,
            failureMessage,
            ...(finalResult.outputs ? { outputs: finalResult.outputs } : {}),
            ...(finalResult.artifacts
              ? { artifacts: finalResult.artifacts }
              : {}),
          });
        } else {
          await this.handleTaskFailure({
            job,
            task,
            attemptNumber,
            dispatchLogger,
            failureMessage,
            ...(finalResult.outputs ? { outputs: finalResult.outputs } : {}),
            ...(finalResult.artifacts
              ? { artifacts: finalResult.artifacts }
              : {}),
          });
        }

        return;
      }

      if (this.shouldHandOffTaskToQa(task)) {
        const normalizedArtifacts = this.normalizeTaskArtifactRefs(
          finalResult.artifacts,
        );

        await this.tasksService.updateTask(task._id, {
          status: "qa",
          target: {
            agentId: this.qaAgentId,
          },
          ...(finalResult.outputs ? { outputs: finalResult.outputs } : {}),
          ...(normalizedArtifacts.length > 0
            ? { artifacts: normalizedArtifacts }
            : {}),
        });

        dispatchLogger.info(
          {
            qaAgentId: this.qaAgentId,
          },
          "Task succeeded and was handed off to QA.",
        );

        return;
      }

      await this.enqueueFollowUpWork({
        job,
        task,
        finalResult,
        dispatchLogger,
      });

      await this.tasksService.markSucceeded({
        taskId: task._id,
        ...(finalResult.outputs ? { outputs: finalResult.outputs } : {}),
        ...(this.normalizeTaskArtifactRefs(finalResult.artifacts).length > 0
          ? { artifacts: this.normalizeTaskArtifactRefs(finalResult.artifacts) }
          : {}),
      });

      dispatchLogger.info("Task succeeded.");
    } catch (error) {
      const failureMessage =
        error instanceof Error ? error.message : "Unknown dispatch error";

      dispatchLogger.error(
        { err: error },
        "Task dispatch failed unexpectedly.",
      );

      await this.handleTaskFailure({
        job,
        task,
        attemptNumber,
        dispatchLogger,
        failureMessage,
      });
    }
  }

  private async findNextDispatchableTaskForJob(
    jobId: string,
  ): Promise<TaskRecord | null> {
    const nextRunnableTask = await this.tasksService.listNextRunnableTask({
      jobId,
    });

    if (nextRunnableTask) {
      return nextRunnableTask;
    }

    const tasks = await this.tasksService.listTasks({
      jobId,
      limit: 500,
    });

    const nextQaTask = tasks
      .filter(
        (task) =>
          task.status === "qa" && task.target.agentId === this.qaAgentId,
      )
      .sort((left, right) => {
        if (left.sequence !== right.sequence) {
          return left.sequence - right.sequence;
        }

        return left.createdAt.getTime() - right.createdAt.getTime();
      })[0];

    return nextQaTask ?? null;
  }

  private async handleTaskFailure(input: {
    job: JobRecord;
    task: TaskRecord;
    attemptNumber: number;
    dispatchLogger: typeof logger;
    failureMessage: string;
    outputs?: Record<string, unknown>;
    artifacts?: unknown;
  }): Promise<void> {
    if (this.shouldRetryTask(input.task, input.attemptNumber)) {
      const nextRetryAt = this.computeNextRetryAt(input.attemptNumber);

      await this.tasksService.requeueTask({
        taskId: input.task._id,
        error: input.failureMessage,
        ...(nextRetryAt ? { nextRetryAt } : {}),
        ...(input.outputs ? { outputs: input.outputs } : {}),
        ...(this.normalizeTaskArtifactRefs(input.artifacts).length > 0
          ? { artifacts: this.normalizeTaskArtifactRefs(input.artifacts) }
          : {}),
      });

      input.dispatchLogger.warn(
        {
          failureMessage: input.failureMessage,
          nextRetryAt,
        },
        "Task failed but was requeued for retry.",
      );

      return;
    }

    await this.tasksService.markFailedExhausted({
      taskId: input.task._id,
      errors: [input.failureMessage],
      ...(input.outputs ? { outputs: input.outputs } : {}),
      ...(this.normalizeTaskArtifactRefs(input.artifacts).length > 0
        ? { artifacts: this.normalizeTaskArtifactRefs(input.artifacts) }
        : {}),
    });

    await this.jobsService.markFailed(input.job._id, input.failureMessage);

    input.dispatchLogger.warn(
      {
        failureMessage: input.failureMessage,
        attemptNumber: input.attemptNumber,
        maxAttempts: input.task.maxAttempts,
      },
      "Task exhausted all retries and job was marked FAILED.",
    );
  }

  private async handleQaReviewFailure(input: {
    job: JobRecord;
    task: TaskRecord;
    attemptNumber: number;
    dispatchLogger: typeof logger;
    failureMessage: string;
    outputs?: Record<string, unknown>;
    artifacts?: unknown;
  }): Promise<void> {
    const normalizedArtifacts = this.normalizeTaskArtifactRefs(input.artifacts);

    if (this.shouldRetryTask(input.task, input.attemptNumber)) {
      const nextRetryAt = this.computeNextRetryAt(input.attemptNumber);

      await this.tasksService.requeueTask({
        taskId: input.task._id,
        error: input.failureMessage,
        ...(nextRetryAt ? { nextRetryAt } : {}),
        ...(input.outputs ? { outputs: input.outputs } : {}),
        ...(normalizedArtifacts.length > 0
          ? { artifacts: normalizedArtifacts }
          : {}),
      });

      await this.tasksService.updateTask(input.task._id, {
        target: {
          agentId: this.implementerAgentId,
        },
      });

      input.dispatchLogger.warn(
        {
          failureMessage: input.failureMessage,
          nextRetryAt,
          implementerAgentId: this.implementerAgentId,
        },
        "QA review failed and the task was requeued for the implementer.",
      );

      return;
    }

    await this.tasksService.markFailedExhausted({
      taskId: input.task._id,
      errors: [input.failureMessage],
      ...(input.outputs ? { outputs: input.outputs } : {}),
      ...(normalizedArtifacts.length > 0
        ? { artifacts: normalizedArtifacts }
        : {}),
    });

    await this.jobsService.markFailed(input.job._id, input.failureMessage);

    input.dispatchLogger.warn(
      {
        failureMessage: input.failureMessage,
        attemptNumber: input.attemptNumber,
        maxAttempts: input.task.maxAttempts,
      },
      "QA review failed, retries were exhausted, and the job was marked FAILED.",
    );
  }

  private shouldHandOffTaskToQa(task: TaskRecord): boolean {
    return (
      task.status !== "qa" && task.target.agentId === this.implementerAgentId
    );
  }

  private shouldRetryTask(task: TaskRecord, attemptNumber: number): boolean {
    return task.retryable && attemptNumber < task.maxAttempts;
  }

  private computeNextRetryAt(attemptNumber: number): Date | undefined {
    const now = Date.now();

    if (attemptNumber <= 1) {
      return new Date(now + 300_000);
    }

    if (attemptNumber === 2) {
      return new Date(now + 900_000);
    }

    if (attemptNumber === 3) {
      return new Date(now + 2_000_000);
    }

    return new Date(now + 300_000);
  }

  private async enqueueFollowUpWork(input: {
    job: JobRecord;
    task: TaskRecord;
    finalResult: OpenClawTaskStatusResponse;
    dispatchLogger: typeof logger;
  }): Promise<void> {
    const intent = String(input.task.intent);

    switch (intent) {
      case "plan_project_phases":
        await this.createMilestonesAndPlannerTasks(input);
        return;
      case "plan_phase_tasks":
      case "plan_next_tasks":
        await this.enqueueConcreteMilestoneTasks(input);
        return;
      default:
        return;
    }
  }

  private async createMilestonesAndPlannerTasks(input: {
    job: JobRecord;
    task: TaskRecord;
    finalResult: OpenClawTaskStatusResponse;
    dispatchLogger: typeof logger;
  }): Promise<void> {
    const createTask = this.getCreateTaskOrThrow();
    const phases = this.extractPlannedPhases(input.finalResult.outputs);

    if (phases.length === 0) {
      throw createServiceError({
        message:
          "plan_project_phases succeeded but returned no usable phases in outputs.phases.",
        code: "PHASES_OUTPUT_MISSING",
        statusCode: 500,
        details: {
          jobId: input.task.jobId,
          taskId: input.task._id,
          outputs: input.finalResult.outputs,
        },
      });
    }

    const existingMilestones = await this.milestonesService.listMilestones({
      projectId: input.task.projectId,
      limit: 200,
    });

    const existingByOrder = new Map<number, MilestoneRecord>();
    for (const milestone of existingMilestones) {
      existingByOrder.set(milestone.order, milestone);
    }

    const parentInputs = this.asRecord(input.task.inputs);

    let previousMilestoneId: string = input.task.milestoneId;
    const createdMilestoneIds: string[] = [];
    const createdPlannerTaskIds: string[] = [];
    let reusedMilestoneCount = 0;
    let skippedPlannerTaskCount = 0;

    for (const [index, phase] of phases.entries()) {
      const order = index + 1;
      let milestone = existingByOrder.get(order);

      if (!milestone) {
        milestone = await this.milestonesService.createMilestone({
          projectId: input.task.projectId,
          title: phase.name,
          ...(phase.description ? { description: phase.description } : {}),
          order,
          status: "ready",
          ...(phase.goal ? { goal: phase.goal } : {}),
          scope: phase.deliverables,
          acceptanceCriteria: phase.exitCriteria,
          dependsOnMilestoneId: previousMilestoneId,
        });

        existingByOrder.set(order, milestone);
        createdMilestoneIds.push(milestone._id);
      } else {
        reusedMilestoneCount += 1;
      }

      previousMilestoneId = milestone._id;

      const plannerIdempotencyKey = `${input.task.jobId}:milestone:${milestone._id}:plan_phase_tasks`;
      const existingPlannerTask =
        await this.tasksService.getTaskByIdempotencyKey(plannerIdempotencyKey);

      if (existingPlannerTask) {
        skippedPlannerTaskCount += 1;
        continue;
      }

      const plannerTask = await createTask({
        jobId: input.task.jobId,
        projectId: input.task.projectId,
        milestoneId: milestone._id,
        parentTaskId: input.task._id,
        dependencies: [],
        issuer: {
          kind: "system",
          id: "app-factory-orchestrator",
          role: "orchestrator",
        },
        target: {
          agentId: this.projectManagerAgentId,
        },
        intent: this.asCreateTaskIntent("plan_phase_tasks"),
        inputs: {
          ...(typeof parentInputs.projectName === "string"
            ? { projectName: parentInputs.projectName }
            : {}),
          ...(typeof parentInputs.projectRequest === "string"
            ? { projectRequest: parentInputs.projectRequest }
            : typeof parentInputs.prompt === "string"
              ? { projectRequest: parentInputs.prompt }
              : {}),
          ...(typeof parentInputs.request === "string"
            ? { request: parentInputs.request }
            : {}),
          ...(typeof parentInputs.appType === "string"
            ? { appType: parentInputs.appType }
            : {}),
          ...(typeof parentInputs.stack === "string"
            ? { stack: parentInputs.stack }
            : {}),
          ...(typeof parentInputs.deployment === "string"
            ? { deployment: parentInputs.deployment }
            : {}),
          milestoneId: milestone._id,
          phaseId: phase.phaseId,
          phaseName: phase.name,
          ...(phase.goal ? { phaseGoal: phase.goal } : {}),
          phase: phase.raw,
        },
        constraints: this.buildTaskConstraints(input.task.constraints),
        requiredArtifacts: [],
        acceptanceCriteria: phase.exitCriteria,
        idempotencyKey: plannerIdempotencyKey,
        sequence: 0,
      });

      createdPlannerTaskIds.push(plannerTask._id);
    }

    input.dispatchLogger.info(
      {
        phaseCount: phases.length,
        createdMilestoneCount: createdMilestoneIds.length,
        reusedMilestoneCount,
        createdPlannerTaskCount: createdPlannerTaskIds.length,
        skippedPlannerTaskCount,
        createdMilestoneIds,
        createdPlannerTaskIds,
      },
      "Created milestone records and PM milestone planning tasks.",
    );
  }

  private async enqueueConcreteMilestoneTasks(input: {
    job: JobRecord;
    task: TaskRecord;
    finalResult: OpenClawTaskStatusResponse;
    dispatchLogger: typeof logger;
  }): Promise<void> {
    const createTask = this.getCreateTaskOrThrow();
    const plannedTasks = this.extractPlannedTasks(input.finalResult.outputs);

    if (plannedTasks.length === 0) {
      throw createServiceError({
        message:
          "Phase task planner succeeded but returned no usable tasks in outputs.tasks.",
        code: "PLANNED_TASKS_OUTPUT_MISSING",
        statusCode: 500,
        details: {
          jobId: input.task.jobId,
          taskId: input.task._id,
          outputs: input.finalResult.outputs,
        },
      });
    }

    const existingTasks = await this.tasksService.listTasks({
      jobId: input.task.jobId,
      milestoneId: input.task.milestoneId,
      limit: 500,
    });

    const existingByIdempotencyKey = new Map<string, TaskRecord>();
    for (const existingTask of existingTasks) {
      existingByIdempotencyKey.set(existingTask.idempotencyKey, existingTask);
    }

    const parentInputs = this.asRecord(input.task.inputs);
    const phaseId =
      this.readOptionalString(parentInputs, ["phaseId"]) ?? "phase-1";
    const phaseName =
      this.readOptionalString(parentInputs, ["phaseName"]) ?? "Current phase";
    const phaseGoal = this.readOptionalString(parentInputs, ["phaseGoal"]);

    const resolvedTasks: Array<{
      plannedTask: PlannedTaskDefinition;
      taskRecord: TaskRecord;
      referenceKeys: string[];
    }> = [];

    const createdTaskIds: string[] = [];
    let skippedExistingTaskCount = 0;

    for (const [index, plannedTask] of plannedTasks.entries()) {
      const idempotencyKey =
        plannedTask.idempotencyKey ??
        `${input.task.jobId}:${input.task.milestoneId}:planned-task:${index + 1}:${plannedTask.intent}`;

      let taskRecord = existingByIdempotencyKey.get(idempotencyKey);

      if (!taskRecord) {
        taskRecord = await createTask({
          jobId: input.task.jobId,
          projectId: input.task.projectId,
          milestoneId: input.task.milestoneId,
          parentTaskId: input.task._id,
          dependencies: [],
          issuer: {
            kind: "system",
            id: "app-factory-orchestrator",
            role: "orchestrator",
          },
          target: {
            agentId: this.resolveTargetAgentId(
              plannedTask.intent,
              plannedTask.targetAgentId,
            ),
          },
          intent: this.asCreateTaskIntent(plannedTask.intent),
          inputs: this.buildConcreteTaskInputs({
            parentTask: input.task,
            plannedTask,
            phaseId,
            phaseName,
            ...(phaseGoal ? { phaseGoal } : {}),
          }),
          constraints: this.buildTaskConstraints(
            input.task.constraints,
            plannedTask.constraints,
          ),
          requiredArtifacts: plannedTask.requiredArtifacts,
          acceptanceCriteria: plannedTask.acceptanceCriteria,
          idempotencyKey,
          sequence: this.getConcreteTaskSequence(input.task.sequence, index),
        });

        existingByIdempotencyKey.set(idempotencyKey, taskRecord);
        createdTaskIds.push(taskRecord._id);
      } else {
        skippedExistingTaskCount += 1;
      }

      const referenceKeys = this.buildPlannedTaskReferenceKeys(
        plannedTask,
        idempotencyKey,
      );

      resolvedTasks.push({
        plannedTask,
        taskRecord,
        referenceKeys,
      });
    }

    const referenceToTaskId = new Map<string, string>();

    for (const resolvedTask of resolvedTasks) {
      for (const referenceKey of resolvedTask.referenceKeys) {
        if (!referenceToTaskId.has(referenceKey)) {
          referenceToTaskId.set(referenceKey, resolvedTask.taskRecord._id);
        }
      }
    }

    for (const resolvedTask of resolvedTasks) {
      if (resolvedTask.plannedTask.dependsOn.length === 0) {
        continue;
      }

      const dependencyTaskIds: string[] = [];

      for (const dependencyRef of resolvedTask.plannedTask.dependsOn) {
        const dependencyTaskId = referenceToTaskId.get(dependencyRef);

        if (!dependencyTaskId) {
          throw createServiceError({
            message: `Planned task dependency "${dependencyRef}" could not be resolved for milestone ${input.task.milestoneId}.`,
            code: "PLANNED_TASK_DEPENDENCY_UNRESOLVED",
            statusCode: 500,
            details: {
              milestoneId: input.task.milestoneId,
              taskId: input.task._id,
              dependencyRef,
              plannedTaskIntent: resolvedTask.plannedTask.intent,
              plannedTaskLocalId: resolvedTask.plannedTask.localId,
            },
          });
        }

        if (
          dependencyTaskId !== resolvedTask.taskRecord._id &&
          !dependencyTaskIds.includes(dependencyTaskId)
        ) {
          dependencyTaskIds.push(dependencyTaskId);
        }
      }

      const currentDependencyIds = [
        ...resolvedTask.taskRecord.dependencies,
      ].sort();
      const nextDependencyIds = [...dependencyTaskIds].sort();

      if (
        currentDependencyIds.length === nextDependencyIds.length &&
        currentDependencyIds.every(
          (value, index) => value === nextDependencyIds[index],
        )
      ) {
        continue;
      }

      await this.tasksService.updateTask(resolvedTask.taskRecord._id, {
        dependencies: dependencyTaskIds,
      });
    }

    input.dispatchLogger.info(
      {
        phaseId,
        createdTaskCount: createdTaskIds.length,
        skippedExistingTaskCount,
        createdTaskIds,
      },
      "Created concrete milestone tasks and resolved dependencies.",
    );
  }

  private buildConcreteTaskInputs(input: {
    parentTask: TaskRecord;
    plannedTask: PlannedTaskDefinition;
    phaseId: string;
    phaseName: string;
    phaseGoal?: string;
  }): Record<string, unknown> {
    const parentInputs = this.asRecord(input.parentTask.inputs);
    const plannedInputs = this.asRecord(input.plannedTask.inputs);
    const plannedPhaseGoal = this.readOptionalString(plannedInputs, [
      "phaseGoal",
    ]);

    return {
      ...(typeof parentInputs.projectName === "string"
        ? { projectName: parentInputs.projectName }
        : {}),
      ...(typeof parentInputs.projectRequest === "string"
        ? { projectRequest: parentInputs.projectRequest }
        : typeof parentInputs.prompt === "string"
          ? { projectRequest: parentInputs.prompt }
          : {}),
      ...(typeof parentInputs.request === "string"
        ? { request: parentInputs.request }
        : {}),
      ...(typeof parentInputs.appType === "string"
        ? { appType: parentInputs.appType }
        : {}),
      ...(typeof parentInputs.stack === "string"
        ? { stack: parentInputs.stack }
        : {}),
      ...(typeof parentInputs.deployment === "string"
        ? { deployment: parentInputs.deployment }
        : {}),
      ...(this.isRecord(parentInputs.phase)
        ? { phase: parentInputs.phase }
        : {}),
      ...plannedInputs,
      milestoneId: input.parentTask.milestoneId,
      phaseId:
        this.readOptionalString(plannedInputs, ["phaseId"]) ?? input.phaseId,
      phaseName:
        this.readOptionalString(plannedInputs, ["phaseName"]) ??
        input.phaseName,
      ...((plannedPhaseGoal ?? input.phaseGoal)
        ? {
            phaseGoal: plannedPhaseGoal ?? input.phaseGoal,
          }
        : {}),
    };
  }

  private buildTaskConstraints(
    fallback: TaskRecord["constraints"],
    override?: Record<string, unknown>,
  ): CreateTaskInput["constraints"] {
    const toolProfile =
      typeof override?.toolProfile === "string" &&
      override.toolProfile.trim().length > 0
        ? override.toolProfile.trim()
        : fallback.toolProfile;

    const sandbox =
      override?.sandbox === "off" ||
      override?.sandbox === "non-main" ||
      override?.sandbox === "all"
        ? override.sandbox
        : fallback.sandbox;

    const maxTokens =
      typeof override?.maxTokens === "number"
        ? override.maxTokens
        : typeof fallback.maxTokens === "number"
          ? fallback.maxTokens
          : undefined;

    const maxCost =
      typeof override?.maxCost === "number"
        ? override.maxCost
        : typeof fallback.maxCost === "number"
          ? fallback.maxCost
          : undefined;

    return {
      toolProfile,
      sandbox,
      ...(typeof maxTokens === "number" ? { maxTokens } : {}),
      ...(typeof maxCost === "number" ? { maxCost } : {}),
    };
  }

  private getConcreteTaskSequence(
    phasePlannerSequence: number,
    taskIndex: number,
  ): number {
    return phasePlannerSequence + (taskIndex + 1) * 10;
  }

  private buildPlannedTaskReferenceKeys(
    plannedTask: PlannedTaskDefinition,
    idempotencyKey: string,
  ): string[] {
    const referenceKeys = new Set<string>();

    if (plannedTask.localId) {
      referenceKeys.add(plannedTask.localId);
    }

    if (plannedTask.idempotencyKey) {
      referenceKeys.add(plannedTask.idempotencyKey);
    }

    referenceKeys.add(idempotencyKey);

    return Array.from(referenceKeys);
  }

  private resolveTargetAgentId(
    intent: string,
    explicitTargetAgentId?: string,
  ): string {
    if (
      typeof explicitTargetAgentId === "string" &&
      explicitTargetAgentId.trim().length > 0
    ) {
      return explicitTargetAgentId.trim();
    }

    switch (intent) {
      case "plan_project_phases":
        return this.projectOwnerAgentId;
      case "plan_phase_tasks":
      case "plan_next_tasks":
        return this.projectManagerAgentId;
      case "run_tests":
      case "review_security":
        return this.qaAgentId;
      case "design_architecture":
      case "generate_scaffold":
      case "implement_feature":
      case "prepare_staging":
      default:
        return this.implementerAgentId;
    }
  }

  private extractPlannedPhases(outputs: unknown): PlannedPhase[] {
    const record = this.asRecord(outputs);

    const direct = Array.isArray(record.phases) ? record.phases : [];
    const nestedOutputs = this.asRecord(record.outputs);
    const nested = Array.isArray(nestedOutputs.phases)
      ? nestedOutputs.phases
      : [];

    const source = direct.length > 0 ? direct : nested;

    return source
      .map((item) => this.normalizePlannedPhase(item))
      .filter((item): item is PlannedPhase => item !== null);
  }

  private normalizePlannedPhase(value: unknown): PlannedPhase | null {
    const record = this.asRecord(value);

    const phaseId = this.readOptionalString(record, ["phaseId", "id"]);
    const name = this.readOptionalString(record, ["name", "title"]);
    const goal = this.readOptionalString(record, ["goal"]);
    const description = this.readOptionalString(record, ["description"]);

    const dependsOn = this.normalizeStringArray(
      record.dependsOn ?? record.dependencies,
    );

    const inputs = this.isRecord(record.inputs) ? record.inputs : {};
    const deliverables = this.normalizeStringArray(record.deliverables);
    const exitCriteria = this.normalizeStringArray(
      record.exitCriteria ?? record.acceptanceCriteria,
    );

    if (!phaseId || !name) {
      return null;
    }

    return {
      phaseId,
      name,
      ...(goal ? { goal } : {}),
      ...(description ? { description } : {}),
      dependsOn,
      inputs,
      deliverables,
      exitCriteria,
      raw: record,
    };
  }

  private extractPlannedTasks(outputs: unknown): PlannedTaskDefinition[] {
    const record = this.asRecord(outputs);

    const direct = Array.isArray(record.tasks) ? record.tasks : [];
    const nestedOutputs = this.asRecord(record.outputs);
    const nested = Array.isArray(nestedOutputs.tasks)
      ? nestedOutputs.tasks
      : [];

    const source = direct.length > 0 ? direct : nested;

    return source
      .map((item) => this.normalizePlannedTask(item))
      .filter((item): item is PlannedTaskDefinition => item !== null);
  }

  private normalizePlannedTask(value: unknown): PlannedTaskDefinition | null {
    const record = this.asRecord(value);
    const target = this.asRecord(record.target);

    const intent = this.readOptionalString(record, ["intent"]);
    const localId = this.readOptionalString(record, [
      "taskId",
      "id",
      "key",
      "localId",
      "slug",
    ]);

    const targetAgentId =
      this.readOptionalString(target, ["agentId"]) ??
      this.readOptionalString(record, ["agentId", "targetAgentId"]);

    const inputs = this.isRecord(record.inputs) ? record.inputs : {};
    const constraints = this.isRecord(record.constraints)
      ? record.constraints
      : undefined;
    const requiredArtifacts = this.normalizeStringArray(
      record.requiredArtifacts,
    );
    const acceptanceCriteria = this.normalizeStringArray(
      record.acceptanceCriteria,
    );
    const idempotencyKey = this.readOptionalString(record, ["idempotencyKey"]);
    const dependsOn = this.normalizeStringArray(
      record.dependsOn ?? record.dependencies,
    );

    if (!intent) {
      return null;
    }

    return {
      ...(localId ? { localId } : {}),
      intent,
      ...(targetAgentId ? { targetAgentId } : {}),
      inputs,
      ...(constraints ? { constraints } : {}),
      requiredArtifacts,
      acceptanceCriteria,
      ...(idempotencyKey ? { idempotencyKey } : {}),
      dependsOn,
    };
  }

  private getCreateTaskOrThrow(): NonNullable<TasksServicePort["createTask"]> {
    const createTask = this.tasksService.createTask;

    if (typeof createTask !== "function") {
      throw createServiceError({
        message:
          "Tasks service does not support createTask, so follow-up tasks cannot be enqueued.",
        code: "TASK_CREATE_NOT_SUPPORTED",
        statusCode: 500,
      });
    }

    return createTask.bind(this.tasksService);
  }

  private asCreateTaskIntent(intent: string): CreateTaskInput["intent"] {
    return intent as CreateTaskInput["intent"];
  }

  private async resolveFinalResult(input: {
    openclawTaskId?: string;
    sessionId?: string;
    agentId: string;
    status: "queued" | "running" | "qa" | "succeeded" | "failed" | "canceled";
    summary?: string;
    outputs?: Record<string, unknown>;
    artifacts?: unknown;
    errors?: string[];
    raw?: unknown;
  }): Promise<OpenClawTaskStatusResponse> {
    const normalizedArtifacts = this.normalizeTaskArtifactRefs(input.artifacts);

    if (
      input.status === "succeeded" ||
      input.status === "failed" ||
      input.status === "canceled"
    ) {
      const response: OpenClawTaskStatusResponse = {
        ...(typeof input.openclawTaskId === "string"
          ? { openclawTaskId: input.openclawTaskId }
          : {}),
        ...(typeof input.sessionId === "string"
          ? { sessionId: input.sessionId }
          : {}),
        agentId: input.agentId,
        status: input.status,
        ...(typeof input.summary === "string" && input.summary.trim().length > 0
          ? { summary: input.summary }
          : {}),
        ...(input.outputs ? { outputs: input.outputs } : {}),
        ...(normalizedArtifacts.length > 0
          ? { artifacts: normalizedArtifacts }
          : {}),
        ...(Array.isArray(input.errors) && input.errors.length > 0
          ? { errors: input.errors }
          : {}),
        ...(input.raw !== undefined ? { raw: input.raw } : {}),
      };

      return response;
    }

    const nonTerminalMessage =
      typeof input.summary === "string" && input.summary.trim().length > 0
        ? input.summary.trim()
        : `OpenClaw returned non-terminal status "${input.status}" in synchronous /v1/responses mode.`;

    const nextErrors = [
      ...(Array.isArray(input.errors) ? input.errors : []),
      `Non-terminal status received from OpenClaw synchronous adapter: ${input.status}.`,
    ];

    const response: OpenClawTaskStatusResponse = {
      ...(typeof input.openclawTaskId === "string"
        ? { openclawTaskId: input.openclawTaskId }
        : {}),
      ...(typeof input.sessionId === "string"
        ? { sessionId: input.sessionId }
        : {}),
      agentId: input.agentId,
      status: "failed",
      summary: nonTerminalMessage,
      ...(input.outputs ? { outputs: input.outputs } : {}),
      ...(normalizedArtifacts.length > 0
        ? { artifacts: normalizedArtifacts }
        : {}),
      errors: nextErrors,
      ...(input.raw !== undefined ? { raw: input.raw } : {}),
    };

    return response;
  }

  private normalizeTaskArtifactRefs(value: unknown): string[] {
    if (!Array.isArray(value)) {
      return [];
    }

    const refs = new Set<string>();

    for (const item of value) {
      if (typeof item === "string") {
        const normalized = item.trim();
        if (normalized.length > 0) {
          refs.add(this.extractArtifactName(normalized));
        }
        continue;
      }

      if (typeof item !== "object" || item === null) {
        continue;
      }

      const candidate = item as Record<string, unknown>;
      const explicitName =
        typeof candidate.name === "string"
          ? candidate.name.trim()
          : typeof candidate.fileName === "string"
            ? candidate.fileName.trim()
            : "";
      const relativePath =
        typeof candidate.relativePath === "string"
          ? candidate.relativePath.trim()
          : typeof candidate.path === "string"
            ? candidate.path.trim()
            : "";

      const ref =
        explicitName.length > 0
          ? explicitName
          : relativePath.length > 0
            ? this.extractArtifactName(relativePath)
            : "";

      if (ref.length > 0) {
        refs.add(ref);
      }
    }

    return Array.from(refs);
  }

  private extractArtifactName(relativePath: string): string {
    const parts = relativePath.split("/").filter(Boolean);
    return parts[parts.length - 1] ?? relativePath;
  }

  private buildFailureMessage(result: OpenClawTaskStatusResponse): string {
    if (Array.isArray(result.errors) && result.errors.length > 0) {
      return result.errors.join("; ");
    }

    if (
      typeof result.summary === "string" &&
      result.summary.trim().length > 0
    ) {
      return result.summary.trim();
    }

    return `OpenClaw task ended with status "${result.status}".`;
  }

  private readOptionalString(
    source: Record<string, unknown>,
    keys: string[],
  ): string | undefined {
    for (const key of keys) {
      const value = source[key];

      if (typeof value === "string" && value.trim().length > 0) {
        return value.trim();
      }
    }

    return undefined;
  }

  private normalizeStringArray(value: unknown): string[] {
    if (!Array.isArray(value)) {
      return [];
    }

    return value
      .filter((item): item is string => typeof item === "string")
      .map((item) => item.trim())
      .filter((item) => item.length > 0);
  }

  private isRecord(value: unknown): value is Record<string, unknown> {
    return typeof value === "object" && value !== null && !Array.isArray(value);
  }

  private asRecord(value: unknown): Record<string, unknown> {
    return this.isRecord(value) ? value : {};
  }
}

export default AgentDispatchService;
