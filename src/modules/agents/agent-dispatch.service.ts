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

type PatchMilestonePlan = {
  title: string;
  goal?: string;
  description?: string;
  scope: string[];
  acceptanceCriteria: string[];
};

type MilestoneReviewOutcome = {
  decision: "pass" | "patch";
  summary?: string;
  metAcceptanceCriteria: string[];
  missingOrBrokenItems: string[];
  patchMilestone?: PatchMilestonePlan;
};

type PreparedConcreteTask = {
  index: number;
  plannedTask: PlannedTaskDefinition;
  idempotencyKey: string;
  sequence: number;
  targetAgentId: string;
  createIntent: CreateTaskInput["intent"];
  inputs: Record<string, unknown>;
  constraints: CreateTaskInput["constraints"];
  referenceKeys: string[];
  dependencyRefs: string[];
  existingTask?: TaskRecord;
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

    const isQaDispatch = task.status === "qa";
    const attemptNumber = isQaDispatch
      ? Math.max(task.attemptCount, 1)
      : task.attemptCount + 1;
    const sessionState = this.getTaskSessionState(task);

    const dispatchLogger = logger.child({
      jobId: job._id,
      projectId: job.projectId,
      milestoneId: task.milestoneId,
      taskId: task._id,
      intent: task.intent,
      agentId: task.target.agentId,
      attemptNumber,
      maxAttempts: task.maxAttempts,
      sessionCount: sessionState.sessionCount,
      maxSessions: sessionState.maxSessions,
      sequence: task.sequence,
    });

    dispatchLogger.info("Dispatching task to OpenClaw.");

    try {
      if (isQaDispatch) {
        await this.tasksService.updateTask(task._id, {
          status: "running",
        });
      } else {
        await this.tasksService.markRunning(task._id);
      }

      const dispatchInputs = await this.buildDispatchInputs(task);

      const sendResult = await this.openClawClient.sendTask({
        agentId: task.target.agentId,
        ...(sessionState.sessionName
          ? { sessionId: sessionState.sessionName }
          : {}),
        payload: {
          projectId: task.projectId,
          jobId: task.jobId,
          milestoneId: task.milestoneId,
          taskId: task._id,
          intent: String(task.intent),
          inputs: dispatchInputs,
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
          ...(sessionState.sessionName
            ? { sessionName: sessionState.sessionName }
            : {}),
          sessionCount: sessionState.sessionCount,
          maxSessions: sessionState.maxSessions,
          ...(task.errors.length > 0 ? { errors: task.errors } : {}),
          ...(typeof task.lastError === "string" &&
          task.lastError.trim().length > 0
            ? { lastError: task.lastError }
            : {}),
          ...(task.outputs ? { outputs: task.outputs } : {}),
          ...(task.artifacts.length > 0 ? { artifacts: task.artifacts } : {}),
        },
      });

      const finalResult = await this.resolveFinalResult({
        ...sendResult,
        agentId: sendResult.agentId ?? task.target.agentId,
      });

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

      input.dispatchLogger.warn(
        {
          failureMessage: input.failureMessage,
          nextRetryAt,
        },
        "Task failed but was requeued for retry.",
      );

      return;
    }

    if (this.shouldRotateTaskSession(input.task)) {
      await this.rotateTaskSessionAndRequeue({
        task: input.task,
        dispatchLogger: input.dispatchLogger,
        failureMessage: input.failureMessage,
        ...(input.outputs ? { outputs: input.outputs } : {}),
        ...(normalizedArtifacts.length > 0
          ? { artifacts: normalizedArtifacts }
          : {}),
      });

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

    const sessionState = this.getTaskSessionState(input.task);

    input.dispatchLogger.warn(
      {
        failureMessage: input.failureMessage,
        attemptNumber: input.attemptNumber,
        maxAttempts: input.task.maxAttempts,
        sessionCount: sessionState.sessionCount,
        maxSessions: sessionState.maxSessions,
      },
      "Task exhausted all retries and sessions and job was marked FAILED.",
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

    if (this.shouldRotateTaskSession(input.task)) {
      await this.rotateTaskSessionAndRequeue({
        task: input.task,
        dispatchLogger: input.dispatchLogger,
        failureMessage: input.failureMessage,
        resetTargetAgentId: this.implementerAgentId,
        ...(input.outputs ? { outputs: input.outputs } : {}),
        ...(normalizedArtifacts.length > 0
          ? { artifacts: normalizedArtifacts }
          : {}),
      });

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

    const sessionState = this.getTaskSessionState(input.task);

    input.dispatchLogger.warn(
      {
        failureMessage: input.failureMessage,
        attemptNumber: input.attemptNumber,
        maxAttempts: input.task.maxAttempts,
        sessionCount: sessionState.sessionCount,
        maxSessions: sessionState.maxSessions,
      },
      "QA review failed, retries and sessions were exhausted, and the job was marked FAILED.",
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

  private shouldRotateTaskSession(task: TaskRecord): boolean {
    const sessionState = this.getTaskSessionState(task);
    return (
      task.retryable && sessionState.sessionCount < sessionState.maxSessions
    );
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

  private async rotateTaskSessionAndRequeue(input: {
    task: TaskRecord;
    dispatchLogger: typeof logger;
    failureMessage: string;
    outputs?: Record<string, unknown>;
    artifacts?: string[];
    resetTargetAgentId?: string;
  }): Promise<void> {
    const currentSessionState = this.getTaskSessionState(input.task);
    const nextSessionCount = currentSessionState.sessionCount + 1;
    const nextSessionName = this.buildTaskSessionName(
      input.task,
      currentSessionState.sessionName,
      nextSessionCount,
    );

    await this.tasksService.updateTask(input.task._id, {
      attemptCount: 0,
      sessionCount: nextSessionCount,
      sessionName: nextSessionName,
      ...(typeof input.resetTargetAgentId === "string" &&
      input.resetTargetAgentId.trim().length > 0
        ? {
            target: {
              agentId: input.resetTargetAgentId.trim(),
            },
          }
        : {}),
    });

    await this.tasksService.requeueTask({
      taskId: input.task._id,
      error: input.failureMessage,
      ...(input.outputs ? { outputs: input.outputs } : {}),
      ...(Array.isArray(input.artifacts) && input.artifacts.length > 0
        ? { artifacts: input.artifacts }
        : {}),
    });

    input.dispatchLogger.warn(
      {
        failureMessage: input.failureMessage,
        previousSessionName: currentSessionState.sessionName,
        nextSessionName,
        previousSessionCount: currentSessionState.sessionCount,
        nextSessionCount,
        maxSessions: currentSessionState.maxSessions,
        ...(typeof input.resetTargetAgentId === "string" &&
        input.resetTargetAgentId.trim().length > 0
          ? { resetTargetAgentId: input.resetTargetAgentId.trim() }
          : {}),
      },
      "Task exhausted retries in the current session and was requeued into a fresh session.",
    );
  }

  private getTaskSessionState(task: TaskRecord): {
    sessionName: string;
    sessionCount: number;
    maxSessions: number;
  } {
    const taskRecord = task as TaskRecord & {
      sessionName?: unknown;
      sessionCount?: unknown;
      maxSessions?: unknown;
    };

    const sessionNameValue = taskRecord.sessionName;
    const sessionCountValue = taskRecord.sessionCount;
    const maxSessionsValue = taskRecord.maxSessions;

    const sessionName =
      typeof sessionNameValue === "string" && sessionNameValue.trim().length > 0
        ? sessionNameValue.trim()
        : this.buildTaskSessionBaseName(task);
    const sessionCount =
      typeof sessionCountValue === "number" &&
      Number.isFinite(sessionCountValue)
        ? Math.max(Math.trunc(sessionCountValue), 1)
        : 1;
    const maxSessions =
      typeof maxSessionsValue === "number" && Number.isFinite(maxSessionsValue)
        ? Math.max(Math.trunc(maxSessionsValue), 1)
        : 2;

    return {
      sessionName,
      sessionCount,
      maxSessions,
    };
  }

  private buildTaskSessionBaseName(task: TaskRecord): string {
    return `orchestrator:agent:${task.target.agentId}:task:${task._id}`;
  }

  private buildTaskSessionName(
    task: TaskRecord,
    baseSessionName: string | undefined,
    sessionCount: number,
  ): string {
    const base =
      typeof baseSessionName === "string" && baseSessionName.trim().length > 0
        ? baseSessionName.trim().replace(/:session-attempt:\d+$/u, "")
        : this.buildTaskSessionBaseName(task);

    return `${base}:session-attempt:${Math.max(Math.trunc(sessionCount), 1)}`;
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
        await this.enqueueConcreteMilestoneTasks(input);
        return;
      case "plan_next_tasks":
        await this.enqueueConcreteMilestoneTasks(input);
        return;
      case "review_milestone":
        await this.handleMilestoneReviewOutcome(input);
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
    const plannedTasks = this.extractPlannedTasks(
      input.finalResult.outputs,
    ).filter((plannedTask) => plannedTask.intent !== "review_milestone");

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

    const parentInputs = this.asRecord(input.task.inputs);
    const phaseId =
      this.readOptionalString(parentInputs, ["phaseId"]) ?? "phase-1";
    const phaseName =
      this.readOptionalString(parentInputs, ["phaseName"]) ?? "Current phase";
    const phaseGoal = this.readOptionalString(parentInputs, ["phaseGoal"]);

    const preparedTasks = this.prepareConcreteMilestoneTasks({
      plannerTask: input.task,
      plannedTasks,
      existingByIdempotencyKey,
      phaseId,
      phaseName,
      ...(phaseGoal ? { phaseGoal } : {}),
    });

    const resolvedTasks: Array<{
      preparedTask: PreparedConcreteTask;
      taskRecord: TaskRecord;
      created: boolean;
    }> = [];

    const createdTaskIds: string[] = [];
    const syncedExistingTaskRollbackStates: Array<{
      taskId: string;
      dependencies: string[];
      sequence: number;
      status: TaskRecord["status"];
    }> = [];
    let reviewTaskResult:
      | {
          reviewTaskId: string;
          created: boolean;
          updated: boolean;
        }
      | undefined;
    let skippedExistingTaskCount = 0;

    try {
      for (const preparedTask of preparedTasks) {
        let taskRecord = preparedTask.existingTask;
        let created = false;

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
              agentId: preparedTask.targetAgentId,
            },
            intent: preparedTask.createIntent,
            inputs: preparedTask.inputs,
            constraints: preparedTask.constraints,
            requiredArtifacts: preparedTask.plannedTask.requiredArtifacts,
            acceptanceCriteria: preparedTask.plannedTask.acceptanceCriteria,
            idempotencyKey: preparedTask.idempotencyKey,
            sequence: preparedTask.sequence,
          });

          existingByIdempotencyKey.set(preparedTask.idempotencyKey, taskRecord);
          createdTaskIds.push(taskRecord._id);
          created = true;
        } else {
          skippedExistingTaskCount += 1;
        }

        resolvedTasks.push({
          preparedTask,
          taskRecord,
          created,
        });
      }

      const referenceToTaskId = new Map<string, string>();

      for (const resolvedTask of resolvedTasks) {
        for (const referenceKey of resolvedTask.preparedTask.referenceKeys) {
          if (!referenceToTaskId.has(referenceKey)) {
            referenceToTaskId.set(referenceKey, resolvedTask.taskRecord._id);
          }
        }
      }

      for (const resolvedTask of resolvedTasks) {
        const dependencyTaskIds: string[] = [];

        for (const dependencyRef of resolvedTask.preparedTask.dependencyRefs) {
          const dependencyTaskId = referenceToTaskId.get(dependencyRef);

          if (!dependencyTaskId) {
            throw this.createPlannedTaskValidationError({
              code: "PLANNED_TASK_DEPENDENCY_RUNTIME_UNRESOLVED",
              message: `Validated dependency "${dependencyRef}" could not be resolved to a created task for ${this.describePlannedTask(resolvedTask.preparedTask.plannedTask, resolvedTask.preparedTask.index)}.`,
              plannerTask: input.task,
              plannedTask: resolvedTask.preparedTask.plannedTask,
              plannedTaskIndex: resolvedTask.preparedTask.index,
              details: {
                dependencyRef,
                idempotencyKey: resolvedTask.preparedTask.idempotencyKey,
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
        const shouldUpdateDependencies = !(
          currentDependencyIds.length === nextDependencyIds.length &&
          currentDependencyIds.every(
            (value, index) => value === nextDependencyIds[index],
          )
        );
        const shouldUpdateSequence =
          resolvedTask.taskRecord.sequence !==
          resolvedTask.preparedTask.sequence;
        const shouldResetQueued = this.shouldResetConcreteTaskToQueued(
          resolvedTask.taskRecord,
        );

        if (
          !shouldUpdateDependencies &&
          !shouldUpdateSequence &&
          !shouldResetQueued
        ) {
          continue;
        }

        if (!resolvedTask.created) {
          syncedExistingTaskRollbackStates.push({
            taskId: resolvedTask.taskRecord._id,
            dependencies: [...resolvedTask.taskRecord.dependencies],
            sequence: resolvedTask.taskRecord.sequence,
            status: resolvedTask.taskRecord.status,
          });
        }

        await this.tasksService.updateTask(resolvedTask.taskRecord._id, {
          ...(shouldUpdateDependencies
            ? { dependencies: dependencyTaskIds }
            : {}),
          ...(shouldUpdateSequence
            ? { sequence: resolvedTask.preparedTask.sequence }
            : {}),
          ...(shouldResetQueued ? { status: "queued" } : {}),
        });
      }

      reviewTaskResult = await this.ensureMilestoneReviewTask({
        createTask,
        plannerTask: input.task,
        milestone,
        phaseId,
        phaseName,
        ...(phaseGoal ? { phaseGoal } : {}),
        concreteTasks: resolvedTasks.map(
          (resolvedTask) => resolvedTask.taskRecord,
        ),
      });
    } catch (error) {
      await this.rollbackConcreteMilestoneTaskBatch({
        createdTaskIds,
        ...(reviewTaskResult?.created
          ? { createdReviewTaskId: reviewTaskResult.reviewTaskId }
          : {}),
        syncedExistingTaskRollbackStates,
        dispatchLogger: input.dispatchLogger,
      });

      throw error;
    }

    if (!reviewTaskResult) {
      throw createServiceError({
        message:
          "Milestone planning batch completed without producing a review task result.",
        code: "MILESTONE_REVIEW_SYNC_MISSING",
        statusCode: 500,
        details: {
          jobId: input.task.jobId,
          taskId: input.task._id,
          milestoneId: input.task.milestoneId,
        },
      });
    }

    input.dispatchLogger.info(
      {
        phaseId,
        createdTaskCount: createdTaskIds.length,
        skippedExistingTaskCount,
        createdTaskIds,
        reviewTaskId: reviewTaskResult.reviewTaskId,
        reviewTaskCreated: reviewTaskResult.created,
        reviewTaskUpdated: reviewTaskResult.updated,
      },
      "Committed concrete milestone tasks and synced the milestone review task atomically for the planning batch.",
    );
  }

  private shouldResetConcreteTaskToQueued(task: TaskRecord): boolean {
    return task.status === "failed" || task.status === "canceled";
  }

  private async rollbackConcreteMilestoneTaskBatch(input: {
    createdTaskIds: string[];
    createdReviewTaskId?: string;
    syncedExistingTaskRollbackStates: Array<{
      taskId: string;
      dependencies: string[];
      sequence: number;
      status: TaskRecord["status"];
    }>;
    dispatchLogger: typeof logger;
  }): Promise<void> {
    const rollbackErrors: Array<{ taskId: string; message: string }> = [];

    for (const rollbackState of [
      ...input.syncedExistingTaskRollbackStates,
    ].reverse()) {
      try {
        await this.tasksService.updateTask(rollbackState.taskId, {
          dependencies: rollbackState.dependencies,
          sequence: rollbackState.sequence,
          status: rollbackState.status,
        });
      } catch (error) {
        rollbackErrors.push({
          taskId: rollbackState.taskId,
          message:
            error instanceof Error ? error.message : "Unknown rollback error",
        });
      }
    }

    const createdTaskIds = [...input.createdTaskIds];
    if (input.createdReviewTaskId) {
      createdTaskIds.push(input.createdReviewTaskId);
    }

    for (const taskId of createdTaskIds.reverse()) {
      try {
        await this.tasksService.updateTask(taskId, {
          status: "canceled",
          dependencies: [],
        });
      } catch (error) {
        rollbackErrors.push({
          taskId,
          message:
            error instanceof Error ? error.message : "Unknown rollback error",
        });
      }
    }

    if (rollbackErrors.length > 0) {
      input.dispatchLogger.error(
        {
          rollbackErrors,
        },
        "Planning batch rollback encountered errors after a partial task creation failure.",
      );
      return;
    }

    input.dispatchLogger.warn(
      {
        rolledBackCreatedTaskIds: input.createdTaskIds,
        ...(input.createdReviewTaskId
          ? { rolledBackReviewTaskId: input.createdReviewTaskId }
          : {}),
        rolledBackExistingTaskCount:
          input.syncedExistingTaskRollbackStates.length,
      },
      "Rolled back the partial milestone planning batch after a follow-up task creation failure.",
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

  private async buildDispatchInputs(
    task: TaskRecord,
  ): Promise<Record<string, unknown>> {
    const baseInputs = this.asRecord(task.inputs);

    if (String(task.intent) !== "review_milestone") {
      return baseInputs;
    }

    const milestone = await this.milestonesService.requireMilestoneById(
      task.milestoneId,
    );

    const milestoneTasks = await this.tasksService.listTasks({
      jobId: task.jobId,
      milestoneId: task.milestoneId,
      limit: 500,
    });

    const reviewEvidence = milestoneTasks
      .filter(
        (candidateTask) =>
          candidateTask._id !== task._id &&
          this.shouldIncludeTaskInMilestoneReviewEvidence(
            String(candidateTask.intent),
          ),
      )
      .sort((left, right) => {
        if (left.sequence !== right.sequence) {
          return left.sequence - right.sequence;
        }

        return left.createdAt.getTime() - right.createdAt.getTime();
      })
      .map((candidateTask) => ({
        taskId: candidateTask._id,
        intent: String(candidateTask.intent),
        targetAgentId: candidateTask.target.agentId,
        status: candidateTask.status,
        acceptanceCriteria: [...candidateTask.acceptanceCriteria],
        ...(candidateTask.outputs ? { outputs: candidateTask.outputs } : {}),
        ...(candidateTask.artifacts.length > 0
          ? { artifacts: [...candidateTask.artifacts] }
          : {}),
        ...(candidateTask.errors.length > 0
          ? { errors: [...candidateTask.errors] }
          : {}),
        ...(typeof candidateTask.lastError === "string" &&
        candidateTask.lastError.trim().length > 0
          ? { lastError: candidateTask.lastError }
          : {}),
      }));

    return {
      ...baseInputs,
      milestone: {
        id: milestone._id,
        title: milestone.title,
        ...(milestone.description
          ? { description: milestone.description }
          : {}),
        order: milestone.order,
        status: milestone.status,
        ...(milestone.goal ? { goal: milestone.goal } : {}),
        scope: [...milestone.scope],
        acceptanceCriteria: [...milestone.acceptanceCriteria],
        ...(milestone.dependsOnMilestoneId
          ? { dependsOnMilestoneId: milestone.dependsOnMilestoneId }
          : {}),
      },
      milestoneExecution: {
        completedTaskCount: reviewEvidence.length,
        tasks: reviewEvidence,
      },
    };
  }

  private shouldIncludeTaskInMilestoneReviewEvidence(intent: string): boolean {
    return (
      intent !== "plan_project_phases" &&
      intent !== "plan_phase_tasks" &&
      intent !== "plan_next_tasks" &&
      intent !== "review_milestone"
    );
  }

  private async ensureMilestoneReviewTask(input: {
    createTask: NonNullable<TasksServicePort["createTask"]>;
    plannerTask: TaskRecord;
    milestone: MilestoneRecord;
    phaseId: string;
    phaseName: string;
    phaseGoal?: string;
    concreteTasks: TaskRecord[];
  }): Promise<{
    reviewTaskId: string;
    created: boolean;
    updated: boolean;
  }> {
    const reviewIdempotencyKey = `${input.plannerTask.jobId}:milestone:${input.plannerTask.milestoneId}:review_milestone`;
    const dependencyTaskIds = Array.from(
      new Set([
        input.plannerTask._id,
        ...input.concreteTasks.map((task) => task._id),
      ]),
    );
    const reviewInputs = this.buildMilestoneReviewTaskInputs({
      plannerTask: input.plannerTask,
      milestone: input.milestone,
      phaseId: input.phaseId,
      phaseName: input.phaseName,
      ...(input.phaseGoal ? { phaseGoal: input.phaseGoal } : {}),
      concreteTasks: input.concreteTasks,
    });
    const reviewSequence = this.getMilestoneReviewTaskSequence(
      input.plannerTask.sequence,
      input.concreteTasks,
    );
    const existingReviewTask =
      await this.tasksService.getTaskByIdempotencyKey(reviewIdempotencyKey);

    if (!existingReviewTask) {
      const reviewTask = await input.createTask({
        jobId: input.plannerTask.jobId,
        projectId: input.plannerTask.projectId,
        milestoneId: input.plannerTask.milestoneId,
        parentTaskId: input.plannerTask._id,
        dependencies: dependencyTaskIds,
        issuer: {
          kind: "system",
          id: "app-factory-orchestrator",
          role: "orchestrator",
        },
        target: {
          agentId: this.projectOwnerAgentId,
        },
        intent: this.asCreateTaskIntent("review_milestone"),
        inputs: reviewInputs,
        constraints: this.buildTaskConstraints(input.plannerTask.constraints),
        requiredArtifacts: [],
        acceptanceCriteria: [...input.milestone.acceptanceCriteria],
        idempotencyKey: reviewIdempotencyKey,
        sequence: reviewSequence,
      });

      return {
        reviewTaskId: reviewTask._id,
        created: true,
        updated: false,
      };
    }

    const canSyncQueuedReviewTask =
      existingReviewTask.status === "queued" ||
      existingReviewTask.status === "qa";

    const shouldRequeueFailedReviewTask =
      existingReviewTask.status === "failed" ||
      existingReviewTask.status === "canceled";

    if (canSyncQueuedReviewTask || shouldRequeueFailedReviewTask) {
      await this.tasksService.updateTask(existingReviewTask._id, {
        dependencies: dependencyTaskIds,
        target: {
          agentId: this.projectOwnerAgentId,
        },
        sequence: reviewSequence,
        ...(shouldRequeueFailedReviewTask ? { status: "queued" } : {}),
      });
    }

    return {
      reviewTaskId: existingReviewTask._id,
      created: false,
      updated: canSyncQueuedReviewTask || shouldRequeueFailedReviewTask,
    };
  }

  private buildMilestoneReviewTaskInputs(input: {
    plannerTask: TaskRecord;
    milestone: MilestoneRecord;
    phaseId: string;
    phaseName: string;
    phaseGoal?: string;
    concreteTasks: TaskRecord[];
  }): Record<string, unknown> {
    const plannerInputs = this.asRecord(input.plannerTask.inputs);

    return {
      ...(typeof plannerInputs.projectName === "string"
        ? { projectName: plannerInputs.projectName }
        : {}),
      ...(typeof plannerInputs.projectRequest === "string"
        ? { projectRequest: plannerInputs.projectRequest }
        : typeof plannerInputs.prompt === "string"
          ? { projectRequest: plannerInputs.prompt }
          : {}),
      ...(typeof plannerInputs.request === "string"
        ? { request: plannerInputs.request }
        : {}),
      ...(typeof plannerInputs.appType === "string"
        ? { appType: plannerInputs.appType }
        : {}),
      ...(typeof plannerInputs.stack === "string"
        ? { stack: plannerInputs.stack }
        : {}),
      ...(typeof plannerInputs.deployment === "string"
        ? { deployment: plannerInputs.deployment }
        : {}),
      milestoneId: input.milestone._id,
      phaseId: input.phaseId,
      phaseName: input.phaseName,
      ...(input.phaseGoal ? { phaseGoal: input.phaseGoal } : {}),
      milestone: {
        id: input.milestone._id,
        title: input.milestone.title,
        ...(input.milestone.description
          ? { description: input.milestone.description }
          : {}),
        order: input.milestone.order,
        ...(input.milestone.goal ? { goal: input.milestone.goal } : {}),
        scope: [...input.milestone.scope],
        acceptanceCriteria: [...input.milestone.acceptanceCriteria],
        ...(input.milestone.dependsOnMilestoneId
          ? { dependsOnMilestoneId: input.milestone.dependsOnMilestoneId }
          : {}),
      },
      reviewContract: {
        allowedDecisions: ["pass", "patch"],
        patchRule:
          "If the milestone is incomplete or broken, define the smallest possible patch milestone needed to satisfy the stated acceptance criteria without adding new scope.",
      },
      milestoneTaskPlan: input.concreteTasks.map((task) => ({
        taskId: task._id,
        intent: String(task.intent),
        targetAgentId: task.target.agentId,
        acceptanceCriteria: [...task.acceptanceCriteria],
      })),
    };
  }

  private getMilestoneReviewTaskSequence(
    plannerSequence: number,
    concreteTasks: TaskRecord[],
  ): number {
    let maxSequence = plannerSequence;

    for (const concreteTask of concreteTasks) {
      if (concreteTask.sequence > maxSequence) {
        maxSequence = concreteTask.sequence;
      }
    }

    return maxSequence + 10;
  }

  private async handleMilestoneReviewOutcome(input: {
    job: JobRecord;
    task: TaskRecord;
    finalResult: OpenClawTaskStatusResponse;
    dispatchLogger: typeof logger;
  }): Promise<void> {
    const milestone = await this.milestonesService.requireMilestoneById(
      input.task.milestoneId,
    );
    const outcome = this.extractMilestoneReviewOutcome(
      input.finalResult.outputs,
    );

    if (!outcome) {
      throw createServiceError({
        message:
          "review_milestone succeeded but returned no usable decision in outputs.decision.",
        code: "MILESTONE_REVIEW_OUTPUT_MISSING",
        statusCode: 500,
        details: {
          jobId: input.task.jobId,
          taskId: input.task._id,
          outputs: input.finalResult.outputs,
        },
      });
    }

    if (outcome.decision === "pass") {
      input.dispatchLogger.info(
        {
          decision: outcome.decision,
          milestoneId: milestone._id,
          milestoneTitle: milestone.title,
          metAcceptanceCriteria: outcome.metAcceptanceCriteria,
        },
        "Milestone review passed with no patch milestone required.",
      );
      return;
    }

    const patchResult = await this.createPatchMilestoneFromReview({
      job: input.job,
      reviewTask: input.task,
      sourceMilestone: milestone,
      outcome,
    });

    input.dispatchLogger.info(
      {
        sourceMilestoneId: milestone._id,
        sourceMilestoneTitle: milestone.title,
        patchMilestoneId: patchResult.patchMilestone._id,
        patchMilestoneTitle: patchResult.patchMilestone.title,
        patchPlannerTaskId: patchResult.patchPlannerTaskId,
        createdPatchMilestone: patchResult.createdPatchMilestone,
        reusedPatchMilestone: patchResult.reusedPatchMilestone,
        rewiredMilestoneIds: patchResult.rewiredMilestoneIds,
      },
      "Milestone review requested a patch milestone and the follow-up work was enqueued.",
    );
  }

  private extractMilestoneReviewOutcome(
    outputs: unknown,
  ): MilestoneReviewOutcome | null {
    const record = this.asRecord(outputs);
    const nestedOutputs = this.asRecord(record.outputs);
    const source = Object.keys(record).length > 0 ? record : nestedOutputs;

    const decisionValue =
      this.readOptionalString(source, [
        "decision",
        "reviewDecision",
        "outcome",
      ]) ??
      this.readOptionalString(nestedOutputs, [
        "decision",
        "reviewDecision",
        "outcome",
      ]);

    const decision =
      decisionValue === "pass" || decisionValue === "patch"
        ? decisionValue
        : decisionValue?.toLowerCase() === "pass"
          ? "pass"
          : decisionValue?.toLowerCase() === "patch"
            ? "patch"
            : undefined;

    if (!decision) {
      return null;
    }

    const metAcceptanceCriteria = this.normalizeStringArray(
      source.metAcceptanceCriteria ??
        source.criteriaMet ??
        nestedOutputs.metAcceptanceCriteria ??
        nestedOutputs.criteriaMet,
    );
    const missingOrBrokenItems = this.normalizeStringArray(
      source.missingOrBrokenItems ??
        source.issues ??
        nestedOutputs.missingOrBrokenItems ??
        nestedOutputs.issues,
    );
    const summary =
      this.readOptionalString(source, ["summary"]) ??
      this.readOptionalString(nestedOutputs, ["summary"]);
    const patchMilestone = this.normalizePatchMilestonePlan(
      source.patchMilestone ??
        source.patch ??
        nestedOutputs.patchMilestone ??
        nestedOutputs.patch,
      missingOrBrokenItems,
    );

    return {
      decision,
      ...(summary ? { summary } : {}),
      metAcceptanceCriteria,
      missingOrBrokenItems,
      ...(patchMilestone ? { patchMilestone } : {}),
    };
  }

  private normalizePatchMilestonePlan(
    value: unknown,
    fallbackItems: string[],
  ): PatchMilestonePlan | null {
    const record = this.asRecord(value);

    if (Object.keys(record).length === 0 && fallbackItems.length === 0) {
      return null;
    }

    const title = this.readOptionalString(record, ["title", "name"]);
    const goal = this.readOptionalString(record, ["goal"]);
    const rawDescription = this.readOptionalString(record, ["description"]);
    const scope = this.normalizeStringArray(
      record.scope ?? record.deliverables ?? fallbackItems,
    );
    const acceptanceCriteria = this.normalizeStringArray(
      record.acceptanceCriteria ?? record.exitCriteria ?? fallbackItems,
    );

    if (!title && acceptanceCriteria.length === 0 && scope.length === 0) {
      return null;
    }

    return {
      title: title ?? "Patch milestone",
      ...(goal ? { goal } : {}),
      ...(rawDescription ? { description: rawDescription } : {}),
      scope,
      acceptanceCriteria,
    };
  }

  private async createPatchMilestoneFromReview(input: {
    job: JobRecord;
    reviewTask: TaskRecord;
    sourceMilestone: MilestoneRecord;
    outcome: MilestoneReviewOutcome;
  }): Promise<{
    patchMilestone: MilestoneRecord;
    patchPlannerTaskId?: string;
    createdPatchMilestone: boolean;
    reusedPatchMilestone: boolean;
    rewiredMilestoneIds: string[];
  }> {
    const createTask = this.getCreateTaskOrThrow();
    const milestones = await this.milestonesService.listMilestones({
      projectId: input.job.projectId,
      limit: 200,
    });

    let patchMilestone = milestones.find((milestone) =>
      this.isPatchMilestoneForReview(milestone, input.reviewTask._id),
    );
    let createdPatchMilestone = false;
    let reusedPatchMilestone = false;

    if (!patchMilestone) {
      await this.shiftMilestoneOrdersForPatchInsertion({
        sourceMilestone: input.sourceMilestone,
        milestones,
      });

      const patchPlan = this.buildPatchMilestonePlan({
        sourceMilestone: input.sourceMilestone,
        reviewTaskId: input.reviewTask._id,
        outcome: input.outcome,
      });

      patchMilestone = await this.milestonesService.createMilestone({
        projectId: input.job.projectId,
        title: patchPlan.title,
        ...(patchPlan.description
          ? { description: patchPlan.description }
          : {}),
        order: input.sourceMilestone.order + 1,
        status: "ready",
        ...(patchPlan.goal ? { goal: patchPlan.goal } : {}),
        scope: [...patchPlan.scope],
        acceptanceCriteria: [...patchPlan.acceptanceCriteria],
        dependsOnMilestoneId: input.sourceMilestone._id,
      });
      createdPatchMilestone = true;
    } else {
      reusedPatchMilestone = true;
    }

    const refreshedMilestones = await this.milestonesService.listMilestones({
      projectId: input.job.projectId,
      limit: 200,
    });
    const rewiredMilestoneIds = await this.rewireMilestonesToPatch({
      sourceMilestone: input.sourceMilestone,
      patchMilestone,
      milestones: refreshedMilestones,
    });

    const patchPlannerIdempotencyKey = `${input.reviewTask.jobId}:milestone:${patchMilestone._id}:plan_phase_tasks`;
    const existingPatchPlannerTask =
      await this.tasksService.getTaskByIdempotencyKey(
        patchPlannerIdempotencyKey,
      );

    let patchPlannerTaskId: string | undefined = existingPatchPlannerTask?._id;

    if (!existingPatchPlannerTask) {
      const patchPlannerTask = await createTask({
        jobId: input.reviewTask.jobId,
        projectId: input.reviewTask.projectId,
        milestoneId: patchMilestone._id,
        parentTaskId: input.reviewTask._id,
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
          ...(typeof input.reviewTask.inputs === "object" &&
          input.reviewTask.inputs !== null
            ? this.asRecord(input.reviewTask.inputs)
            : {}),
          milestoneId: patchMilestone._id,
          phaseId: `patch-${patchMilestone.order}`,
          phaseName: patchMilestone.title,
          ...(patchMilestone.goal ? { phaseGoal: patchMilestone.goal } : {}),
          phase: {
            phaseId: `patch-${patchMilestone.order}`,
            name: patchMilestone.title,
            ...(patchMilestone.goal ? { goal: patchMilestone.goal } : {}),
            ...(patchMilestone.description
              ? { description: patchMilestone.description }
              : {}),
            deliverables: [...patchMilestone.scope],
            exitCriteria: [...patchMilestone.acceptanceCriteria],
            sourceMilestoneId: input.sourceMilestone._id,
            reviewTaskId: input.reviewTask._id,
            patchForMilestoneId: input.sourceMilestone._id,
            patchReason: input.outcome.missingOrBrokenItems,
          },
        },
        constraints: this.buildTaskConstraints(input.reviewTask.constraints),
        requiredArtifacts: [],
        acceptanceCriteria: [...patchMilestone.acceptanceCriteria],
        idempotencyKey: patchPlannerIdempotencyKey,
        sequence: 0,
      });

      patchPlannerTaskId = patchPlannerTask._id;
    }

    return {
      patchMilestone,
      ...(patchPlannerTaskId ? { patchPlannerTaskId } : {}),
      createdPatchMilestone,
      reusedPatchMilestone: reusedPatchMilestone || !createdPatchMilestone,
      rewiredMilestoneIds,
    };
  }

  private buildPatchMilestonePlan(input: {
    sourceMilestone: MilestoneRecord;
    reviewTaskId: string;
    outcome: MilestoneReviewOutcome;
  }): PatchMilestonePlan {
    const reviewMarker = this.buildPatchReviewMarker(input.reviewTaskId);
    const fallbackTitle = `${input.sourceMilestone.title} Patch`;
    const requestedPatch = input.outcome.patchMilestone;
    const title = requestedPatch?.title?.trim().length
      ? requestedPatch.title.trim()
      : fallbackTitle;
    const goal = requestedPatch?.goal?.trim().length
      ? requestedPatch.goal.trim()
      : input.outcome.summary;
    const descriptionLines = [reviewMarker];

    if (
      typeof requestedPatch?.description === "string" &&
      requestedPatch.description.trim().length > 0
    ) {
      descriptionLines.push(requestedPatch.description.trim());
    }

    const scopeSource = requestedPatch?.scope.length
      ? requestedPatch.scope
      : input.outcome.missingOrBrokenItems;
    const acceptanceCriteriaSource = requestedPatch?.acceptanceCriteria.length
      ? requestedPatch.acceptanceCriteria
      : input.outcome.missingOrBrokenItems;

    return {
      title,
      ...(goal ? { goal } : {}),
      description: descriptionLines.join("\n\n"),
      scope: [...scopeSource],
      acceptanceCriteria: [...acceptanceCriteriaSource],
    };
  }

  private buildPatchReviewMarker(reviewTaskId: string): string {
    return `Patch source review task: ${reviewTaskId}`;
  }

  private isPatchMilestoneForReview(
    milestone: MilestoneRecord,
    reviewTaskId: string,
  ): boolean {
    return (
      typeof milestone.description === "string" &&
      milestone.description.includes(this.buildPatchReviewMarker(reviewTaskId))
    );
  }

  private async shiftMilestoneOrdersForPatchInsertion(input: {
    sourceMilestone: MilestoneRecord;
    milestones: MilestoneRecord[];
  }): Promise<void> {
    const milestonesToShift = input.milestones
      .filter((milestone) => milestone.order > input.sourceMilestone.order)
      .sort((left, right) => right.order - left.order);

    for (const milestone of milestonesToShift) {
      await this.milestonesService.updateMilestone(milestone._id, {
        order: milestone.order + 1,
      });
    }
  }

  private async rewireMilestonesToPatch(input: {
    sourceMilestone: MilestoneRecord;
    patchMilestone: MilestoneRecord;
    milestones: MilestoneRecord[];
  }): Promise<string[]> {
    const rewiredMilestoneIds: string[] = [];
    const downstreamMilestones = input.milestones.filter(
      (milestone) =>
        milestone._id !== input.patchMilestone._id &&
        milestone.dependsOnMilestoneId === input.sourceMilestone._id,
    );

    for (const downstreamMilestone of downstreamMilestones) {
      await this.milestonesService.updateMilestone(downstreamMilestone._id, {
        dependsOnMilestoneId: input.patchMilestone._id,
      });
      rewiredMilestoneIds.push(downstreamMilestone._id);
    }

    return rewiredMilestoneIds;
  }

  private prepareConcreteMilestoneTasks(input: {
    plannerTask: TaskRecord;
    plannedTasks: PlannedTaskDefinition[];
    existingByIdempotencyKey: Map<string, TaskRecord>;
    phaseId: string;
    phaseName: string;
    phaseGoal?: string;
  }): PreparedConcreteTask[] {
    const preparedTasks: PreparedConcreteTask[] = [];
    const seenIdempotencyKeys = new Set<string>();
    const referenceKeyOwnerByKey = new Map<string, PreparedConcreteTask>();

    for (const [index, plannedTask] of input.plannedTasks.entries()) {
      const idempotencyKey =
        plannedTask.idempotencyKey ??
        `${input.plannerTask.jobId}:${input.plannerTask.milestoneId}:planned-task:${index + 1}:${plannedTask.intent}`;
      const referenceKeys = this.buildPlannedTaskReferenceKeys(
        plannedTask,
        idempotencyKey,
      );
      const targetAgentId = this.resolveTargetAgentId(
        plannedTask.intent,
        plannedTask.targetAgentId,
      );
      const preparedTask: PreparedConcreteTask = {
        index,
        plannedTask,
        idempotencyKey,
        sequence: this.getConcreteTaskSequence(
          input.plannerTask.sequence,
          index,
        ),
        targetAgentId,
        createIntent: this.asCreateTaskIntent(plannedTask.intent),
        inputs: this.buildConcreteTaskInputs({
          parentTask: input.plannerTask,
          plannedTask,
          phaseId: input.phaseId,
          phaseName: input.phaseName,
          ...(input.phaseGoal ? { phaseGoal: input.phaseGoal } : {}),
        }),
        constraints: this.buildTaskConstraints(
          input.plannerTask.constraints,
          plannedTask.constraints,
        ),
        referenceKeys,
        dependencyRefs: [...plannedTask.dependsOn],
        ...(input.existingByIdempotencyKey.get(idempotencyKey)
          ? {
              existingTask: input.existingByIdempotencyKey.get(idempotencyKey)!,
            }
          : {}),
      };

      if (seenIdempotencyKeys.has(idempotencyKey)) {
        throw this.createPlannedTaskValidationError({
          code: "PLANNED_TASK_DUPLICATE_IDEMPOTENCY_KEY",
          message: `Planned task list contains a duplicate idempotency key "${idempotencyKey}" for ${this.describePlannedTask(plannedTask, index)}. Each task must be uniquely identifiable.`,
          plannerTask: input.plannerTask,
          plannedTask,
          plannedTaskIndex: index,
          details: {
            idempotencyKey,
          },
        });
      }

      seenIdempotencyKeys.add(idempotencyKey);

      for (const referenceKey of referenceKeys) {
        const existingOwner = referenceKeyOwnerByKey.get(referenceKey);
        if (existingOwner) {
          throw this.createPlannedTaskValidationError({
            code: "PLANNED_TASK_DUPLICATE_REFERENCE",
            message: `Planned task reference "${referenceKey}" is used more than once. ${this.describePlannedTask(plannedTask, index)} conflicts with ${this.describePlannedTask(existingOwner.plannedTask, existingOwner.index)}.`,
            plannerTask: input.plannerTask,
            plannedTask,
            plannedTaskIndex: index,
            details: {
              referenceKey,
              conflictingTaskIndex: existingOwner.index,
              conflictingTaskLocalId: existingOwner.plannedTask.localId,
              conflictingTaskIntent: existingOwner.plannedTask.intent,
            },
          });
        }
        referenceKeyOwnerByKey.set(referenceKey, preparedTask);
      }

      if (preparedTask.existingTask) {
        this.verifyExistingConcreteTaskRecord({
          plannerTask: input.plannerTask,
          preparedTask,
        });
      }

      preparedTasks.push(preparedTask);
    }

    for (const preparedTask of preparedTasks) {
      for (const dependencyRef of preparedTask.dependencyRefs) {
        const owner = referenceKeyOwnerByKey.get(dependencyRef);

        if (!owner) {
          throw this.createPlannedTaskValidationError({
            code: "PLANNED_TASK_DEPENDENCY_UNRESOLVED",
            message: `${this.describePlannedTask(preparedTask.plannedTask, preparedTask.index)} depends on "${dependencyRef}", but no task in the plan matches that reference. Fix the dependsOn values and return the full corrected task list.`,
            plannerTask: input.plannerTask,
            plannedTask: preparedTask.plannedTask,
            plannedTaskIndex: preparedTask.index,
            details: {
              dependencyRef,
              availableReferences: Array.from(referenceKeyOwnerByKey.keys()),
            },
          });
        }

        if (owner.idempotencyKey === preparedTask.idempotencyKey) {
          throw this.createPlannedTaskValidationError({
            code: "PLANNED_TASK_SELF_DEPENDENCY",
            message: `${this.describePlannedTask(preparedTask.plannedTask, preparedTask.index)} cannot depend on itself (${dependencyRef}). Remove that self-dependency and return the full corrected task list.`,
            plannerTask: input.plannerTask,
            plannedTask: preparedTask.plannedTask,
            plannedTaskIndex: preparedTask.index,
            details: {
              dependencyRef,
            },
          });
        }
      }
    }

    return preparedTasks;
  }

  private verifyExistingConcreteTaskRecord(input: {
    plannerTask: TaskRecord;
    preparedTask: PreparedConcreteTask;
  }): void {
    const existingTask = input.preparedTask.existingTask;

    if (!existingTask) {
      return;
    }

    if (
      existingTask.milestoneId !== input.plannerTask.milestoneId ||
      existingTask.jobId !== input.plannerTask.jobId ||
      existingTask.parentTaskId !== input.plannerTask._id
    ) {
      throw this.createPlannedTaskValidationError({
        code: "PLANNED_TASK_EXISTING_RECORD_MISMATCH",
        message: `${this.describePlannedTask(input.preparedTask.plannedTask, input.preparedTask.index)} reuses idempotency key "${input.preparedTask.idempotencyKey}", but the existing task belongs to a different parent or milestone. Return a corrected task list with stable unique task identifiers.`,
        plannerTask: input.plannerTask,
        plannedTask: input.preparedTask.plannedTask,
        plannedTaskIndex: input.preparedTask.index,
        details: {
          idempotencyKey: input.preparedTask.idempotencyKey,
          existingTaskId: existingTask._id,
          existingMilestoneId: existingTask.milestoneId,
          existingParentTaskId: existingTask.parentTaskId,
        },
      });
    }

    if (
      String(existingTask.intent) !== input.preparedTask.plannedTask.intent ||
      existingTask.target.agentId !== input.preparedTask.targetAgentId
    ) {
      throw this.createPlannedTaskValidationError({
        code: "PLANNED_TASK_EXISTING_RECORD_SHAPE_MISMATCH",
        message: `${this.describePlannedTask(input.preparedTask.plannedTask, input.preparedTask.index)} reuses idempotency key "${input.preparedTask.idempotencyKey}", but the existing task has a different intent or target agent. Return a corrected task list with unique task ids or consistent task shapes.`,
        plannerTask: input.plannerTask,
        plannedTask: input.preparedTask.plannedTask,
        plannedTaskIndex: input.preparedTask.index,
        details: {
          idempotencyKey: input.preparedTask.idempotencyKey,
          existingTaskId: existingTask._id,
          existingIntent: String(existingTask.intent),
          existingTargetAgentId: existingTask.target.agentId,
          expectedIntent: input.preparedTask.plannedTask.intent,
          expectedTargetAgentId: input.preparedTask.targetAgentId,
        },
      });
    }
  }

  private describePlannedTask(
    plannedTask: PlannedTaskDefinition,
    index: number,
  ): string {
    const label = plannedTask.localId ?? `task-${index + 1}`;
    return `planned task ${label} (intent=${plannedTask.intent})`;
  }

  private createPlannedTaskValidationError(input: {
    code: string;
    message: string;
    plannerTask: TaskRecord;
    plannedTask: PlannedTaskDefinition;
    plannedTaskIndex: number;
    details?: Record<string, unknown>;
  }): ServiceError {
    return createServiceError({
      message: input.message,
      code: input.code,
      statusCode: 500,
      details: {
        jobId: input.plannerTask.jobId,
        milestoneId: input.plannerTask.milestoneId,
        plannerTaskId: input.plannerTask._id,
        plannedTaskIndex: input.plannedTaskIndex,
        plannedTaskLocalId: input.plannedTask.localId,
        plannedTaskIntent: input.plannedTask.intent,
        ...(input.details ?? {}),
      },
    });
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
      case "review_milestone":
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
