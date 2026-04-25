import { createLogger } from "../../config/logger";
import type { JobRecord, JobsServicePort } from "../jobs/job.service";
import type {
  MilestoneRecord,
  MilestonesServicePort,
} from "../milestones/milestone.service";
import type { ProjectsServicePort } from "../projects/project.service";
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
  retryable?: boolean;
};

function createServiceError(input: {
  message: string;
  code: string;
  statusCode: number;
  details?: unknown;
  retryable?: boolean;
}): ServiceError {
  const error = new Error(input.message) as ServiceError;
  error.code = input.code;
  error.statusCode = input.statusCode;

  if ("details" in input) {
    error.details = input.details;
  }

  if (typeof input.retryable === "boolean") {
    error.retryable = input.retryable;
  }

  return error;
}

type PlanningValidationIssue = {
  code: string;
  message: string;
  stage:
    | "planned-task-validation"
    | "planned-task-graph"
    | "expanded-task-validation"
    | "batch-preflight";
  plannedTaskIndex?: number;
  taskLocalId?: string;
  taskIntent?: string;
  taskVariant?: "enrichment" | "execution" | "review";
  operationKind?: "create" | "update";
  operationIndex?: number;
  ownerLabel?: string;
  idempotencyKey?: string;
  field?: string;
  details?: Record<string, unknown>;
};

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
  variant: "enrichment" | "execution";
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

type AtomicMilestonePlanningBatchMutation = {
  referenceKeys: string[];
  dependencyTaskIds: string[];
  dependencyRefs: string[];
};

type AtomicMilestonePlanningBatchCreate =
  AtomicMilestonePlanningBatchMutation & {
    kind: "create";
    task: CreateTaskInput;
  };

type AtomicMilestonePlanningBatchUpdate =
  AtomicMilestonePlanningBatchMutation & {
    kind: "update";
    taskId: string;
    patch: Record<string, unknown>;
  };

type AtomicMilestonePlanningBatch = {
  plannerTaskId: string;
  jobId: string;
  projectId: string;
  milestoneId: string;
  creates: AtomicMilestonePlanningBatchCreate[];
  updates: AtomicMilestonePlanningBatchUpdate[];
};

type CommittedMilestonePlanningBatchTask = {
  stage: "validation" | "seed_updates" | "create" | "update" | "finalize";
  operationKind: "create" | "update";
  operationIndex: number;
  taskId: string;
  ownerLabel: string;
  referenceKeys: string[];
  dependencyTaskIds: string[];
  dependencyRefs: string[];
  taskIntent?: string;
  idempotencyKey?: string;
};

type AtomicMilestonePlanningBatchResult = {
  createdTaskIds: string[];
  updatedTaskIds: string[];
  reviewTaskId: string;
  reviewTaskCreated: boolean;
  reviewTaskUpdated: boolean;
  createdTasks: CommittedMilestonePlanningBatchTask[];
  updatedTasks: CommittedMilestonePlanningBatchTask[];
};

type AtomicMilestonePlanningBatchCommitter = (
  batch: AtomicMilestonePlanningBatch,
) => Promise<AtomicMilestonePlanningBatchResult>;

export type AgentDispatchServiceDependencies = {
  openClawClient: OpenClawClient;
  tasksService: TasksServicePort;
  jobsService: JobsServicePort;
  milestonesService: MilestonesServicePort;
  projectsService?: ProjectsServicePort | undefined;
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
  private readonly projectsService: ProjectsServicePort | undefined;
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
    this.projectsService = dependencies.projectsService;
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

      const followUpHandledSuccess = await this.enqueueFollowUpWork({
        job,
        task,
        finalResult,
        dispatchLogger,
      });

      if (!followUpHandledSuccess) {
        const normalizedArtifacts = this.normalizeTaskArtifactRefs(
          finalResult.artifacts,
        );

        await this.tasksService.markSucceeded({
          taskId: task._id,
          ...(finalResult.outputs ? { outputs: finalResult.outputs } : {}),
          ...(normalizedArtifacts.length > 0
            ? { artifacts: normalizedArtifacts }
            : {}),
        });
      }

      dispatchLogger.info("Task succeeded.");
    } catch (error) {
      const failureMessage = this.buildDispatchFailureMessage(
        error,
        error instanceof Error ? error.message : "Unknown dispatch error",
      );

      dispatchLogger.error(
        { err: error },
        "Task dispatch failed unexpectedly.",
      );

      if (isQaDispatch) {
        await this.handleQaReviewFailure({
          job,
          task,
          attemptNumber,
          dispatchLogger,
          failureMessage,
          error,
        });
      } else {
        await this.handleTaskFailure({
          job,
          task,
          attemptNumber,
          dispatchLogger,
          failureMessage,
          error,
        });
      }
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
    error?: unknown;
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
    error?: unknown;
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

  private getErrorCode(error: unknown): string | undefined {
    if (!error || typeof error !== "object") {
      return undefined;
    }

    const code = (error as { code?: unknown }).code;
    return typeof code === "string" && code.trim().length > 0
      ? code.trim()
      : undefined;
  }

  private getErrorStatusCode(error: unknown): number | undefined {
    if (!error || typeof error !== "object") {
      return undefined;
    }

    const statusCode = (error as { statusCode?: unknown }).statusCode;
    return typeof statusCode === "number" ? statusCode : undefined;
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
  }): Promise<boolean> {
    const intent = String(input.task.intent);

    switch (intent) {
      case "plan_project_phases":
        await this.createMilestonesAndPlannerTasks(input);
        return false;
      case "plan_phase_tasks":
        await this.enqueueConcreteMilestoneTasks(input);
        return true;
      case "plan_next_tasks":
        await this.enqueueConcreteMilestoneTasks(input);
        return true;
      case "review_milestone":
        await this.handleMilestoneReviewOutcome(input);
        return false;
      case "enrich_task":
        await this.applyEnrichmentToExecutionTask(input);
        return false;
      default:
        return false;
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
        retryable: false,
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
    const plannedTasks = this.extractPlannedTasks(
      input.finalResult.outputs,
    ).filter((plannedTask) => plannedTask.intent !== "review_milestone");

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

    const parentInputs = this.asRecord(input.task.inputs);
    const phaseId =
      this.readOptionalString(parentInputs, ["phaseId"]) ?? "phase-1";
    const phaseName =
      this.readOptionalString(parentInputs, ["phaseName"]) ?? "Current phase";
    const phaseGoal = this.readOptionalString(parentInputs, ["phaseGoal"]);

    const plannedTaskValidation = this.validatePlannedTaskList({
      plannerTask: input.task,
      plannedTasks,
    });
    plannerValidationIssues.push(...plannedTaskValidation.issues);

    let preparedTasks: PreparedConcreteTask[] = [];

    if (plannerValidationIssues.length === 0) {
      preparedTasks = this.prepareConcreteMilestoneTasks({
        plannerTask: input.task,
        plannedTasks,
        plannedTaskMetadata: plannedTaskValidation.plannedTaskMetadata,
        rawReferenceOwnerByKey: plannedTaskValidation.rawReferenceOwnerByKey,
        existingByIdempotencyKey,
        phaseId,
        phaseName,
        ...(phaseGoal ? { phaseGoal } : {}),
      });

      plannerValidationIssues.push(
        ...this.validatePreparedConcreteMilestoneTasks({
          plannerTask: input.task,
          preparedTasks,
        }),
      );
    }

    if (plannerValidationIssues.length > 0) {
      throw this.createAggregatePlannedTaskValidationError({
        plannerTask: input.task,
        issues: plannerValidationIssues,
      });
    }

    const batch = this.prepareAtomicMilestonePlanningBatch({
      plannerTask: input.task,
      milestone,
      preparedTasks,
      existingByIdempotencyKey,
      phaseId,
      phaseName,
      ...(phaseGoal ? { phaseGoal } : {}),
    });

    const validateMilestonePlanningBatch =
      this.getValidateMilestonePlanningBatchOrThrow();
    const preflightResult = await validateMilestonePlanningBatch(batch);
    const batchIssues = this.normalizeBatchValidationIssues(preflightResult);
    if (batchIssues.length > 0) {
      throw this.createAggregatePlannedTaskValidationError({
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

  private prepareAtomicMilestonePlanningBatch(input: {
    plannerTask: TaskRecord;
    milestone: MilestoneRecord;
    preparedTasks: PreparedConcreteTask[];
    existingByIdempotencyKey: Map<string, TaskRecord>;
    phaseId: string;
    phaseName: string;
    phaseGoal?: string;
  }): AtomicMilestonePlanningBatch {
    const creates: AtomicMilestonePlanningBatchCreate[] = [];
    const updates: AtomicMilestonePlanningBatchUpdate[] = [];

    for (const preparedTask of input.preparedTasks) {
      const dependencyRefs = [...new Set(preparedTask.dependencyRefs)];
      const referenceKeys = [...new Set(preparedTask.referenceKeys)];

      if (!preparedTask.existingTask) {
        creates.push({
          kind: "create",
          referenceKeys,
          dependencyTaskIds: [],
          dependencyRefs,
          task: {
            jobId: input.plannerTask.jobId,
            projectId: input.plannerTask.projectId,
            milestoneId: input.plannerTask.milestoneId,
            parentTaskId: input.plannerTask._id,
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
            requiredArtifacts:
              preparedTask.variant === "execution"
                ? preparedTask.plannedTask.requiredArtifacts
                : [],
            acceptanceCriteria:
              preparedTask.variant === "execution"
                ? preparedTask.plannedTask.acceptanceCriteria
                : [
                    `Produce enriched task context for ${this.describePlannedTask(preparedTask.plannedTask, preparedTask.index)} without changing the approved scope.`,
                  ],
            idempotencyKey: preparedTask.idempotencyKey,
            sequence: preparedTask.sequence,
          },
        });
        continue;
      }

      const shouldUpdateSequence =
        preparedTask.existingTask.sequence !== preparedTask.sequence;
      const shouldResetQueued = this.shouldResetConcreteTaskToQueued(
        preparedTask.existingTask,
      );
      const canSyncExistingTask = this.canSyncConcreteTaskDefinition(
        preparedTask.existingTask,
      );
      const concreteRequiredArtifacts =
        preparedTask.variant === "execution"
          ? [...preparedTask.plannedTask.requiredArtifacts]
          : [];
      const concreteAcceptanceCriteria =
        preparedTask.variant === "execution"
          ? [...preparedTask.plannedTask.acceptanceCriteria]
          : [
              `Produce enriched task context for ${this.describePlannedTask(preparedTask.plannedTask, preparedTask.index)} without changing the approved scope.`,
            ];
      const shouldUpdateTargetAgent =
        preparedTask.existingTask.target.agentId !== preparedTask.targetAgentId;
      const plannedTaskInputs = this.asRecord(preparedTask.plannedTask.inputs);
      const existingTaskInputs = this.asRecord(
        preparedTask.existingTask.inputs,
      );
      const nextExecutionPrompt = this.readOptionalString(plannedTaskInputs, [
        "prompt",
      ]);
      const existingExecutionPrompt = this.readOptionalString(
        existingTaskInputs,
        ["prompt"],
      );
      const nextExecutionTestingCriteria = this.normalizeStringArray(
        plannedTaskInputs.testingCriteria,
      );
      const existingExecutionTestingCriteria = this.normalizeStringArray(
        existingTaskInputs.testingCriteria,
      );
      const existingExecutionAcceptanceCriteria = this.normalizeStringArray(
        existingTaskInputs.acceptanceCriteria,
      );
      const shouldUpdateInputs =
        preparedTask.variant === "enrichment"
          ? !this.areValuesEquivalent(
              preparedTask.existingTask.inputs,
              preparedTask.inputs,
            )
          : false;
      const shouldUpdateConcretePrompt =
        preparedTask.variant === "execution" &&
        nextExecutionPrompt !== existingExecutionPrompt;
      const shouldUpdateConcreteTestingCriteria =
        preparedTask.variant === "execution" &&
        !this.areStringArraysEquivalent(
          existingExecutionTestingCriteria,
          nextExecutionTestingCriteria,
        );
      const shouldUpdateConcreteInputAcceptanceCriteria =
        preparedTask.variant === "execution" &&
        !this.areStringArraysEquivalent(
          existingExecutionAcceptanceCriteria,
          concreteAcceptanceCriteria,
        );
      const shouldUpdateConstraints = !this.areValuesEquivalent(
        preparedTask.existingTask.constraints as Record<string, unknown>,
        preparedTask.constraints as Record<string, unknown>,
      );
      const shouldUpdateRequiredArtifacts = !this.areStringArraysEquivalent(
        preparedTask.existingTask.requiredArtifacts,
        concreteRequiredArtifacts,
      );
      const shouldUpdateAcceptanceCriteria = !this.areStringArraysEquivalent(
        preparedTask.existingTask.acceptanceCriteria,
        concreteAcceptanceCriteria,
      );

      if (!canSyncExistingTask) {
        continue;
      }

      if (
        !shouldUpdateSequence &&
        !shouldResetQueued &&
        !shouldUpdateTargetAgent &&
        !shouldUpdateInputs &&
        !shouldUpdateConcretePrompt &&
        !shouldUpdateConcreteTestingCriteria &&
        !shouldUpdateConcreteInputAcceptanceCriteria &&
        !shouldUpdateConstraints &&
        !shouldUpdateRequiredArtifacts &&
        !shouldUpdateAcceptanceCriteria
      ) {
        continue;
      }

      updates.push({
        kind: "update",
        taskId: preparedTask.existingTask._id,
        referenceKeys,
        dependencyTaskIds: [],
        dependencyRefs,
        patch: {
          ...(shouldUpdateTargetAgent
            ? {
                target: {
                  agentId: preparedTask.targetAgentId,
                },
              }
            : {}),
          ...(shouldUpdateSequence ? { sequence: preparedTask.sequence } : {}),
          ...(shouldUpdateInputs ? { inputs: preparedTask.inputs } : {}),
          ...(shouldUpdateConcretePrompt &&
          typeof nextExecutionPrompt === "string"
            ? { prompt: nextExecutionPrompt }
            : {}),
          ...(shouldUpdateConcreteTestingCriteria
            ? { testingCriteria: nextExecutionTestingCriteria }
            : {}),
          ...(shouldUpdateConcreteInputAcceptanceCriteria
            ? { inputAcceptanceCriteria: concreteAcceptanceCriteria }
            : {}),
          ...(shouldUpdateConstraints
            ? { constraints: preparedTask.constraints }
            : {}),
          ...(shouldUpdateRequiredArtifacts
            ? { requiredArtifacts: concreteRequiredArtifacts }
            : {}),
          ...(shouldUpdateAcceptanceCriteria
            ? { acceptanceCriteria: concreteAcceptanceCriteria }
            : {}),
          ...(shouldResetQueued ? { status: "queued" } : {}),
        },
      });
    }

    const executionTasks = input.preparedTasks.filter(
      (preparedTask) => preparedTask.variant === "execution",
    );
    const reviewIdempotencyKey = `${input.plannerTask.jobId}:milestone:${input.plannerTask.milestoneId}:review_milestone`;
    const reviewReferenceKeys = [
      `review:${reviewIdempotencyKey}`,
      reviewIdempotencyKey,
    ];
    const reviewDependencyRefs = executionTasks.map(
      (preparedTask) => preparedTask.referenceKeys[0]!,
    );
    const reviewSequence = this.getMilestoneReviewTaskSequenceForPreparedTasks(
      input.plannerTask.sequence,
      executionTasks,
    );
    const reviewInputs = this.buildMilestoneReviewTaskInputsForPreparedTasks({
      plannerTask: input.plannerTask,
      milestone: input.milestone,
      phaseId: input.phaseId,
      phaseName: input.phaseName,
      ...(input.phaseGoal ? { phaseGoal: input.phaseGoal } : {}),
      concreteTasks: executionTasks,
    });
    const existingReviewTask =
      input.existingByIdempotencyKey.get(reviewIdempotencyKey);

    if (!existingReviewTask) {
      creates.push({
        kind: "create",
        referenceKeys: reviewReferenceKeys,
        dependencyTaskIds: [input.plannerTask._id],
        dependencyRefs: reviewDependencyRefs,
        task: {
          jobId: input.plannerTask.jobId,
          projectId: input.plannerTask.projectId,
          milestoneId: input.plannerTask.milestoneId,
          parentTaskId: input.plannerTask._id,
          dependencies: [],
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
        },
      });
    } else {
      const shouldRequeueFailedReviewTask =
        existingReviewTask.status === "failed" ||
        existingReviewTask.status === "canceled";
      const canSyncReviewTask =
        existingReviewTask.status === "queued" ||
        existingReviewTask.status === "qa" ||
        shouldRequeueFailedReviewTask;

      if (canSyncReviewTask) {
        updates.push({
          kind: "update",
          taskId: existingReviewTask._id,
          referenceKeys: reviewReferenceKeys,
          dependencyTaskIds: [input.plannerTask._id],
          dependencyRefs: reviewDependencyRefs,
          patch: {
            target: {
              agentId: this.projectOwnerAgentId,
            },
            sequence: reviewSequence,
            inputs: reviewInputs,
            acceptanceCriteria: [...input.milestone.acceptanceCriteria],
            ...(shouldRequeueFailedReviewTask ? { status: "queued" } : {}),
          },
        });
      } else {
        throw createServiceError({
          message: `Existing milestone review task ${existingReviewTask._id} is in non-syncable status "${existingReviewTask.status}".`,
          code: "REVIEW_TASK_STATE_INVALID_FOR_SYNC",
          statusCode: 409,
          retryable: false,
          details: {
            plannerTaskId: input.plannerTask._id,
            milestoneId: input.plannerTask.milestoneId,
            reviewTaskId: existingReviewTask._id,
            reviewTaskStatus: existingReviewTask.status,
            reviewTaskIntent: existingReviewTask.intent,
          },
        });
      }
    }

    return {
      plannerTaskId: input.plannerTask._id,
      jobId: input.plannerTask.jobId,
      projectId: input.plannerTask.projectId,
      milestoneId: input.plannerTask.milestoneId,
      creates,
      updates,
    };
  }

  private shouldResetConcreteTaskToQueued(task: TaskRecord): boolean {
    return task.status === "failed" || task.status === "canceled";
  }

  private canSyncConcreteTaskDefinition(task: TaskRecord): boolean {
    return (
      task.status === "queued" ||
      task.status === "qa" ||
      task.status === "failed" ||
      task.status === "canceled"
    );
  }

  private areValuesEquivalent(left: unknown, right: unknown): boolean {
    return JSON.stringify(left ?? null) === JSON.stringify(right ?? null);
  }

  private areStringArraysEquivalent(left: string[], right: string[]): boolean {
    return this.areValuesEquivalent(left, right);
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
    const isMilestoneReviewTask = String(task.intent) === "review_milestone";

    if (isMilestoneReviewTask) {
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

    const systemTaskType = this.readOptionalString(baseInputs, [
      "systemTaskType",
    ]);
    const isQaDispatch =
      task.status === "qa" || task.target.agentId === this.qaAgentId;
    const isExecutionDispatch = systemTaskType === "execution" || isQaDispatch;

    if (!isExecutionDispatch) {
      return baseInputs;
    }

    return this.buildConcreteTaskDispatchInputs({
      task,
      baseInputs,
      isQaDispatch,
    });
  }

  private async buildConcreteTaskDispatchInputs(input: {
    task: TaskRecord;
    baseInputs: Record<string, unknown>;
    isQaDispatch: boolean;
  }): Promise<Record<string, unknown>> {
    const milestoneTasks = await this.tasksService.listTasks({
      jobId: input.task.jobId,
      milestoneId: input.task.milestoneId,
      limit: 500,
    });
    const dependencyTaskIds = new Set(input.task.dependencies);
    const dependencyTasks = milestoneTasks.filter((candidateTask) =>
      dependencyTaskIds.has(candidateTask._id),
    );
    const enrichmentTaskIdempotencyKey = this.readOptionalString(
      input.baseInputs,
      ["enrichmentTaskIdempotencyKey", "enrichmentIdempotencyKey"],
    );
    const enrichmentTask = this.resolveConcreteTaskEnrichmentTask({
      dependencyTasks,
      ...(enrichmentTaskIdempotencyKey ? { enrichmentTaskIdempotencyKey } : {}),
    });
    const dependencyContext = this.buildConcreteTaskDependencyContext({
      task: input.task,
      dependencyTasks,
      ...(enrichmentTask ? { enrichmentTask } : {}),
      includeCurrentTask: input.isQaDispatch,
    });
    const enrichmentContext =
      this.extractConcreteTaskEnrichmentContext(enrichmentTask);

    return {
      ...this.sanitizeConcreteTaskInputsForDispatch(
        input.baseInputs,
        input.isQaDispatch,
      ),
      ...(dependencyContext.length > 0
        ? { dependencyTaskContext: dependencyContext }
        : {}),
      ...(enrichmentContext ? { enrichment: enrichmentContext } : {}),
    };
  }

  private sanitizeConcreteTaskInputsForDispatch(
    baseInputs: Record<string, unknown>,
    isQaDispatch: boolean,
  ): Record<string, unknown> {
    const sanitizedInputs = {
      ...baseInputs,
    };

    delete sanitizedInputs.taskPlan;
    delete sanitizedInputs.milestoneTaskGraph;
    delete sanitizedInputs.dependencyTaskContext;
    delete sanitizedInputs.enrichment;
    delete sanitizedInputs.enrichmentTask;

    if (isQaDispatch) {
      delete sanitizedInputs.systemTaskType;
    }

    return sanitizedInputs;
  }

  private resolveConcreteTaskEnrichmentTask(input: {
    dependencyTasks: TaskRecord[];
    enrichmentTaskIdempotencyKey?: string;
  }): TaskRecord | undefined {
    if (input.enrichmentTaskIdempotencyKey) {
      const exactMatch = input.dependencyTasks.find(
        (candidateTask) =>
          candidateTask.idempotencyKey === input.enrichmentTaskIdempotencyKey,
      );

      if (exactMatch) {
        return exactMatch;
      }
    }

    return input.dependencyTasks.find((candidateTask) =>
      this.isEnrichmentTask(candidateTask),
    );
  }

  private buildConcreteTaskDependencyContext(input: {
    task: TaskRecord;
    dependencyTasks: TaskRecord[];
    enrichmentTask?: TaskRecord;
    includeCurrentTask: boolean;
  }): Array<Record<string, unknown>> {
    const contextTasks: Array<Record<string, unknown>> = [];

    if (input.includeCurrentTask) {
      contextTasks.push(
        this.buildMilestoneTaskSnapshot(input.task, {
          includeOutputs: true,
        }),
      );
    }

    const dependencySnapshots = input.dependencyTasks
      .filter(
        (candidateTask) =>
          candidateTask._id !== input.enrichmentTask?._id &&
          !this.isEnrichmentTask(candidateTask),
      )
      .sort((left, right) => {
        if (left.sequence !== right.sequence) {
          return left.sequence - right.sequence;
        }

        return left.createdAt.getTime() - right.createdAt.getTime();
      })
      .map((candidateTask) =>
        this.buildMilestoneTaskSnapshot(candidateTask, {
          includeOutputs: true,
        }),
      );

    return [...contextTasks, ...dependencySnapshots];
  }

  private extractConcreteTaskEnrichmentContext(
    enrichmentTask?: TaskRecord,
  ): Record<string, unknown> | undefined {
    if (!enrichmentTask || !enrichmentTask.outputs) {
      return undefined;
    }

    const outputRecord = this.asRecord(enrichmentTask.outputs);
    const nestedEnrichmentRecord = this.asRecord(outputRecord.enrichment);

    if (Object.keys(nestedEnrichmentRecord).length > 0) {
      return nestedEnrichmentRecord;
    }

    return Object.keys(outputRecord).length > 0 ? outputRecord : undefined;
  }

  private isEnrichmentTask(task: TaskRecord): boolean {
    if (String(task.intent) === "enrich_task") {
      return true;
    }

    const taskInputs = this.asRecord(task.inputs);
    return (
      this.readOptionalString(taskInputs, ["systemTaskType"]) === "enrichment"
    );
  }

  private buildMilestoneReviewTaskInputsForPreparedTasks(input: {
    plannerTask: TaskRecord;
    milestone: MilestoneRecord;
    phaseId: string;
    phaseName: string;
    phaseGoal?: string;
    concreteTasks: PreparedConcreteTask[];
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
        taskId: task.existingTask?._id ?? task.idempotencyKey,
        intent: String(task.createIntent),
        targetAgentId: task.targetAgentId,
        acceptanceCriteria: [...task.plannedTask.acceptanceCriteria],
      })),
    };
  }

  private getMilestoneReviewTaskSequenceForPreparedTasks(
    plannerSequence: number,
    concreteTasks: Array<{ sequence: number }>,
  ): number {
    let maxSequence = plannerSequence;

    for (const concreteTask of concreteTasks) {
      if (concreteTask.sequence > maxSequence) {
        maxSequence = concreteTask.sequence;
      }
    }

    return maxSequence + 10;
  }

  private shouldIncludeTaskInMilestoneReviewEvidence(intent: string): boolean {
    return (
      intent !== "plan_project_phases" &&
      intent !== "plan_phase_tasks" &&
      intent !== "plan_next_tasks" &&
      intent !== "enrich_task" &&
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
        retryable: false,
        details: {
          jobId: input.task.jobId,
          taskId: input.task._id,
          outputs: input.finalResult.outputs,
        },
      });
    }

    if (outcome.decision === "pass") {
      const completedMilestone = await this.milestonesService.updateMilestone(
        milestone._id,
        { status: "completed" },
      );

      input.dispatchLogger.info(
        {
          decision: outcome.decision,
          milestoneId: completedMilestone._id,
          milestoneTitle: completedMilestone.title,
          milestoneStatus: completedMilestone.status,
          metAcceptanceCriteria: outcome.metAcceptanceCriteria,
        },
        "Milestone review passed with no patch milestone required.",
      );

      await this.completeJobAndProjectIfLastNonCompletedMilestoneReviewPassed({
        job: input.job,
        milestone: completedMilestone,
        dispatchLogger: input.dispatchLogger,
      });
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

  private async completeJobAndProjectIfLastNonCompletedMilestoneReviewPassed(input: {
    job: JobRecord;
    milestone: MilestoneRecord;
    dispatchLogger: typeof logger;
  }): Promise<void> {
    const milestones = await this.milestonesService.listMilestones({
      projectId: input.job.projectId,
      limit: 200,
    });

    const remainingNonCompletedMilestones = milestones.filter((milestone) => {
      if (milestone._id === input.milestone._id) {
        return false;
      }

      return (
        milestone.status !== "completed" && milestone.status !== "cancelled"
      );
    });

    if (remainingNonCompletedMilestones.length > 0) {
      input.dispatchLogger.debug(
        {
          jobId: input.job._id,
          projectId: input.job.projectId,
          milestoneId: input.milestone._id,
          remainingNonCompletedMilestoneCount:
            remainingNonCompletedMilestones.length,
          remainingNonCompletedMilestones: remainingNonCompletedMilestones.map(
            (milestone) => ({
              milestoneId: milestone._id,
              title: milestone.title,
              order: milestone.order,
              status: milestone.status,
            }),
          ),
        },
        "Passing milestone review did not complete the job because non-completed project milestones still exist.",
      );
      return;
    }

    const completedState = "COMPLETED" as Parameters<
      JobsServicePort["advanceState"]
    >[1];
    const updatedJob = await this.jobsService.advanceState(
      input.job._id,
      completedState,
    );

    if (!this.projectsService) {
      input.dispatchLogger.warn(
        {
          jobId: updatedJob._id,
          projectId: updatedJob.projectId,
          milestoneId: input.milestone._id,
        },
        "Job marked completed after the last non-completed milestone review passed, but no projectsService was configured to mark the project ready for review.",
      );
      return;
    }

    try {
      const readyForReviewStatus = "ready_for_review" as Parameters<
        ProjectsServicePort["updateProject"]
      >[1]["status"];
      const updatedProject = await this.projectsService.updateProject(
        input.job.projectId,
        { status: readyForReviewStatus },
      );

      input.dispatchLogger.info(
        {
          jobId: updatedJob._id,
          jobState: updatedJob.state,
          projectId: updatedProject._id,
          projectStatus: updatedProject.status,
          milestoneId: input.milestone._id,
          milestoneTitle: input.milestone.title,
        },
        "Last non-completed milestone review passed; marked the job completed and the project ready for review.",
      );
    } catch (error: unknown) {
      input.dispatchLogger.warn(
        {
          jobId: updatedJob._id,
          projectId: updatedJob.projectId,
          milestoneId: input.milestone._id,
          error,
        },
        "Job was marked completed after the last non-completed milestone review passed, but the project could not be marked ready for review.",
      );
    }
  }

  private async applyEnrichmentToExecutionTask(input: {
    job: JobRecord;
    task: TaskRecord;
    finalResult: OpenClawTaskStatusResponse;
    dispatchLogger: typeof logger;
  }): Promise<void> {
    const enrichmentInputs = this.asRecord(input.task.inputs);
    const executionTaskIdempotencyKey = this.readOptionalString(
      enrichmentInputs,
      ["executionTaskIdempotencyKey"],
    );

    if (!executionTaskIdempotencyKey) {
      throw createServiceError({
        message:
          "enrich_task succeeded but inputs.executionTaskIdempotencyKey is missing.",
        code: "ENRICHMENT_EXECUTION_TASK_KEY_MISSING",
        statusCode: 500,
        retryable: false,
        details: {
          taskId: input.task._id,
          jobId: input.task.jobId,
          inputs: input.task.inputs,
        },
      });
    }

    const executionTask = await this.tasksService.getTaskByIdempotencyKey(
      executionTaskIdempotencyKey,
    );

    if (!executionTask) {
      throw createServiceError({
        message:
          "enrich_task succeeded but the sibling execution task could not be found by idempotency key.",
        code: "ENRICHMENT_EXECUTION_TASK_NOT_FOUND",
        statusCode: 500,
        retryable: true,
        details: {
          taskId: input.task._id,
          jobId: input.task.jobId,
          executionTaskIdempotencyKey,
        },
      });
    }

    const enrichmentUpdates = this.extractExecutionTaskEnrichmentUpdates(
      input.finalResult.outputs,
    );

    if (!enrichmentUpdates) {
      throw createServiceError({
        message:
          "enrich_task succeeded but returned no usable enriched prompt or criteria.",
        code: "ENRICHMENT_OUTPUT_MISSING",
        statusCode: 500,
        retryable: false,
        details: {
          taskId: input.task._id,
          jobId: input.task.jobId,
          outputs: input.finalResult.outputs,
        },
      });
    }

    await this.tasksService.updateConcreteTaskExecution(
      executionTask._id,
      enrichmentUpdates,
    );

    input.dispatchLogger.info(
      {
        enrichmentTaskId: input.task._id,
        executionTaskId: executionTask._id,
        executionTaskIdempotencyKey,
        updatedPrompt: typeof enrichmentUpdates.prompt === "string",
        updatedTestingCriteria:
          Array.isArray(enrichmentUpdates.testingCriteria) &&
          enrichmentUpdates.testingCriteria.length > 0,
        updatedAcceptanceCriteria:
          Array.isArray(enrichmentUpdates.acceptanceCriteria) &&
          enrichmentUpdates.acceptanceCriteria.length > 0,
        updatedRequiredArtifacts:
          Array.isArray(enrichmentUpdates.requiredArtifacts) &&
          enrichmentUpdates.requiredArtifacts.length > 0,
      },
      "Applied enrichment outputs to the sibling execution task record.",
    );
  }

  private extractExecutionTaskEnrichmentUpdates(
    outputs: unknown,
  ): Parameters<TasksServicePort["updateConcreteTaskExecution"]>[1] | null {
    const direct = this.asRecord(outputs);
    const nestedOutputs = this.asRecord(direct.outputs);
    const candidates = [
      this.asRecord(direct.enrichment),
      this.asRecord(nestedOutputs.enrichment),
      direct,
      nestedOutputs,
    ];

    for (const candidate of candidates) {
      const prompt = this.readOptionalString(candidate, ["prompt"]);
      const testingCriteria = this.normalizeStringArray(
        candidate.testingCriteria,
      );
      const acceptanceCriteria = this.normalizeStringArray(
        candidate.acceptanceCriteria,
      );
      const requiredArtifacts = this.normalizeStringArray(
        candidate.requiredArtifacts,
      );

      const hasUsableFields =
        typeof prompt === "string" ||
        testingCriteria.length > 0 ||
        acceptanceCriteria.length > 0 ||
        requiredArtifacts.length > 0;

      if (!hasUsableFields) {
        continue;
      }

      return {
        ...(typeof prompt === "string" ? { prompt } : {}),
        ...(testingCriteria.length > 0 ? { testingCriteria } : {}),
        ...(acceptanceCriteria.length > 0 ? { acceptanceCriteria } : {}),
        ...(requiredArtifacts.length > 0 ? { requiredArtifacts } : {}),
      };
    }

    return null;
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
    plannedTaskMetadata: Array<{
      index: number;
      plannedTask: PlannedTaskDefinition;
      executionIdempotencyKey: string;
      enrichmentIdempotencyKey: string;
      executionReferenceKey: string;
      enrichmentReferenceKey: string;
      rawReferenceKeys: string[];
    }>;
    rawReferenceOwnerByKey: Map<
      string,
      {
        index: number;
        plannedTask: PlannedTaskDefinition;
        executionIdempotencyKey: string;
        enrichmentIdempotencyKey: string;
        executionReferenceKey: string;
        enrichmentReferenceKey: string;
      }
    >;
    existingByIdempotencyKey: Map<string, TaskRecord>;
    phaseId: string;
    phaseName: string;
    phaseGoal?: string;
  }): PreparedConcreteTask[] {
    const preparedTasks: PreparedConcreteTask[] = [];
    const taskPlan = this.buildNormalizedTaskPlan(input.plannedTasks);

    for (const metadata of input.plannedTaskMetadata) {
      const targetAgentId = this.resolveTargetAgentId(
        metadata.plannedTask.intent,
        metadata.plannedTask.targetAgentId,
      );
      const baseExecutionInputs = this.buildConcreteTaskInputs({
        parentTask: input.plannerTask,
        plannedTask: metadata.plannedTask,
        phaseId: input.phaseId,
        phaseName: input.phaseName,
        ...(input.phaseGoal ? { phaseGoal: input.phaseGoal } : {}),
      });
      const enrichmentTask: PreparedConcreteTask = {
        index: metadata.index,
        variant: "enrichment",
        plannedTask: metadata.plannedTask,
        idempotencyKey: metadata.enrichmentIdempotencyKey,
        sequence: this.getEnrichmentTaskSequence(
          input.plannerTask.sequence,
          metadata.index,
        ),
        targetAgentId: this.projectManagerAgentId,
        createIntent: this.asCreateTaskIntent("enrich_task"),
        inputs: this.buildEnrichmentTaskInputs({
          executionInputs: baseExecutionInputs,
          plannedTask: metadata.plannedTask,
          taskPlan,
          executionIdempotencyKey: metadata.executionIdempotencyKey,
          enrichmentIdempotencyKey: metadata.enrichmentIdempotencyKey,
          phaseId: input.phaseId,
          phaseName: input.phaseName,
          ...(input.phaseGoal ? { phaseGoal: input.phaseGoal } : {}),
        }),
        constraints: this.buildTaskConstraints(input.plannerTask.constraints),
        referenceKeys: [
          metadata.enrichmentReferenceKey,
          metadata.enrichmentIdempotencyKey,
        ],
        dependencyRefs: metadata.plannedTask.dependsOn.map((dependencyRef) => {
          const owner = input.rawReferenceOwnerByKey.get(dependencyRef)!;
          return owner.enrichmentReferenceKey;
        }),
        ...(input.existingByIdempotencyKey.get(
          metadata.enrichmentIdempotencyKey,
        )
          ? {
              existingTask: input.existingByIdempotencyKey.get(
                metadata.enrichmentIdempotencyKey,
              )!,
            }
          : {}),
      };

      const executionTask: PreparedConcreteTask = {
        index: metadata.index,
        variant: "execution",
        plannedTask: metadata.plannedTask,
        idempotencyKey: metadata.executionIdempotencyKey,
        sequence: this.getExecutionTaskSequence(
          input.plannerTask.sequence,
          metadata.index,
        ),
        targetAgentId,
        createIntent: this.asCreateTaskIntent(metadata.plannedTask.intent),
        inputs: this.buildExecutionTaskInputs({
          executionInputs: baseExecutionInputs,
          plannedTask: metadata.plannedTask,
          taskPlan,
          executionIdempotencyKey: metadata.executionIdempotencyKey,
          enrichmentIdempotencyKey: metadata.enrichmentIdempotencyKey,
          phaseId: input.phaseId,
          phaseName: input.phaseName,
          ...(input.phaseGoal ? { phaseGoal: input.phaseGoal } : {}),
        }),
        constraints: this.buildTaskConstraints(
          input.plannerTask.constraints,
          metadata.plannedTask.constraints,
        ),
        referenceKeys: [
          metadata.executionReferenceKey,
          metadata.executionIdempotencyKey,
        ],
        dependencyRefs: [
          metadata.enrichmentReferenceKey,
          ...metadata.plannedTask.dependsOn.map((dependencyRef) => {
            const owner = input.rawReferenceOwnerByKey.get(dependencyRef)!;
            return owner.executionReferenceKey;
          }),
        ],
        ...(input.existingByIdempotencyKey.get(metadata.executionIdempotencyKey)
          ? {
              existingTask: input.existingByIdempotencyKey.get(
                metadata.executionIdempotencyKey,
              )!,
            }
          : {}),
      };

      preparedTasks.push(enrichmentTask, executionTask);
    }

    return preparedTasks;
  }

  private validatePreparedConcreteMilestoneTasks(input: {
    plannerTask: TaskRecord;
    preparedTasks: PreparedConcreteTask[];
  }): PlanningValidationIssue[] {
    const issues: PlanningValidationIssue[] = [];

    for (const preparedTask of input.preparedTasks) {
      const existingTaskIssue = this.verifyExistingConcreteTaskRecord({
        plannerTask: input.plannerTask,
        preparedTask,
      });

      if (existingTaskIssue) {
        issues.push(existingTaskIssue);
      }
    }

    issues.push(
      ...this.validateExpandedPreparedTaskGraph({
        plannerTask: input.plannerTask,
        preparedTasks: input.preparedTasks,
      }),
    );

    return issues;
  }

  private verifyExistingConcreteTaskRecord(input: {
    plannerTask: TaskRecord;
    preparedTask: PreparedConcreteTask;
  }): PlanningValidationIssue | null {
    const existingTask = input.preparedTask.existingTask;

    if (!existingTask) {
      return null;
    }

    if (
      existingTask.milestoneId !== input.plannerTask.milestoneId ||
      existingTask.jobId !== input.plannerTask.jobId ||
      existingTask.parentTaskId !== input.plannerTask._id
    ) {
      return this.createPlannedTaskValidationIssue({
        code: "PLANNED_TASK_EXISTING_RECORD_MISMATCH",
        message: `${this.describePlannedTask(input.preparedTask.plannedTask, input.preparedTask.index)} reuses idempotency key "${input.preparedTask.idempotencyKey}", but the existing task belongs to a different parent or milestone. Return a corrected task list with stable unique task identifiers.`,
        stage: "expanded-task-validation",
        plannerTask: input.plannerTask,
        plannedTask: input.preparedTask.plannedTask,
        plannedTaskIndex: input.preparedTask.index,
        taskVariant: input.preparedTask.variant,
        details: {
          idempotencyKey: input.preparedTask.idempotencyKey,
          existingTaskId: existingTask._id,
          existingMilestoneId: existingTask.milestoneId,
          existingParentTaskId: existingTask.parentTaskId,
        },
      });
    }

    if (
      String(existingTask.intent) !== String(input.preparedTask.createIntent) ||
      existingTask.target.agentId !== input.preparedTask.targetAgentId
    ) {
      return this.createPlannedTaskValidationIssue({
        code: "PLANNED_TASK_EXISTING_RECORD_SHAPE_MISMATCH",
        message: `${this.describePlannedTask(input.preparedTask.plannedTask, input.preparedTask.index)} reuses idempotency key "${input.preparedTask.idempotencyKey}", but the existing ${input.preparedTask.variant} task has a different intent or target agent. Return a corrected task list with unique task ids or consistent task shapes.`,
        stage: "expanded-task-validation",
        plannerTask: input.plannerTask,
        plannedTask: input.preparedTask.plannedTask,
        plannedTaskIndex: input.preparedTask.index,
        taskVariant: input.preparedTask.variant,
        details: {
          idempotencyKey: input.preparedTask.idempotencyKey,
          variant: input.preparedTask.variant,
          existingTaskId: existingTask._id,
          existingIntent: String(existingTask.intent),
          existingTargetAgentId: existingTask.target.agentId,
          expectedIntent: String(input.preparedTask.createIntent),
          expectedTargetAgentId: input.preparedTask.targetAgentId,
        },
      });
    }

    return null;
  }

  private describePlannedTask(
    plannedTask: PlannedTaskDefinition,
    index: number,
  ): string {
    const label = plannedTask.localId ?? `task-${index + 1}`;
    return `planned task ${label} (intent=${plannedTask.intent})`;
  }

  private createPlannedTaskValidationIssue(input: {
    code: string;
    message: string;
    stage: PlanningValidationIssue["stage"];
    plannerTask: TaskRecord;
    plannedTask: PlannedTaskDefinition;
    plannedTaskIndex: number;
    taskVariant?: PlanningValidationIssue["taskVariant"];
    field?: string;
    details?: Record<string, unknown>;
  }): PlanningValidationIssue {
    return {
      code: input.code,
      message: input.message,
      stage: input.stage,
      plannedTaskIndex: input.plannedTaskIndex,
      ...(input.plannedTask.localId
        ? { taskLocalId: input.plannedTask.localId }
        : {}),
      taskIntent: input.plannedTask.intent,
      ...(input.taskVariant ? { taskVariant: input.taskVariant } : {}),
      ...(input.field ? { field: input.field } : {}),
      details: {
        jobId: input.plannerTask.jobId,
        milestoneId: input.plannerTask.milestoneId,
        plannerTaskId: input.plannerTask._id,
        plannedTaskIndex: input.plannedTaskIndex,
        plannedTaskLocalId: input.plannedTask.localId,
        plannedTaskIntent: input.plannedTask.intent,
        ...(input.details ?? {}),
      },
    };
  }

  private createAggregatePlannedTaskValidationError(input: {
    plannerTask: TaskRecord;
    issues: PlanningValidationIssue[];
  }): ServiceError {
    const firstIssue = input.issues[0];
    const extraIssueCount =
      input.issues.length > 1 ? input.issues.length - 1 : 0;
    const issueSummary =
      typeof firstIssue?.message === "string" &&
      firstIssue.message.trim().length > 0
        ? firstIssue.message.trim()
        : undefined;

    return createServiceError({
      code: "PLANNED_TASK_BATCH_INVALID",
      message: issueSummary
        ? `Phase task plan failed validation: ${issueSummary}${extraIssueCount > 0 ? ` (+${extraIssueCount} more issue${extraIssueCount === 1 ? "" : "s"})` : ""}`
        : `Phase task plan failed validation with ${input.issues.length} issue${input.issues.length === 1 ? "" : "s"}.`,
      statusCode: 422,
      retryable: true,
      details: {
        jobId: input.plannerTask.jobId,
        milestoneId: input.plannerTask.milestoneId,
        plannerTaskId: input.plannerTask._id,
        issues: input.issues,
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

  private getEnrichmentTaskSequence(
    phasePlannerSequence: number,
    taskIndex: number,
  ): number {
    return phasePlannerSequence + (taskIndex + 1) * 20;
  }

  private getExecutionTaskSequence(
    phasePlannerSequence: number,
    taskIndex: number,
  ): number {
    return this.getEnrichmentTaskSequence(phasePlannerSequence, taskIndex) + 10;
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

  private buildNormalizedTaskPlan(
    plannedTasks: PlannedTaskDefinition[],
  ): Array<{
    localId: string;
    intent: string;
    targetAgentId: string;
    dependsOn: string[];
    acceptanceCriteria: string[];
    testingCriteria: string[];
    prompt?: string;
  }> {
    return plannedTasks.map((plannedTask, index) => {
      const plannedInputs = this.asRecord(plannedTask.inputs);
      const prompt = this.readOptionalString(plannedInputs, ["prompt"]);
      const testingCriteria = this.normalizeStringArray(
        plannedInputs.testingCriteria,
      );

      return {
        localId: plannedTask.localId ?? `task-${index + 1}`,
        intent: plannedTask.intent,
        targetAgentId: this.resolveTargetAgentId(
          plannedTask.intent,
          plannedTask.targetAgentId,
        ),
        dependsOn: [...plannedTask.dependsOn],
        acceptanceCriteria: [...plannedTask.acceptanceCriteria],
        testingCriteria,
        ...(prompt ? { prompt } : {}),
      };
    });
  }

  private buildEnrichmentTaskInputs(input: {
    executionInputs: Record<string, unknown>;
    plannedTask: PlannedTaskDefinition;
    taskPlan: Array<{
      localId: string;
      intent: string;
      targetAgentId: string;
      dependsOn: string[];
      acceptanceCriteria: string[];
      testingCriteria: string[];
      prompt?: string;
    }>;
    executionIdempotencyKey: string;
    enrichmentIdempotencyKey: string;
    phaseId: string;
    phaseName: string;
    phaseGoal?: string;
  }): Record<string, unknown> {
    return {
      ...input.executionInputs,
      systemTaskType: "enrichment",
      sourceTask: {
        ...(input.plannedTask.localId
          ? { localId: input.plannedTask.localId }
          : {}),
        intent: input.plannedTask.intent,
        targetAgentId: this.resolveTargetAgentId(
          input.plannedTask.intent,
          input.plannedTask.targetAgentId,
        ),
        inputs: input.plannedTask.inputs,
        constraints: input.plannedTask.constraints ?? {},
        requiredArtifacts: [...input.plannedTask.requiredArtifacts],
        acceptanceCriteria: [...input.plannedTask.acceptanceCriteria],
        dependsOn: [...input.plannedTask.dependsOn],
      },
      taskPlan: input.taskPlan,
      executionTaskIdempotencyKey: input.executionIdempotencyKey,
      enrichmentTaskIdempotencyKey: input.enrichmentIdempotencyKey,
      phaseId: input.phaseId,
      phaseName: input.phaseName,
      ...(input.phaseGoal ? { phaseGoal: input.phaseGoal } : {}),
    };
  }

  private buildExecutionTaskInputs(input: {
    executionInputs: Record<string, unknown>;
    plannedTask: PlannedTaskDefinition;
    taskPlan: Array<{
      localId: string;
      intent: string;
      targetAgentId: string;
      dependsOn: string[];
      acceptanceCriteria: string[];
      testingCriteria: string[];
      prompt?: string;
    }>;
    executionIdempotencyKey: string;
    enrichmentIdempotencyKey: string;
    phaseId: string;
    phaseName: string;
    phaseGoal?: string;
  }): Record<string, unknown> {
    return {
      ...input.executionInputs,
      systemTaskType: "execution",
      ...(input.plannedTask.localId
        ? { plannedTaskLocalId: input.plannedTask.localId }
        : {}),
      plannedTaskIntent: input.plannedTask.intent,
      taskPlan: input.taskPlan,
      executionTaskIdempotencyKey: input.executionIdempotencyKey,
      enrichmentTaskIdempotencyKey: input.enrichmentIdempotencyKey,
      phaseId: input.phaseId,
      phaseName: input.phaseName,
      ...(input.phaseGoal ? { phaseGoal: input.phaseGoal } : {}),
    };
  }

  private buildPlannedTaskMetadata(input: {
    plannerTask: TaskRecord;
    plannedTasks: PlannedTaskDefinition[];
  }): {
    plannedTaskMetadata: Array<{
      index: number;
      plannedTask: PlannedTaskDefinition;
      executionIdempotencyKey: string;
      enrichmentIdempotencyKey: string;
      executionReferenceKey: string;
      enrichmentReferenceKey: string;
      rawReferenceKeys: string[];
    }>;
    rawReferenceOwnerByKey: Map<
      string,
      {
        index: number;
        plannedTask: PlannedTaskDefinition;
        executionIdempotencyKey: string;
        enrichmentIdempotencyKey: string;
        executionReferenceKey: string;
        enrichmentReferenceKey: string;
      }
    >;
    issues: PlanningValidationIssue[];
  } {
    const issues: PlanningValidationIssue[] = [];
    const seenIdempotencyKeys = new Set<string>();
    const rawReferenceOwnerByKey = new Map<
      string,
      {
        index: number;
        plannedTask: PlannedTaskDefinition;
        executionIdempotencyKey: string;
        enrichmentIdempotencyKey: string;
        executionReferenceKey: string;
        enrichmentReferenceKey: string;
      }
    >();

    const plannedTaskMetadata = input.plannedTasks.map((plannedTask, index) => {
      const executionIdempotencyKey =
        plannedTask.idempotencyKey ??
        `${input.plannerTask.jobId}:${input.plannerTask.milestoneId}:planned-task:${index + 1}:${plannedTask.intent}`;
      const enrichmentIdempotencyKey = `${executionIdempotencyKey}:enrich`;
      const executionReferenceKey = `execution:${executionIdempotencyKey}`;
      const enrichmentReferenceKey = `enrichment:${executionIdempotencyKey}`;
      const rawReferenceKeys = this.buildPlannedTaskReferenceKeys(
        plannedTask,
        executionIdempotencyKey,
      );

      if (seenIdempotencyKeys.has(executionIdempotencyKey)) {
        issues.push(
          this.createPlannedTaskValidationIssue({
            code: "PLANNED_TASK_DUPLICATE_IDEMPOTENCY_KEY",
            message: `Planned task list contains a duplicate idempotency key "${executionIdempotencyKey}" for ${this.describePlannedTask(plannedTask, index)}. Each task must be uniquely identifiable.`,
            stage: "planned-task-validation",
            plannerTask: input.plannerTask,
            plannedTask,
            plannedTaskIndex: index,
            details: {
              idempotencyKey: executionIdempotencyKey,
            },
          }),
        );
      }

      if (seenIdempotencyKeys.has(enrichmentIdempotencyKey)) {
        issues.push(
          this.createPlannedTaskValidationIssue({
            code: "PLANNED_TASK_DUPLICATE_ENRICHMENT_IDEMPOTENCY_KEY",
            message: `The synthesized enrichment idempotency key "${enrichmentIdempotencyKey}" collides with another task for ${this.describePlannedTask(plannedTask, index)}.`,
            stage: "planned-task-validation",
            plannerTask: input.plannerTask,
            plannedTask,
            plannedTaskIndex: index,
            details: {
              idempotencyKey: enrichmentIdempotencyKey,
            },
          }),
        );
      }

      seenIdempotencyKeys.add(executionIdempotencyKey);
      seenIdempotencyKeys.add(enrichmentIdempotencyKey);

      for (const referenceKey of rawReferenceKeys) {
        const existingOwner = rawReferenceOwnerByKey.get(referenceKey);
        if (existingOwner) {
          issues.push(
            this.createPlannedTaskValidationIssue({
              code: "PLANNED_TASK_DUPLICATE_REFERENCE",
              message: `Planned task reference "${referenceKey}" is used more than once. ${this.describePlannedTask(plannedTask, index)} conflicts with ${this.describePlannedTask(existingOwner.plannedTask, existingOwner.index)}.`,
              stage: "planned-task-validation",
              plannerTask: input.plannerTask,
              plannedTask,
              plannedTaskIndex: index,
              details: {
                referenceKey,
                conflictingTaskIndex: existingOwner.index,
                conflictingTaskLocalId: existingOwner.plannedTask.localId,
                conflictingTaskIntent: existingOwner.plannedTask.intent,
              },
            }),
          );
          continue;
        }

        rawReferenceOwnerByKey.set(referenceKey, {
          index,
          plannedTask,
          executionIdempotencyKey,
          enrichmentIdempotencyKey,
          executionReferenceKey,
          enrichmentReferenceKey,
        });
      }

      return {
        index,
        plannedTask,
        executionIdempotencyKey,
        enrichmentIdempotencyKey,
        executionReferenceKey,
        enrichmentReferenceKey,
        rawReferenceKeys,
      };
    });

    return {
      plannedTaskMetadata,
      rawReferenceOwnerByKey,
      issues,
    };
  }

  private validatePlannedTaskList(input: {
    plannerTask: TaskRecord;
    plannedTasks: PlannedTaskDefinition[];
  }): {
    plannedTaskMetadata: Array<{
      index: number;
      plannedTask: PlannedTaskDefinition;
      executionIdempotencyKey: string;
      enrichmentIdempotencyKey: string;
      executionReferenceKey: string;
      enrichmentReferenceKey: string;
      rawReferenceKeys: string[];
    }>;
    rawReferenceOwnerByKey: Map<
      string,
      {
        index: number;
        plannedTask: PlannedTaskDefinition;
        executionIdempotencyKey: string;
        enrichmentIdempotencyKey: string;
        executionReferenceKey: string;
        enrichmentReferenceKey: string;
      }
    >;
    issues: PlanningValidationIssue[];
  } {
    const metadata = this.buildPlannedTaskMetadata(input);
    const issues = [...metadata.issues];

    issues.push(
      ...this.validatePlannedTaskDependencyGraph({
        plannerTask: input.plannerTask,
        plannedTaskMetadata: metadata.plannedTaskMetadata,
        rawReferenceOwnerByKey: metadata.rawReferenceOwnerByKey,
      }),
    );

    return {
      plannedTaskMetadata: metadata.plannedTaskMetadata,
      rawReferenceOwnerByKey: metadata.rawReferenceOwnerByKey,
      issues,
    };
  }

  private validatePlannedTaskDependencyGraph(input: {
    plannerTask: TaskRecord;
    plannedTaskMetadata: Array<{
      index: number;
      plannedTask: PlannedTaskDefinition;
      executionIdempotencyKey: string;
      enrichmentIdempotencyKey: string;
      executionReferenceKey: string;
      enrichmentReferenceKey: string;
      rawReferenceKeys: string[];
    }>;
    rawReferenceOwnerByKey: Map<
      string,
      {
        index: number;
        plannedTask: PlannedTaskDefinition;
        executionIdempotencyKey: string;
        enrichmentIdempotencyKey: string;
        executionReferenceKey: string;
        enrichmentReferenceKey: string;
      }
    >;
  }): PlanningValidationIssue[] {
    const issues: PlanningValidationIssue[] = [];
    const dependencyGraph = new Map<string, string[]>();

    for (const metadata of input.plannedTaskMetadata) {
      const dependencyNodeIds: string[] = [];

      for (const dependencyRef of metadata.plannedTask.dependsOn) {
        const owner = input.rawReferenceOwnerByKey.get(dependencyRef);

        if (!owner) {
          issues.push(
            this.createPlannedTaskValidationIssue({
              code: "PLANNED_TASK_DEPENDENCY_UNRESOLVED",
              message: `${this.describePlannedTask(metadata.plannedTask, metadata.index)} depends on "${dependencyRef}", but no task in the plan matches that reference. Fix the dependsOn values and return the full corrected task list.`,
              stage: "planned-task-graph",
              plannerTask: input.plannerTask,
              plannedTask: metadata.plannedTask,
              plannedTaskIndex: metadata.index,
              field: "dependsOn",
              details: {
                dependencyRef,
                availableReferences: Array.from(
                  input.rawReferenceOwnerByKey.keys(),
                ),
              },
            }),
          );
          continue;
        }

        if (
          owner.executionIdempotencyKey === metadata.executionIdempotencyKey
        ) {
          issues.push(
            this.createPlannedTaskValidationIssue({
              code: "PLANNED_TASK_SELF_DEPENDENCY",
              message: `${this.describePlannedTask(metadata.plannedTask, metadata.index)} cannot depend on itself (${dependencyRef}). Remove that self-dependency and return the full corrected task list.`,
              stage: "planned-task-graph",
              plannerTask: input.plannerTask,
              plannedTask: metadata.plannedTask,
              plannedTaskIndex: metadata.index,
              field: "dependsOn",
              details: {
                dependencyRef,
              },
            }),
          );
          continue;
        }

        dependencyNodeIds.push(owner.executionIdempotencyKey);
      }

      dependencyGraph.set(
        metadata.executionIdempotencyKey,
        Array.from(new Set(dependencyNodeIds)),
      );
    }

    issues.push(
      ...this.assertAcyclicPreparedDependencyGraph({
        plannerTask: input.plannerTask,
        dependencyGraph,
        metadataByNodeId: new Map(
          input.plannedTaskMetadata.map((metadata) => [
            metadata.executionIdempotencyKey,
            {
              plannedTask: metadata.plannedTask,
              plannedTaskIndex: metadata.index,
            },
          ]),
        ),
        stage: "planned-task-graph",
        cycleErrorCode: "PLANNED_TASK_DEPENDENCY_CYCLE",
        cycleMessageFactory: (plannedTask, plannedTaskIndex, cycleNodes) =>
          `${this.describePlannedTask(plannedTask, plannedTaskIndex)} participates in a dependency cycle (${cycleNodes.join(" -> ")}). Return a full corrected task list with an acyclic dependency order.`,
      }),
    );

    return issues;
  }

  private validateExpandedPreparedTaskGraph(input: {
    plannerTask: TaskRecord;
    preparedTasks: PreparedConcreteTask[];
  }): PlanningValidationIssue[] {
    const issues: PlanningValidationIssue[] = [];
    const referenceOwnerByKey = new Map<string, PreparedConcreteTask>();
    const dependencyGraph = new Map<string, string[]>();
    const metadataByNodeId = new Map<
      string,
      {
        plannedTask: PlannedTaskDefinition;
        plannedTaskIndex: number;
      }
    >();

    for (const preparedTask of input.preparedTasks) {
      metadataByNodeId.set(preparedTask.idempotencyKey, {
        plannedTask: preparedTask.plannedTask,
        plannedTaskIndex: preparedTask.index,
      });

      for (const referenceKey of preparedTask.referenceKeys) {
        const existingOwner = referenceOwnerByKey.get(referenceKey);
        if (existingOwner) {
          issues.push(
            this.createPlannedTaskValidationIssue({
              code: "PLANNED_TASK_DUPLICATE_EXPANDED_REFERENCE",
              message: `Expanded planning graph reference "${referenceKey}" is used more than once. ${this.describePlannedTask(preparedTask.plannedTask, preparedTask.index)} (${preparedTask.variant}) conflicts with ${this.describePlannedTask(existingOwner.plannedTask, existingOwner.index)} (${existingOwner.variant}).`,
              stage: "expanded-task-validation",
              plannerTask: input.plannerTask,
              plannedTask: preparedTask.plannedTask,
              plannedTaskIndex: preparedTask.index,
              taskVariant: preparedTask.variant,
              details: {
                referenceKey,
                conflictingTaskVariant: existingOwner.variant,
              },
            }),
          );
          continue;
        }

        referenceOwnerByKey.set(referenceKey, preparedTask);
      }
    }

    for (const preparedTask of input.preparedTasks) {
      const dependencyNodeIds: string[] = [];

      for (const dependencyRef of preparedTask.dependencyRefs) {
        const owner = referenceOwnerByKey.get(dependencyRef);

        if (!owner) {
          issues.push(
            this.createPlannedTaskValidationIssue({
              code: "PLANNED_TASK_EXPANDED_DEPENDENCY_UNRESOLVED",
              message: `${this.describePlannedTask(preparedTask.plannedTask, preparedTask.index)} (${preparedTask.variant}) depends on synthesized reference "${dependencyRef}", but no expanded task matches that reference.`,
              stage: "expanded-task-validation",
              plannerTask: input.plannerTask,
              plannedTask: preparedTask.plannedTask,
              plannedTaskIndex: preparedTask.index,
              taskVariant: preparedTask.variant,
              details: {
                dependencyRef,
                variant: preparedTask.variant,
              },
            }),
          );
          continue;
        }

        if (owner.idempotencyKey === preparedTask.idempotencyKey) {
          issues.push(
            this.createPlannedTaskValidationIssue({
              code: "PLANNED_TASK_EXPANDED_SELF_DEPENDENCY",
              message: `${this.describePlannedTask(preparedTask.plannedTask, preparedTask.index)} (${preparedTask.variant}) cannot depend on itself (${dependencyRef}).`,
              stage: "expanded-task-validation",
              plannerTask: input.plannerTask,
              plannedTask: preparedTask.plannedTask,
              plannedTaskIndex: preparedTask.index,
              taskVariant: preparedTask.variant,
              details: {
                dependencyRef,
                variant: preparedTask.variant,
              },
            }),
          );
          continue;
        }

        dependencyNodeIds.push(owner.idempotencyKey);
      }

      dependencyGraph.set(
        preparedTask.idempotencyKey,
        Array.from(new Set(dependencyNodeIds)),
      );
    }

    issues.push(
      ...this.assertAcyclicPreparedDependencyGraph({
        plannerTask: input.plannerTask,
        dependencyGraph,
        metadataByNodeId,
        stage: "expanded-task-validation",
        cycleErrorCode: "PLANNED_TASK_EXPANDED_DEPENDENCY_CYCLE",
        cycleMessageFactory: (plannedTask, plannedTaskIndex, cycleNodes) =>
          `${this.describePlannedTask(plannedTask, plannedTaskIndex)} participates in an expanded dependency cycle (${cycleNodes.join(" -> ")}). Return a corrected task list so enrichment and execution dependencies remain acyclic.`,
      }),
    );

    return issues;
  }

  private assertAcyclicPreparedDependencyGraph(input: {
    plannerTask: TaskRecord;
    dependencyGraph: Map<string, string[]>;
    metadataByNodeId: Map<
      string,
      {
        plannedTask: PlannedTaskDefinition;
        plannedTaskIndex: number;
      }
    >;
    stage: PlanningValidationIssue["stage"];
    cycleErrorCode: string;
    cycleMessageFactory: (
      plannedTask: PlannedTaskDefinition,
      plannedTaskIndex: number,
      cycleNodes: string[],
    ) => string;
  }): PlanningValidationIssue[] {
    const issues: PlanningValidationIssue[] = [];
    const visiting = new Set<string>();
    const visited = new Set<string>();
    const stack: string[] = [];

    const visit = (nodeId: string): void => {
      if (visited.has(nodeId)) {
        return;
      }

      if (visiting.has(nodeId)) {
        const cycleStartIndex = stack.indexOf(nodeId);
        const cycleNodes = [...stack.slice(cycleStartIndex), nodeId];
        const metadata = input.metadataByNodeId.get(nodeId);

        if (!metadata) {
          issues.push({
            code: input.cycleErrorCode,
            message: `Detected a dependency cycle in the milestone planning graph (${cycleNodes.join(" -> ")}), but no metadata was available for node ${nodeId}.`,
            stage: input.stage,
            details: {
              plannerTaskId: input.plannerTask._id,
              milestoneId: input.plannerTask.milestoneId,
              cycleNodes,
            },
          });
          return;
        }

        issues.push(
          this.createPlannedTaskValidationIssue({
            code: input.cycleErrorCode,
            message: input.cycleMessageFactory(
              metadata.plannedTask,
              metadata.plannedTaskIndex,
              cycleNodes,
            ),
            stage: input.stage,
            plannerTask: input.plannerTask,
            plannedTask: metadata.plannedTask,
            plannedTaskIndex: metadata.plannedTaskIndex,
            details: {
              cycleNodes,
            },
          }),
        );
        return;
      }

      visiting.add(nodeId);
      stack.push(nodeId);

      for (const dependencyNodeId of input.dependencyGraph.get(nodeId) ?? []) {
        visit(dependencyNodeId);
      }

      stack.pop();
      visiting.delete(nodeId);
      visited.add(nodeId);
    };

    for (const nodeId of input.dependencyGraph.keys()) {
      visit(nodeId);
    }

    return issues;
  }

  private buildMilestoneTaskSnapshots(
    tasks: TaskRecord[],
  ): Array<Record<string, unknown>> {
    return tasks.map((task) => this.buildMilestoneTaskSnapshot(task));
  }

  private buildMilestoneTaskSnapshot(
    task: TaskRecord,
    options?: {
      includeInputs?: boolean;
      includeOutputs?: boolean;
    },
  ): Record<string, unknown> {
    const baseInputs = this.asRecord(task.inputs);
    const outputRecord = this.asRecord(task.outputs);
    const outputSummary = this.readOptionalString(outputRecord, [
      "summary",
      "resultSummary",
      "findingSummary",
    ]);

    return {
      taskId: task._id,
      intent: String(task.intent),
      targetAgentId: task.target.agentId,
      status: task.status,
      sequence: task.sequence,
      dependencies: [...task.dependencies],
      acceptanceCriteria: [...task.acceptanceCriteria],
      ...(outputSummary ? { summary: outputSummary } : {}),
      ...(this.readOptionalString(baseInputs, ["systemTaskType"])
        ? {
            systemTaskType: this.readOptionalString(baseInputs, [
              "systemTaskType",
            ]),
          }
        : {}),
      ...(this.readOptionalString(baseInputs, ["plannedTaskLocalId"])
        ? {
            plannedTaskLocalId: this.readOptionalString(baseInputs, [
              "plannedTaskLocalId",
            ]),
          }
        : {}),
      ...(options?.includeInputs ? { inputs: task.inputs } : {}),
      ...(options?.includeOutputs && task.outputs
        ? { outputs: task.outputs }
        : {}),
      ...(task.artifacts.length > 0 ? { artifacts: [...task.artifacts] } : {}),
      ...(task.errors.length > 0 ? { errors: [...task.errors] } : {}),
      ...(typeof task.lastError === "string" && task.lastError.trim().length > 0
        ? { lastError: task.lastError }
        : {}),
    };
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
      case "enrich_task":
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

  private async commitMilestonePlanningBatchSafely(input: {
    plannerTask: TaskRecord;
    batch: AtomicMilestonePlanningBatch;
  }): Promise<AtomicMilestonePlanningBatchResult> {
    const commitMilestonePlanningBatch =
      this.getCommitMilestonePlanningBatchOrThrow();

    try {
      const batchResult = await commitMilestonePlanningBatch(input.batch);
      this.assertMilestonePlanningBatchCommitResult({
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
      const originalError = this.serializeUnknownError(error);
      const detailRecord = this.extractErrorDetailsRecord(error);
      const operationSummary =
        this.describeMilestonePlanningOperation(detailRecord);
      const progressSummary = this.describeMilestonePlanningProgress(
        detailRecord,
        {
          createdCount,
        },
      );
      const causeMessage = this.readOptionalString(detailRecord, [
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
          ...(this.isRecord(detailRecord) &&
          Object.keys(detailRecord).length > 0
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
    const normalizedArtifacts = this.normalizeTaskArtifactRefs(
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
          originalError: this.serializeUnknownError(error),
        },
      });
    }
  }

  private assertMilestonePlanningBatchCommitResult(input: {
    plannerTask: TaskRecord;
    batch: AtomicMilestonePlanningBatch;
    batchResult: AtomicMilestonePlanningBatchResult;
  }): void {
    const expectedCreatedCount = input.batch.creates.length;
    const expectedUpdatedCount = input.batch.updates.length;
    const actualCreatedCount = input.batchResult.createdTaskIds.length;
    const actualUpdatedCount = input.batchResult.updatedTaskIds.length;
    const createdTasks = Array.isArray(input.batchResult.createdTasks)
      ? input.batchResult.createdTasks
      : [];
    const updatedTasks = Array.isArray(input.batchResult.updatedTasks)
      ? input.batchResult.updatedTasks
      : [];
    const reviewMutationCount =
      (input.batchResult.reviewTaskCreated ? 1 : 0) +
      (input.batchResult.reviewTaskUpdated ? 1 : 0);

    if (actualCreatedCount !== expectedCreatedCount) {
      throw createServiceError({
        message: `Milestone planning batch expected ${expectedCreatedCount} created task(s) but commit reported ${actualCreatedCount}.`,
        code: "TASK_BATCH_COMMIT_CREATED_COUNT_MISMATCH",
        statusCode: 500,
        retryable: true,
        details: {
          plannerTaskId: input.plannerTask._id,
          expectedCreatedCount,
          actualCreatedCount,
          batchResult: input.batchResult,
        },
      });
    }

    if (createdTasks.length !== actualCreatedCount) {
      throw createServiceError({
        message: `Milestone planning batch reported ${actualCreatedCount} created task id(s) but returned ${createdTasks.length} created task detail entr${createdTasks.length === 1 ? "y" : "ies"}.`,
        code: "TASK_BATCH_COMMIT_CREATED_DETAIL_COUNT_MISMATCH",
        statusCode: 500,
        retryable: true,
        details: {
          plannerTaskId: input.plannerTask._id,
          actualCreatedCount,
          actualCreatedDetailCount: createdTasks.length,
          batchResult: input.batchResult,
        },
      });
    }

    if (actualUpdatedCount !== expectedUpdatedCount) {
      throw createServiceError({
        message: `Milestone planning batch expected ${expectedUpdatedCount} updated task(s) but commit reported ${actualUpdatedCount}.`,
        code: "TASK_BATCH_COMMIT_UPDATED_COUNT_MISMATCH",
        statusCode: 500,
        retryable: true,
        details: {
          plannerTaskId: input.plannerTask._id,
          expectedUpdatedCount,
          actualUpdatedCount,
          batchResult: input.batchResult,
        },
      });
    }

    if (updatedTasks.length !== actualUpdatedCount) {
      throw createServiceError({
        message: `Milestone planning batch reported ${actualUpdatedCount} updated task id(s) but returned ${updatedTasks.length} updated task detail entr${updatedTasks.length === 1 ? "y" : "ies"}.`,
        code: "TASK_BATCH_COMMIT_UPDATED_DETAIL_COUNT_MISMATCH",
        statusCode: 500,
        retryable: true,
        details: {
          plannerTaskId: input.plannerTask._id,
          actualUpdatedCount,
          actualUpdatedDetailCount: updatedTasks.length,
          batchResult: input.batchResult,
        },
      });
    }

    const createdTaskIdSet = new Set(input.batchResult.createdTaskIds);
    for (const createdTask of createdTasks) {
      if (
        typeof createdTask.taskId !== "string" ||
        createdTask.taskId.trim().length === 0 ||
        !createdTaskIdSet.has(createdTask.taskId)
      ) {
        throw createServiceError({
          message:
            "Milestone planning batch returned an invalid created task detail entry.",
          code: "TASK_BATCH_COMMIT_CREATED_DETAIL_INVALID",
          statusCode: 500,
          retryable: true,
          details: {
            plannerTaskId: input.plannerTask._id,
            createdTask,
            batchResult: input.batchResult,
          },
        });
      }
    }

    const updatedTaskIdSet = new Set(input.batchResult.updatedTaskIds);
    for (const updatedTask of updatedTasks) {
      if (
        typeof updatedTask.taskId !== "string" ||
        updatedTask.taskId.trim().length === 0 ||
        !updatedTaskIdSet.has(updatedTask.taskId)
      ) {
        throw createServiceError({
          message:
            "Milestone planning batch returned an invalid updated task detail entry.",
          code: "TASK_BATCH_COMMIT_UPDATED_DETAIL_INVALID",
          statusCode: 500,
          retryable: true,
          details: {
            plannerTaskId: input.plannerTask._id,
            updatedTask,
            batchResult: input.batchResult,
          },
        });
      }
    }

    if (!input.batchResult.reviewTaskId || reviewMutationCount !== 1) {
      throw createServiceError({
        message:
          "Milestone planning batch must commit exactly one review task mutation and return its task id.",
        code: "TASK_BATCH_COMMIT_REVIEW_TASK_INVALID",
        statusCode: 500,
        retryable: true,
        details: {
          plannerTaskId: input.plannerTask._id,
          reviewTaskId: input.batchResult.reviewTaskId,
          reviewTaskCreated: input.batchResult.reviewTaskCreated,
          reviewTaskUpdated: input.batchResult.reviewTaskUpdated,
          batchResult: input.batchResult,
        },
      });
    }

    const reviewTaskEntry = [...createdTasks, ...updatedTasks].find(
      (task) => task.taskId === input.batchResult.reviewTaskId,
    );

    if (!reviewTaskEntry) {
      throw createServiceError({
        message:
          "Milestone planning batch returned a review task id that does not match any committed mutation entry.",
        code: "TASK_BATCH_COMMIT_REVIEW_TASK_MISSING_DETAIL",
        statusCode: 500,
        retryable: true,
        details: {
          plannerTaskId: input.plannerTask._id,
          reviewTaskId: input.batchResult.reviewTaskId,
          batchResult: input.batchResult,
        },
      });
    }

    if (reviewTaskEntry.taskIntent !== "review_milestone") {
      throw createServiceError({
        message:
          "Milestone planning batch review task detail does not point to a review_milestone task.",
        code: "TASK_BATCH_COMMIT_REVIEW_TASK_INTENT_INVALID",
        statusCode: 500,
        retryable: true,
        details: {
          plannerTaskId: input.plannerTask._id,
          reviewTaskId: input.batchResult.reviewTaskId,
          reviewTaskEntry,
          batchResult: input.batchResult,
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

  private serializeUnknownError(error: unknown): Record<string, unknown> {
    if (error instanceof Error) {
      const details = this.extractErrorDetailsRecord(error);
      return {
        message: error.message,
        ...(this.getErrorCode(error) ? { code: this.getErrorCode(error) } : {}),
        ...(typeof this.getErrorStatusCode(error) === "number"
          ? { statusCode: this.getErrorStatusCode(error) }
          : {}),
        ...(details ? { details } : {}),
      };
    }

    if (this.isRecord(error)) {
      return error;
    }

    return {
      value: error,
    };
  }

  private normalizeBatchValidationIssues(
    result: unknown,
  ): PlanningValidationIssue[] {
    const record = this.asRecord(result);
    const rawIssues = Array.isArray(record.issues) ? record.issues : [];

    return rawIssues
      .map((issue) => this.normalizeBatchValidationIssue(issue))
      .filter((issue): issue is PlanningValidationIssue => issue !== null);
  }

  private normalizeBatchValidationIssue(
    issue: unknown,
  ): PlanningValidationIssue | null {
    const record = this.asRecord(issue);
    const code = this.readOptionalString(record, ["code"]);
    const message = this.readOptionalString(record, ["message"]);
    const stageCandidate = this.readOptionalString(record, ["stage"]);
    const taskLocalId = this.readOptionalString(record, ["taskLocalId"]);
    const taskIntent = this.readOptionalString(record, ["taskIntent"]);
    const field = this.readOptionalString(record, ["field"]);
    const taskVariant = this.readOptionalString(record, ["taskVariant"]);
    const ownerLabel = this.readOptionalString(record, ["ownerLabel"]);
    const idempotencyKey = this.readOptionalString(record, ["idempotencyKey"]);
    const operationKindCandidate = this.readOptionalString(record, [
      "operationKind",
    ]);
    const plannedTaskIndex =
      typeof record.plannedTaskIndex === "number"
        ? record.plannedTaskIndex
        : undefined;
    const operationIndex =
      typeof record.operationIndex === "number"
        ? record.operationIndex
        : undefined;

    if (!code || !message) {
      return null;
    }

    const normalizedStage: PlanningValidationIssue["stage"] =
      stageCandidate === "planned-task-validation" ||
      stageCandidate === "planned-task-graph" ||
      stageCandidate === "expanded-task-validation" ||
      stageCandidate === "batch-preflight"
        ? stageCandidate
        : "batch-preflight";

    return {
      code,
      message,
      stage: normalizedStage,
      ...(typeof plannedTaskIndex === "number" ? { plannedTaskIndex } : {}),
      ...(typeof operationIndex === "number" ? { operationIndex } : {}),
      ...(taskLocalId ? { taskLocalId } : {}),
      ...(taskIntent ? { taskIntent } : {}),
      ...(field ? { field } : {}),
      ...(ownerLabel ? { ownerLabel } : {}),
      ...(idempotencyKey ? { idempotencyKey } : {}),
      ...(operationKindCandidate === "create" ||
      operationKindCandidate === "update"
        ? { operationKind: operationKindCandidate }
        : {}),
      ...(taskVariant === "enrichment" ||
      taskVariant === "execution" ||
      taskVariant === "review"
        ? { taskVariant }
        : {}),
      ...(this.isRecord(record.details) ? { details: record.details } : {}),
    };
  }

  private extractErrorDetailsRecord(
    error: unknown,
  ): Record<string, unknown> | undefined {
    if (!error || typeof error !== "object") {
      return undefined;
    }

    const details = (error as { details?: unknown }).details;
    return this.isRecord(details) ? details : undefined;
  }

  private describeMilestonePlanningOperation(
    details: Record<string, unknown> | undefined,
  ): string | undefined {
    if (!details) {
      return undefined;
    }

    const stage = this.readOptionalString(details, ["stage"]);
    const operationKind = this.readOptionalString(details, ["operationKind"]);
    const ownerLabel = this.readOptionalString(details, ["ownerLabel"]);
    const operationIndex =
      typeof details.operationIndex === "number"
        ? details.operationIndex
        : undefined;

    const parts: string[] = [];

    if (stage) {
      parts.push(stage);
    }

    if (operationKind) {
      parts.push(
        typeof operationIndex === "number" && operationIndex >= 0
          ? `${operationKind}[${operationIndex}]`
          : operationKind,
      );
    } else if (typeof operationIndex === "number" && operationIndex >= 0) {
      parts.push(`operation[${operationIndex}]`);
    }

    if (ownerLabel) {
      parts.push(`for ${ownerLabel}`);
    }

    return parts.length > 0 ? parts.join(" ") : undefined;
  }

  private describeMilestonePlanningProgress(
    details: Record<string, unknown> | undefined,
    fallback?: { createdCount?: number },
  ): string | undefined {
    const createdTasksSoFar = Array.isArray(details?.createdTasksSoFar)
      ? details.createdTasksSoFar.length
      : undefined;
    const updatedTasksSoFar = Array.isArray(details?.updatedTasksSoFar)
      ? details.updatedTasksSoFar.length
      : undefined;
    const createdCount =
      typeof createdTasksSoFar === "number"
        ? createdTasksSoFar
        : typeof fallback?.createdCount === "number"
          ? fallback.createdCount
          : undefined;
    const updatedCount =
      typeof updatedTasksSoFar === "number" ? updatedTasksSoFar : 0;

    if (
      typeof createdCount !== "number" &&
      typeof updatedTasksSoFar !== "number"
    ) {
      return undefined;
    }

    const progressParts: string[] = [];

    if (typeof createdCount === "number") {
      progressParts.push(
        `${createdCount} task${createdCount === 1 ? "" : "s"} created`,
      );
    }

    if (typeof updatedCount === "number") {
      progressParts.push(
        `${updatedCount} task${updatedCount === 1 ? "" : "s"} updated`,
      );
    }

    return progressParts.length > 0
      ? `progress before failure: ${progressParts.join(", ")}`
      : undefined;
  }

  private buildDispatchFailureMessage(
    error: unknown,
    fallback: string,
  ): string {
    const baseMessage =
      typeof fallback === "string" && fallback.trim().length > 0
        ? fallback.trim()
        : "Unknown dispatch error";
    const details = this.extractErrorDetailsRecord(error);

    if (!details) {
      return baseMessage;
    }

    const issueMessages = Array.isArray(details.issues)
      ? details.issues
          .map((issue) => {
            const normalized = this.normalizeBatchValidationIssue(issue);
            return normalized?.message;
          })
          .filter(
            (message): message is string =>
              typeof message === "string" && message.trim().length > 0,
          )
      : [];

    if (issueMessages.length > 0) {
      return `${baseMessage} First issue: ${issueMessages[0]}${
        issueMessages.length > 1
          ? ` (+${issueMessages.length - 1} more issue${issueMessages.length - 1 === 1 ? "" : "s"})`
          : ""
      }`;
    }

    const operationSummary = this.describeMilestonePlanningOperation(details);
    const progressSummary = this.describeMilestonePlanningProgress(details);
    const causeMessage = this.readOptionalString(details, [
      "causeMessage",
      "message",
    ]);

    const enriched = [
      baseMessage,
      operationSummary ? `Operation: ${operationSummary}.` : undefined,
      progressSummary ? `${progressSummary}.` : undefined,
      causeMessage && causeMessage !== baseMessage
        ? `Cause: ${causeMessage}.`
        : undefined,
    ]
      .filter(
        (part): part is string =>
          typeof part === "string" && part.trim().length > 0,
      )
      .join(" ");

    return enriched.length > 0 ? enriched : baseMessage;
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
    source: Record<string, unknown> | undefined,
    keys: string[],
  ): string | undefined {
    if (!source) {
      return undefined;
    }

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
