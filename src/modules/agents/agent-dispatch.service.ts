import { createLogger } from "../../config/logger";
import type { JobRecord, JobsServicePort } from "../jobs/job.service";
import type {
  MilestoneRecord,
  MilestonesServicePort,
} from "../milestones/milestone.service";
import type { ProjectsServicePort } from "../projects/project.service";
import type { TaskRecord, TasksServicePort } from "../tasks/task.service";
import type {
  OpenClawClient,
  OpenClawTaskStatusResponse,
} from "./openclaw.client";
import type {
  AgentDispatchServiceDependencies,
  MilestoneReviewOutcome,
} from "./agent.types";
import {
  asCreateTaskIntent,
  asRecord,
  buildDispatchFailureMessage,
  buildFailureMessage,
  buildMilestoneTaskSnapshot,
  buildPatchMilestonePlan,
  buildTaskConstraints,
  createServiceError,
  extractConcreteTaskEnrichmentContext,
  extractExecutionTaskEnrichmentUpdates,
  extractMilestoneReviewOutcome,
  extractPlannedPhases,
  isEnrichmentTask,
  isPatchMilestoneForReview,
  normalizeTaskArtifactRefs,
  readOptionalString,
  sanitizeConcreteTaskInputsForDispatch,
} from "./agent.helpers";
import { AgentResultParser } from "./agent-result.parser";
import { AgentRetryService } from "./agent-retry.service";
import { AgentPlanningBatchService } from "./agent-planning-batch.service";

const logger = createLogger({
  module: "agents",
  component: "agent-dispatch-service",
});

export class AgentDispatchService {
  private readonly openClawClient: OpenClawClient;
  private readonly tasksService: TasksServicePort;
  private readonly jobsService: JobsServicePort;
  private readonly milestonesService: MilestonesServicePort;
  private readonly projectsService: ProjectsServicePort | undefined;
  private readonly taskTimeoutMs: number;
  private readonly resultParser: AgentResultParser;
  private readonly retryService: AgentRetryService;
  private readonly planningBatchService: AgentPlanningBatchService;

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
    this.resultParser = new AgentResultParser();

    this.projectOwnerAgentId =
      dependencies.projectOwnerAgentId ?? "project-owner";
    this.projectManagerAgentId =
      dependencies.projectManagerAgentId ?? "project-manager";
    this.implementerAgentId = dependencies.implementerAgentId ?? "implementer";
    this.qaAgentId = dependencies.qaAgentId ?? "qa";
    this.retryService = new AgentRetryService({
      tasksService: this.tasksService,
      jobsService: this.jobsService,
      implementerAgentId: this.implementerAgentId,
    });
    this.planningBatchService = new AgentPlanningBatchService({
      tasksService: this.tasksService,
      milestonesService: this.milestonesService,
      projectOwnerAgentId: this.projectOwnerAgentId,
      projectManagerAgentId: this.projectManagerAgentId,
      implementerAgentId: this.implementerAgentId,
      qaAgentId: this.qaAgentId,
    });
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
    const attemptNumber = this.retryService.getAttemptNumber(task);
    const sessionState = this.retryService.getSessionState(task);

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
        fallbackTaskId: task._id,
        ...sendResult,
        agentId: sendResult.agentId ?? task.target.agentId,
      });

      if (finalResult.status !== "succeeded") {
        const failureMessage = buildFailureMessage(finalResult);

        await this.handleTaskDispatchFailure({
          job,
          task,
          attemptNumber,
          dispatchLogger,
          failureMessage,
          isQaDispatch,
          ...(finalResult.outputs ? { outputs: finalResult.outputs } : {}),
          ...(finalResult.artifacts
            ? { artifacts: finalResult.artifacts }
            : {}),
        });

        return;
      }

      if (this.shouldHandOffTaskToQa(task)) {
        const normalizedArtifacts = normalizeTaskArtifactRefs(
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
        const normalizedArtifacts = normalizeTaskArtifactRefs(
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
      const failureMessage = buildDispatchFailureMessage(
        error,
        error instanceof Error ? error.message : "Unknown dispatch error",
      );

      dispatchLogger.error(
        { err: error },
        "Task dispatch failed unexpectedly.",
      );

      await this.handleTaskDispatchFailure({
        job,
        task,
        attemptNumber,
        dispatchLogger,
        failureMessage,
        isQaDispatch,
        error,
      });
    }
  }

  private async handleTaskDispatchFailure(input: {
    job: JobRecord;
    task: TaskRecord;
    attemptNumber: number;
    dispatchLogger: typeof logger;
    failureMessage: string;
    isQaDispatch: boolean;
    error?: unknown;
    outputs?: Record<string, unknown>;
    artifacts?: unknown;
  }): Promise<void> {
    if (this.isTerminalFailureAttempt(input.task, input.attemptNumber)) {
      await this.markTaskJobAndMilestoneFailed(input);
      return;
    }

    if (input.isQaDispatch) {
      await this.retryService.handleQaReviewFailure({
        job: input.job,
        task: input.task,
        attemptNumber: input.attemptNumber,
        dispatchLogger: input.dispatchLogger,
        failureMessage: input.failureMessage,
        ...(input.error !== undefined ? { error: input.error } : {}),
        ...(input.outputs ? { outputs: input.outputs } : {}),
        ...(input.artifacts ? { artifacts: input.artifacts } : {}),
      });
      return;
    }

    await this.retryService.handleTaskFailure({
      job: input.job,
      task: input.task,
      attemptNumber: input.attemptNumber,
      dispatchLogger: input.dispatchLogger,
      failureMessage: input.failureMessage,
      ...(input.error !== undefined ? { error: input.error } : {}),
      ...(input.outputs ? { outputs: input.outputs } : {}),
      ...(input.artifacts ? { artifacts: input.artifacts } : {}),
    });
  }

  private isTerminalFailureAttempt(
    task: TaskRecord,
    attemptNumber: number,
  ): boolean {
    const sessionState = this.retryService.getSessionState(task);
    const maxAttempts = Math.max(1, task.maxAttempts);
    const currentAttempt = Math.max(1, attemptNumber);
    const maxSessions = Math.max(1, sessionState.maxSessions);
    const currentSession = Math.max(1, sessionState.sessionCount);

    return currentAttempt >= maxAttempts && currentSession >= maxSessions;
  }

  private async markTaskJobAndMilestoneFailed(input: {
    job: JobRecord;
    task: TaskRecord;
    attemptNumber: number;
    dispatchLogger: typeof logger;
    failureMessage: string;
    isQaDispatch: boolean;
    error?: unknown;
    outputs?: Record<string, unknown>;
    artifacts?: unknown;
  }): Promise<void> {
    const normalizedArtifacts = normalizeTaskArtifactRefs(input.artifacts);

    await this.tasksService.updateTask(input.task._id, {
      status: "failed",
      ...(input.outputs ? { outputs: input.outputs } : {}),
      ...(normalizedArtifacts.length > 0
        ? { artifacts: normalizedArtifacts }
        : {}),
    });

    const failedMilestoneStatus = "failed" as unknown as Parameters<
      MilestonesServicePort["updateMilestone"]
    >[1]["status"];
    const failedMilestone = await this.milestonesService.updateMilestone(
      input.task.milestoneId,
      { status: failedMilestoneStatus },
    );

    const failedJobState = "FAILED" as unknown as Parameters<
      JobsServicePort["advanceState"]
    >[1];
    const failedJob = await this.jobsService.advanceState(
      input.job._id,
      failedJobState,
    );

    input.dispatchLogger.warn(
      {
        jobId: failedJob._id,
        jobState: failedJob.state,
        projectId: input.job.projectId,
        milestoneId: failedMilestone._id,
        milestoneStatus: failedMilestone.status,
        taskId: input.task._id,
        taskStatus: "failed",
        attemptNumber: input.attemptNumber,
        maxAttempts: input.task.maxAttempts,
        isQaDispatch: input.isQaDispatch,
        ...(input.error instanceof Error
          ? { errorName: input.error.name, errorMessage: input.error.message }
          : {}),
      },
      "Task exhausted all retries and sessions; marked task, milestone, and job as failed. Project status was left unchanged.",
    );
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

  private shouldHandOffTaskToQa(task: TaskRecord): boolean {
    return (
      task.status !== "qa" && task.target.agentId === this.implementerAgentId
    );
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
        await this.planningBatchService.enqueueConcreteMilestoneTasks(input);
        return true;
      case "plan_next_tasks":
        await this.planningBatchService.enqueueConcreteMilestoneTasks(input);
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
    const phases = extractPlannedPhases(input.finalResult.outputs);

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

    const parentInputs = asRecord(input.task.inputs);

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
        intent: asCreateTaskIntent("plan_phase_tasks"),
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
        constraints: buildTaskConstraints(input.task.constraints),
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

  private async buildDispatchInputs(
    task: TaskRecord,
  ): Promise<Record<string, unknown>> {
    const baseInputs = asRecord(task.inputs);
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

    const systemTaskType = readOptionalString(baseInputs, ["systemTaskType"]);
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
    const enrichmentTaskIdempotencyKey = readOptionalString(input.baseInputs, [
      "enrichmentTaskIdempotencyKey",
      "enrichmentIdempotencyKey",
    ]);
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
      extractConcreteTaskEnrichmentContext(enrichmentTask);

    return {
      ...sanitizeConcreteTaskInputsForDispatch(
        input.baseInputs,
        input.isQaDispatch,
      ),
      ...(dependencyContext.length > 0
        ? { dependencyTaskContext: dependencyContext }
        : {}),
      ...(enrichmentContext ? { enrichment: enrichmentContext } : {}),
    };
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
      isEnrichmentTask(candidateTask),
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
        buildMilestoneTaskSnapshot(input.task, {
          includeOutputs: true,
        }),
      );
    }

    const dependencySnapshots = input.dependencyTasks
      .filter(
        (candidateTask) =>
          candidateTask._id !== input.enrichmentTask?._id &&
          !isEnrichmentTask(candidateTask),
      )
      .sort((left, right) => {
        if (left.sequence !== right.sequence) {
          return left.sequence - right.sequence;
        }

        return left.createdAt.getTime() - right.createdAt.getTime();
      })
      .map((candidateTask) =>
        buildMilestoneTaskSnapshot(candidateTask, {
          includeOutputs: true,
        }),
      );

    return [...contextTasks, ...dependencySnapshots];
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

  private async handleMilestoneReviewOutcome(input: {
    job: JobRecord;
    task: TaskRecord;
    finalResult: OpenClawTaskStatusResponse;
    dispatchLogger: typeof logger;
  }): Promise<void> {
    const milestone = await this.milestonesService.requireMilestoneById(
      input.task.milestoneId,
    );
    const outcome = extractMilestoneReviewOutcome(input.finalResult.outputs);

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
    const enrichmentInputs = asRecord(input.task.inputs);
    const executionTaskIdempotencyKey = readOptionalString(enrichmentInputs, [
      "executionTaskIdempotencyKey",
    ]);

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

    const enrichmentUpdates = extractExecutionTaskEnrichmentUpdates(
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
      isPatchMilestoneForReview(milestone, input.reviewTask._id),
    );
    let createdPatchMilestone = false;
    let reusedPatchMilestone = false;

    if (!patchMilestone) {
      await this.shiftMilestoneOrdersForPatchInsertion({
        sourceMilestone: input.sourceMilestone,
        milestones,
      });

      const patchPlan = buildPatchMilestonePlan({
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
        intent: asCreateTaskIntent("plan_phase_tasks"),
        inputs: {
          ...(typeof input.reviewTask.inputs === "object" &&
          input.reviewTask.inputs !== null
            ? asRecord(input.reviewTask.inputs)
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
        constraints: buildTaskConstraints(input.reviewTask.constraints),
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

  private async resolveFinalResult(input: {
    fallbackTaskId?: string;
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
    const parsedResult = this.resultParser.parse({
      raw: input,
      ...(typeof input.fallbackTaskId === "string" &&
      input.fallbackTaskId.trim().length > 0
        ? { fallbackTaskId: input.fallbackTaskId }
        : {}),
      fallbackStatus: input.status,
      ...(typeof input.summary === "string"
        ? { fallbackSummary: input.summary }
        : {}),
    });

    const normalizedArtifacts = normalizeTaskArtifactRefs(
      parsedResult.artifacts,
    );
    const parsedSummary =
      typeof parsedResult.summary === "string" ? parsedResult.summary : "";
    const parsedErrors = Array.isArray(parsedResult.errors)
      ? parsedResult.errors
      : [];

    const baseResponse = {
      ...(parsedResult.openclawTaskId
        ? { openclawTaskId: parsedResult.openclawTaskId }
        : {}),
      ...(parsedResult.sessionId ? { sessionId: parsedResult.sessionId } : {}),
      agentId: parsedResult.agentId ?? input.agentId,
      ...(parsedResult.outputs ? { outputs: parsedResult.outputs } : {}),
      ...(normalizedArtifacts.length > 0
        ? { artifacts: normalizedArtifacts }
        : {}),
      ...(parsedResult.raw !== undefined ? { raw: parsedResult.raw } : {}),
    } satisfies Partial<OpenClawTaskStatusResponse> & {
      agentId: string;
    };

    if (
      parsedResult.status === "succeeded" ||
      parsedResult.status === "failed" ||
      parsedResult.status === "canceled"
    ) {
      return {
        ...baseResponse,
        status: parsedResult.status,
        ...(parsedSummary.trim().length > 0 ? { summary: parsedSummary } : {}),
        ...(parsedErrors.length > 0 ? { errors: parsedErrors } : {}),
      };
    }

    const nonTerminalMessage =
      parsedSummary.trim().length > 0
        ? parsedSummary.trim()
        : `OpenClaw returned non-terminal status "${parsedResult.status}" in synchronous /v1/responses mode.`;

    const nextErrors = [
      ...parsedErrors,
      `Non-terminal status received from OpenClaw synchronous adapter: ${parsedResult.status}.`,
    ];

    return {
      ...baseResponse,
      status: "failed",
      summary: nonTerminalMessage,
      errors: nextErrors,
    };
  }
}

export default AgentDispatchService;
