import type { TaskRecord } from "../tasks/task.service";
import type {
  AgentRetryFailureInput,
  AgentRetryServiceDependencies,
  AgentRotateTaskSessionInput,
} from "./agent.types";
import {
  buildTaskSessionName,
  computeNextRetryAt,
  getTaskSessionState,
  normalizeTaskArtifactRefs,
  shouldRetryTask,
  shouldRotateTaskSession,
} from "./agent.helpers";

export class AgentRetryService {
  private readonly tasksService: AgentRetryServiceDependencies["tasksService"];
  private readonly jobsService: AgentRetryServiceDependencies["jobsService"];
  private readonly implementerAgentId: string;

  constructor(dependencies: AgentRetryServiceDependencies) {
    this.tasksService = dependencies.tasksService;
    this.jobsService = dependencies.jobsService;
    this.implementerAgentId = dependencies.implementerAgentId;
  }

  getAttemptNumber(task: TaskRecord): number {
    return task.status === "qa"
      ? Math.max(task.attemptCount, 1)
      : task.attemptCount + 1;
  }

  getSessionState(task: TaskRecord): ReturnType<typeof getTaskSessionState> {
    return getTaskSessionState(task);
  }

  async handleTaskFailure(input: AgentRetryFailureInput): Promise<void> {
    const normalizedArtifacts = normalizeTaskArtifactRefs(input.artifacts);

    if (shouldRetryTask(input.task, input.attemptNumber)) {
      const nextRetryAt = computeNextRetryAt(input.attemptNumber);

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

    if (shouldRotateTaskSession(input.task)) {
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

    const sessionState = getTaskSessionState(input.task);

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

  async handleQaReviewFailure(input: AgentRetryFailureInput): Promise<void> {
    const normalizedArtifacts = normalizeTaskArtifactRefs(input.artifacts);

    if (shouldRetryTask(input.task, input.attemptNumber)) {
      const nextRetryAt = computeNextRetryAt(input.attemptNumber);

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

    if (shouldRotateTaskSession(input.task)) {
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

    const sessionState = getTaskSessionState(input.task);

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

  private async rotateTaskSessionAndRequeue(
    input: AgentRotateTaskSessionInput,
  ): Promise<void> {
    const currentSessionState = getTaskSessionState(input.task);
    const nextSessionCount = currentSessionState.sessionCount + 1;
    const nextSessionName = buildTaskSessionName(
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
}
