import { createLogger } from "../../config/logger";
import type { JobState } from "./job.schemas";
import type { JobRecord } from "./job.service";

const logger = createLogger({ module: "jobs", component: "job-runner" });

const ACTIVE_JOB_STATES: JobState[] = [
  "INTAKE",
  "SPEC_READY",
  "PLAN_READY",
  "ARCH_READY",
  "SCAFFOLD_READY",
  "CODE_READY",
  "TEST_READY",
  "SECURITY_READY",
  "STAGING_READY",
];

export type JobRunnerDecision =
  | {
      type: "noop";
      reason?: string;
    }
  | {
      type: "wait";
      reason: string;
    }
  | {
      type: "dispatch-task";
      taskId?: string;
      reason?: string;
    }
  | {
      type: "advance-state";
      nextState: JobState;
      reason?: string;
    }
  | {
      type: "run-scaffold";
      reason?: string;
    }
  | {
      type: "run-tests";
      reason?: string;
    }
  | {
      type: "deploy-staging";
      reason?: string;
    }
  | {
      type: "fail-job";
      reason: string;
    };

export interface JobsServicePort {
  listJobs(input?: {
    projectId?: string;
    state?: JobState;
    type?: "create-app" | "update-app" | "deploy";
    limit?: number;
    skip?: number;
  }): Promise<JobRecord[]>;
  advanceState(jobId: string, state: JobState): Promise<JobRecord>;
  markFailed(jobId: string, error: string): Promise<JobRecord>;
  setCurrentTask(jobId: string, taskId: string): Promise<JobRecord>;
}

export interface StateMachinePort {
  evaluate(job: JobRecord): Promise<JobRunnerDecision> | JobRunnerDecision;
}

export interface AgentDispatchServicePort {
  dispatchNextTask(job: JobRecord, input?: { taskId?: string }): Promise<void>;
}

export interface ExecutionServicePort {
  scaffoldProject(job: JobRecord): Promise<void>;
  runTests(job: JobRecord): Promise<void>;
}

export interface StagingDeployServicePort {
  deploy(job: JobRecord): Promise<void>;
}

export type MilestoneStatus =
  | "draft"
  | "planned"
  | "ready"
  | "in_progress"
  | "blocked"
  | "review"
  | "completed"
  | "cancelled";

export type MilestoneRecord = {
  _id: string;
  projectId: string;
  title: string;
  description?: string;
  order: number;
  status: MilestoneStatus;
  goal?: string;
  scope: string[];
  acceptanceCriteria: string[];
  dependsOnMilestoneId?: string;
  startedAt?: Date;
  completedAt?: Date;
  confirmedAt?: Date;
  createdAt: Date;
  updatedAt: Date;
};

export type TaskStatus =
  | "queued"
  | "running"
  | "qa"
  | "succeeded"
  | "failed"
  | "canceled";

export type TaskRecord = {
  _id: string;
  jobId: string;
  projectId: string;
  milestoneId: string;
  parentTaskId?: string;
  dependencies: string[];
  status: TaskStatus;
  sequence: number;
  target: {
    agentId: string;
  };
  createdAt: Date;
  updatedAt: Date;
};

export interface MilestonesServicePort {
  getCurrentActiveMilestone(projectId: string): Promise<MilestoneRecord | null>;
  getNextStartableMilestone(projectId: string): Promise<MilestoneRecord | null>;
  startMilestone(milestoneId: string): Promise<MilestoneRecord>;
  completeMilestone(milestoneId: string): Promise<MilestoneRecord>;
}

export interface TasksServicePort {
  countTasks(input?: {
    jobId?: string;
    projectId?: string;
    milestoneId?: string;
    parentTaskId?: string;
    status?: TaskStatus;
    intent?: string;
    agentId?: string;
  }): Promise<number>;
  listNextRunnableTask(input?: {
    jobId?: string;
    agentId?: string;
    milestoneId?: string;
    ignoreRetryAt?: boolean;
  }): Promise<TaskRecord | null>;
}

type TasksServiceWithListTasks = TasksServicePort & {
  listTasks?: (input?: {
    jobId?: string;
    projectId?: string;
    milestoneId?: string;
    parentTaskId?: string;
    status?: TaskStatus;
    intent?: string;
    agentId?: string;
    limit?: number;
    skip?: number;
  }) => Promise<TaskRecord[]>;
};

export type JobRunnerDependencies = {
  jobsService: JobsServicePort;
  stateMachine: StateMachinePort;
  agentDispatchService: AgentDispatchServicePort;
  executionService: ExecutionServicePort;
  stagingDeployService: StagingDeployServicePort;
  milestonesService: MilestonesServicePort;
  tasksService: TasksServicePort;
  pollIntervalMs?: number;
  maxJobsPerTick?: number;
};

type MilestoneTaskSummary = {
  totalCount: number;
  queuedCount: number;
  runningCount: number;
  qaCount: number;
  succeededCount: number;
  failedCount: number;
  canceledCount: number;
  openCount: number;
  nextRunnableTask: TaskRecord | null;
};

export class JobRunner {
  private readonly jobsService: JobsServicePort;
  private readonly stateMachine: StateMachinePort;
  private readonly agentDispatchService: AgentDispatchServicePort;
  private readonly executionService: ExecutionServicePort;
  private readonly stagingDeployService: StagingDeployServicePort;
  private readonly milestonesService: MilestonesServicePort;
  private readonly tasksService: TasksServicePort;

  private readonly pollIntervalMs: number;
  private readonly maxJobsPerTick: number;

  private readonly activeJobIds = new Set<string>();

  private timer: NodeJS.Timeout | null = null;
  private running = false;
  private tickInFlight = false;

  constructor(dependencies: JobRunnerDependencies) {
    this.jobsService = dependencies.jobsService;
    this.stateMachine = dependencies.stateMachine;
    this.agentDispatchService = dependencies.agentDispatchService;
    this.executionService = dependencies.executionService;
    this.stagingDeployService = dependencies.stagingDeployService;
    this.milestonesService = dependencies.milestonesService;
    this.tasksService = dependencies.tasksService;

    this.pollIntervalMs = dependencies.pollIntervalMs ?? 5_000;
    this.maxJobsPerTick = dependencies.maxJobsPerTick ?? 10;
  }

  start(): void {
    if (this.running) {
      logger.warn("Job runner start called, but runner is already active.");
      return;
    }

    this.running = true;

    logger.info(
      {
        pollIntervalMs: this.pollIntervalMs,
        maxJobsPerTick: this.maxJobsPerTick,
      },
      "Job runner started.",
    );

    this.timer = setInterval(() => {
      void this.runOnce();
    }, this.pollIntervalMs);

    void this.runOnce();
  }

  async stop(): Promise<void> {
    if (!this.running) {
      return;
    }

    this.running = false;

    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }

    logger.info("Job runner stopped.");
  }

  async runOnce(): Promise<void> {
    if (!this.running) {
      return;
    }

    if (this.tickInFlight) {
      logger.debug(
        "Skipping tick because another runner tick is still in progress.",
      );
      return;
    }

    this.tickInFlight = true;

    try {
      const jobs = await this.collectRunnableJobs();

      if (jobs.length === 0) {
        logger.debug("No runnable jobs found for this tick.");
        return;
      }

      logger.debug({ jobCount: jobs.length }, "Processing runnable jobs.");

      await Promise.all(jobs.map((job) => this.processJob(job)));
    } catch (error) {
      logger.error(
        {
          err: error,
        },
        "Job runner tick failed.",
      );
    } finally {
      this.tickInFlight = false;
    }
  }

  private async collectRunnableJobs(): Promise<JobRecord[]> {
    const jobBuckets = await Promise.all(
      ACTIVE_JOB_STATES.map((state) =>
        this.jobsService.listJobs({
          state,
          limit: this.maxJobsPerTick,
        }),
      ),
    );

    const dedupedJobs = new Map<string, JobRecord>();

    for (const jobs of jobBuckets) {
      for (const job of jobs) {
        if (this.activeJobIds.has(job._id)) {
          continue;
        }

        if (!dedupedJobs.has(job._id)) {
          dedupedJobs.set(job._id, job);
        }

        if (dedupedJobs.size >= this.maxJobsPerTick) {
          return Array.from(dedupedJobs.values());
        }
      }
    }

    return Array.from(dedupedJobs.values());
  }

  private async processJob(job: JobRecord): Promise<void> {
    if (this.activeJobIds.has(job._id)) {
      return;
    }

    this.activeJobIds.add(job._id);

    const jobLogger = logger.child({
      jobId: job._id,
      projectId: job.projectId,
      state: job.state,
      type: job.type,
    });

    try {
      const milestoneFlowHandled = await this.handleMilestoneFlow(
        job,
        jobLogger,
      );

      if (milestoneFlowHandled) {
        return;
      }

      jobLogger.debug("Evaluating job in state machine.");

      const decision = await this.stateMachine.evaluate(job);

      jobLogger.debug({ decision }, "State machine decision received.");

      await this.handleDecision(job, decision, jobLogger);
    } catch (error) {
      const message =
        error instanceof Error ? error.message : "Unknown job runner error";

      jobLogger.error(
        {
          err: error,
        },
        "Job processing failed. Marking job as FAILED.",
      );

      await this.jobsService.markFailed(job._id, message);
    } finally {
      this.activeJobIds.delete(job._id);
    }
  }

  private async handleMilestoneFlow(
    job: JobRecord,
    jobLogger: typeof logger,
  ): Promise<boolean> {
    if (!this.shouldUseMilestones(job)) {
      return false;
    }

    let activeMilestone =
      await this.milestonesService.getCurrentActiveMilestone(job.projectId);

    if (!activeMilestone) {
      const nextMilestone =
        await this.milestonesService.getNextStartableMilestone(job.projectId);

      if (!nextMilestone) {
        jobLogger.debug(
          "Milestone-driven job has no active or startable milestone yet. Waiting.",
        );
        return true;
      }

      jobLogger.info(
        {
          milestoneId: nextMilestone._id,
          milestoneTitle: nextMilestone.title,
          milestoneOrder: nextMilestone.order,
        },
        "Starting next startable milestone for project.",
      );

      activeMilestone = await this.milestonesService.startMilestone(
        nextMilestone._id,
      );

      jobLogger.debug(
        {
          milestoneId: activeMilestone._id,
          milestoneTitle: activeMilestone.title,
        },
        "Started milestone and continuing milestone evaluation in the same tick.",
      );
    }

    const summary = await this.summarizeMilestoneTasks({
      jobId: job._id,
      milestoneId: activeMilestone._id,
    });

    if (summary.nextRunnableTask) {
      jobLogger.info(
        {
          milestoneId: activeMilestone._id,
          milestoneTitle: activeMilestone.title,
          taskId: summary.nextRunnableTask._id,
          queuedCount: summary.queuedCount,
          runningCount: summary.runningCount,
        },
        "Dispatching next dependency-ready task for active milestone.",
      );

      await this.jobsService.setCurrentTask(
        job._id,
        summary.nextRunnableTask._id,
      );

      await this.agentDispatchService.dispatchNextTask(job, {
        taskId: summary.nextRunnableTask._id,
      });

      return true;
    }

    if (summary.qaCount > 0) {
      const pendingQaTask = await this.findPendingQaTask({
        jobId: job._id,
        milestoneId: activeMilestone._id,
      });

      jobLogger.info(
        {
          milestoneId: activeMilestone._id,
          milestoneTitle: activeMilestone.title,
          qaCount: summary.qaCount,
          queuedCount: summary.queuedCount,
          runningCount: summary.runningCount,
          taskId: pendingQaTask?._id,
        },
        pendingQaTask
          ? "Dispatching pending QA task for active milestone."
          : "Dispatching pending QA work for active milestone through dispatcher lookup.",
      );

      if (pendingQaTask) {
        await this.jobsService.setCurrentTask(job._id, pendingQaTask._id);

        await this.agentDispatchService.dispatchNextTask(job, {
          taskId: pendingQaTask._id,
        });

        return true;
      }

      await this.agentDispatchService.dispatchNextTask(job);
      return true;
    }

    if (summary.runningCount > 0) {
      jobLogger.debug(
        {
          milestoneId: activeMilestone._id,
          runningCount: summary.runningCount,
          queuedCount: summary.queuedCount,
        },
        "Active milestone still has running tasks.",
      );
      return true;
    }

    const retryBypassTask = await this.tasksService.listNextRunnableTask({
      jobId: job._id,
      milestoneId: activeMilestone._id,
      ignoreRetryAt: true,
    });

    if (retryBypassTask) {
      jobLogger.info(
        {
          milestoneId: activeMilestone._id,
          milestoneTitle: activeMilestone.title,
          taskId: retryBypassTask._id,
          queuedCount: summary.queuedCount,
          runningCount: summary.runningCount,
        },
        "Dispatching dependency-ready task immediately because no tasks are running; bypassing retry wait.",
      );

      await this.jobsService.setCurrentTask(job._id, retryBypassTask._id);

      await this.agentDispatchService.dispatchNextTask(job, {
        taskId: retryBypassTask._id,
      });

      return true;
    }

    if (summary.queuedCount > 0) {
      jobLogger.debug(
        {
          milestoneId: activeMilestone._id,
          queuedCount: summary.queuedCount,
        },
        "Active milestone still has queued tasks that are not yet runnable.",
      );
      return true;
    }

    if (summary.totalCount === 0) {
      jobLogger.debug(
        {
          milestoneId: activeMilestone._id,
          milestoneTitle: activeMilestone.title,
        },
        "Active milestone has no tasks yet. Waiting for milestone planning.",
      );
      return true;
    }

    if (summary.failedCount > 0 || summary.canceledCount > 0) {
      jobLogger.warn(
        {
          milestoneId: activeMilestone._id,
          failedCount: summary.failedCount,
          canceledCount: summary.canceledCount,
        },
        "Active milestone has terminal task outcomes and cannot auto-complete yet.",
      );
      return true;
    }

    if (
      summary.openCount === 0 &&
      summary.succeededCount === summary.totalCount
    ) {
      jobLogger.info(
        {
          milestoneId: activeMilestone._id,
          milestoneTitle: activeMilestone.title,
          totalCount: summary.totalCount,
        },
        "Completing active milestone because all milestone tasks succeeded.",
      );

      await this.milestonesService.completeMilestone(activeMilestone._id);
      return true;
    }

    jobLogger.debug(
      {
        milestoneId: activeMilestone._id,
        summary,
      },
      "Milestone flow made no state change this tick.",
    );

    return true;
  }

  private shouldUseMilestones(job: JobRecord): boolean {
    return job.type === "create-app" || job.type === "update-app";
  }

  private async findPendingQaTask(input: {
    jobId: string;
    milestoneId: string;
  }): Promise<TaskRecord | null> {
    const tasksService = this.tasksService as TasksServiceWithListTasks;

    if (typeof tasksService.listTasks !== "function") {
      return null;
    }

    const qaTasks = await tasksService.listTasks({
      jobId: input.jobId,
      milestoneId: input.milestoneId,
      status: "qa",
      limit: 1,
    });

    return qaTasks[0] ?? null;
  }

  private async summarizeMilestoneTasks(input: {
    jobId: string;
    milestoneId: string;
  }): Promise<MilestoneTaskSummary> {
    const [
      totalCount,
      queuedCount,
      runningCount,
      qaCount,
      succeededCount,
      failedCount,
      canceledCount,
      nextRunnableTask,
    ] = await Promise.all([
      this.tasksService.countTasks({
        jobId: input.jobId,
        milestoneId: input.milestoneId,
      }),
      this.tasksService.countTasks({
        jobId: input.jobId,
        milestoneId: input.milestoneId,
        status: "queued",
      }),
      this.tasksService.countTasks({
        jobId: input.jobId,
        milestoneId: input.milestoneId,
        status: "running",
      }),
      this.tasksService.countTasks({
        jobId: input.jobId,
        milestoneId: input.milestoneId,
        status: "qa",
      }),
      this.tasksService.countTasks({
        jobId: input.jobId,
        milestoneId: input.milestoneId,
        status: "succeeded",
      }),
      this.tasksService.countTasks({
        jobId: input.jobId,
        milestoneId: input.milestoneId,
        status: "failed",
      }),
      this.tasksService.countTasks({
        jobId: input.jobId,
        milestoneId: input.milestoneId,
        status: "canceled",
      }),
      this.tasksService.listNextRunnableTask({
        jobId: input.jobId,
        milestoneId: input.milestoneId,
      }),
    ]);

    return {
      totalCount,
      queuedCount,
      runningCount,
      qaCount,
      succeededCount,
      failedCount,
      canceledCount,
      openCount: queuedCount + runningCount + qaCount,
      nextRunnableTask,
    };
  }

  private async handleDecision(
    job: JobRecord,
    decision: JobRunnerDecision,
    jobLogger: typeof logger,
  ): Promise<void> {
    switch (decision.type) {
      case "noop": {
        jobLogger.debug(
          {
            reason: decision.reason,
          },
          "No action required for job.",
        );
        return;
      }

      case "wait": {
        jobLogger.debug(
          {
            reason: decision.reason,
          },
          "Job is waiting for an external condition or downstream completion.",
        );
        return;
      }

      case "dispatch-task": {
        jobLogger.info(
          {
            taskId: decision.taskId,
            reason: decision.reason,
          },
          "Dispatching next task for job.",
        );

        if (decision.taskId) {
          await this.jobsService.setCurrentTask(job._id, decision.taskId);
        }

        await this.agentDispatchService.dispatchNextTask(
          job,
          decision.taskId ? { taskId: decision.taskId } : undefined,
        );
        return;
      }

      case "advance-state": {
        jobLogger.info(
          {
            nextState: decision.nextState,
            reason: decision.reason,
          },
          "Advancing job state.",
        );

        await this.jobsService.advanceState(job._id, decision.nextState);
        return;
      }

      case "run-scaffold": {
        jobLogger.info(
          {
            reason: decision.reason,
          },
          "Running scaffold phase for job.",
        );

        await this.executionService.scaffoldProject(job);
        return;
      }

      case "run-tests": {
        jobLogger.info(
          {
            reason: decision.reason,
          },
          "Running test phase for job.",
        );

        await this.executionService.runTests(job);
        return;
      }

      case "deploy-staging": {
        jobLogger.info(
          {
            reason: decision.reason,
          },
          "Deploying job to staging.",
        );

        await this.stagingDeployService.deploy(job);
        return;
      }

      case "fail-job": {
        jobLogger.warn(
          {
            reason: decision.reason,
          },
          "State machine requested job failure.",
        );

        await this.jobsService.markFailed(job._id, decision.reason);
        return;
      }

      default: {
        const exhaustiveCheck: never = decision;
        throw new Error(
          `Unhandled job runner decision: ${String(exhaustiveCheck)}`,
        );
      }
    }
  }
}

export default JobRunner;
