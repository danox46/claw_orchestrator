import { createLogger } from "../../config/logger";
import type { JobRunnerDecision } from "./job-runner";
import type { JobRecord } from "./job.service";

const logger = createLogger({ module: "jobs", component: "state-machine" });

export interface StateMachinePort {
  evaluate(job: JobRecord): Promise<JobRunnerDecision> | JobRunnerDecision;
}

export class StateMachine implements StateMachinePort {
  evaluate(job: JobRecord): JobRunnerDecision {
    logger.debug(
      {
        jobId: job._id,
        projectId: job.projectId,
        state: job.state,
        type: job.type,
        currentTaskId: job.currentTaskId,
      },
      "Evaluating job state.",
    );

    if (job.state === "FAILED") {
      return {
        type: "noop",
        reason: "Job is already in FAILED state.",
      };
    }

    if (job.state === "DEPLOYED") {
      return {
        type: "noop",
        reason: "Job is already deployed.",
      };
    }

    if (job.currentTaskId) {
      return {
        type: "wait",
        reason: `Task ${job.currentTaskId} is still associated with the job.`,
      };
    }

    if (this.shouldUseMilestones(job)) {
      return {
        type: "wait",
        reason:
          "Milestone-driven job orchestration is handled by the job runner milestone flow.",
      };
    }

    switch (job.state) {
      case "INTAKE":
        return {
          type: "dispatch-task",
          reason:
            "Non-milestone intake job should dispatch its first queued task.",
        };

      case "SPEC_READY":
      case "PLAN_READY":
      case "ARCH_READY":
      case "SCAFFOLD_READY":
      case "CODE_READY":
      case "TEST_READY":
      case "SECURITY_READY":
        return {
          type: "dispatch-task",
          reason: "Non-milestone job is ready; dispatch the next queued task.",
        };

      case "STAGING_READY":
        return {
          type: "advance-state",
          nextState: "DEPLOYED",
          reason:
            "Staging phase completed successfully; mark the job as DEPLOYED.",
        };

      default: {
        return {
          type: "fail-job",
          reason: `Unhandled job state in state machine: ${String(job.state)}`,
        };
      }
    }
  }

  private shouldUseMilestones(job: JobRecord): boolean {
    return job.type === "create-app" || job.type === "update-app";
  }
}

export default StateMachine;
