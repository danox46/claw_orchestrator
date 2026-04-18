import type { PaginatedResult } from "../../shared/http/pagination";
import type { JobRecord } from "./job.service";

export type PresentedJob = {
  id: string;
  projectId: string;
  type: JobRecord["type"];
  state: JobRecord["state"];
  prompt: string;
  metadata: JobRecord["metadata"];
  currentTaskId?: string;
  error?: string;
  createdAt: string;
  updatedAt: string;
};

export type PresentedPaginatedJobs = {
  items: PresentedJob[];
  pagination: PaginatedResult<JobRecord>["pagination"];
};

export function presentJob(job: JobRecord): PresentedJob {
  return {
    id: job._id,
    projectId: job.projectId,
    type: job.type,
    state: job.state,
    prompt: job.prompt,
    metadata: {
      ...job.metadata,
      ...(typeof job.metadata.requestedBy === "string" &&
      job.metadata.requestedBy.length > 0
        ? { requestedBy: job.metadata.requestedBy }
        : {}),
    },
    ...(typeof job.currentTaskId === "string" && job.currentTaskId.length > 0
      ? { currentTaskId: job.currentTaskId }
      : {}),
    ...(typeof job.error === "string" && job.error.length > 0
      ? { error: job.error }
      : {}),
    createdAt: job.createdAt.toISOString(),
    updatedAt: job.updatedAt.toISOString(),
  };
}

export function presentJobs(jobs: JobRecord[]): PresentedJob[] {
  return jobs.map(presentJob);
}

export function presentPaginatedJobs(
  result: PaginatedResult<JobRecord>,
): PresentedPaginatedJobs {
  return {
    items: result.items.map(presentJob),
    pagination: result.pagination,
  };
}
