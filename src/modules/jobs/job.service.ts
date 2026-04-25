import { type QueryFilter, Types } from "mongoose";
import { NotFoundError } from "../../shared/errors/not-found-error";
import { ValidationError } from "../../shared/errors/validation-error";
import type {
  CreateJobRequest,
  JobMetadata,
  JobState,
  JobType,
  UpdateJobRequest,
} from "./job.schemas";
import JobModel, { type JobModelType } from "./job.model";

export type CreateJobInput = CreateJobRequest;
export type UpdateJobInput = UpdateJobRequest;

type JobRecordMetadata = {
  requestedBy?: string;
  appType: JobMetadata["appType"];
  stack: JobMetadata["stack"];
  deployment: JobMetadata["deployment"];
} & Record<string, unknown>;

export type JobRecord = {
  _id: string;
  projectId: string;
  type: JobType;
  state: JobState;
  prompt: string;
  metadata: JobRecordMetadata;
  currentTaskId?: string;
  error?: string;
  createdAt: Date;
  updatedAt: Date;
};

export type ListJobsInput = {
  projectId?: string;
  state?: JobState;
  type?: JobType;
  limit?: number;
  skip?: number;
};

export type CountJobsInput = Pick<
  ListJobsInput,
  "projectId" | "state" | "type"
>;

export interface JobsServicePort {
  createJob(input: CreateJobInput): Promise<JobRecord>;
  getJobById(jobId: string): Promise<JobRecord | null>;
  requireJobById(jobId: string): Promise<JobRecord>;
  updateJob(jobId: string, updates: UpdateJobInput): Promise<JobRecord>;
  setCurrentTask(jobId: string, taskId: string): Promise<JobRecord>;
  advanceState(jobId: string, state: JobState): Promise<JobRecord>;
  markFailed(jobId: string, error: string): Promise<JobRecord>;
  clearError(jobId: string): Promise<JobRecord>;
  listJobs(input?: ListJobsInput): Promise<JobRecord[]>;
  countJobs(input?: CountJobsInput): Promise<number>;
}

type JobDocumentLike = {
  id: string;
  projectId: Types.ObjectId;
  type: JobType;
  state: JobState;
  prompt: string;
  metadata: {
    requestedBy?: string | null;
    appType: JobMetadata["appType"];
    stack: JobMetadata["stack"];
    deployment: JobMetadata["deployment"];
  } & Record<string, unknown>;
  currentTaskId?: Types.ObjectId | null;
  error?: string | null;
  createdAt: Date;
  updatedAt: Date;
};

function toObjectId(value: string, fieldName: string): Types.ObjectId {
  if (!Types.ObjectId.isValid(value)) {
    throw new ValidationError({
      message: `Invalid ${fieldName}: ${value}`,
      code: "INVALID_OBJECT_ID",
      details: {
        fieldName,
        value,
      },
      statusCode: 400,
    });
  }

  return new Types.ObjectId(value);
}

function mapJobMetadata(
  metadata: JobDocumentLike["metadata"],
): JobRecordMetadata {
  const { requestedBy, appType, stack, deployment, ...rest } =
    metadata as JobDocumentLike["metadata"] & Record<string, unknown>;

  return {
    ...rest,
    ...(requestedBy ? { requestedBy } : {}),
    appType,
    stack: {
      frontend: stack.frontend,
      backend: stack.backend,
      database: stack.database,
    },
    deployment: {
      target: deployment.target,
      environment: deployment.environment,
    },
  };
}

function mapJob(document: JobDocumentLike): JobRecord {
  return {
    _id: document.id,
    projectId: document.projectId.toString(),
    type: document.type,
    state: document.state,
    prompt: document.prompt,
    metadata: mapJobMetadata(document.metadata),
    ...(document.currentTaskId
      ? { currentTaskId: document.currentTaskId.toString() }
      : {}),
    ...(document.error ? { error: document.error } : {}),
    createdAt: document.createdAt,
    updatedAt: document.updatedAt,
  };
}

function normalizeJobMetadata(
  input: CreateJobInput["metadata"],
): Record<string, unknown> {
  const metadata = input as CreateJobInput["metadata"] &
    Record<string, unknown>;
  const { requestedBy, appType, stack, deployment, ...rest } = metadata;

  return {
    ...rest,
    ...(typeof requestedBy === "string" && requestedBy.trim().length > 0
      ? { requestedBy: requestedBy.trim() }
      : {}),
    appType,
    stack,
    deployment,
  };
}

export class JobService implements JobsServicePort {
  async createJob(input: CreateJobInput): Promise<JobRecord> {
    const created = await JobModel.create({
      projectId: toObjectId(input.projectId, "projectId"),
      type: input.type,
      state: input.state,
      prompt: input.prompt.trim(),
      metadata: normalizeJobMetadata(input.metadata),
      ...(typeof input.currentTaskId === "string"
        ? { currentTaskId: toObjectId(input.currentTaskId, "currentTaskId") }
        : {}),
      ...(typeof input.error === "string" && input.error.trim().length > 0
        ? { error: input.error.trim() }
        : {}),
    });

    return mapJob(created);
  }

  async getJobById(jobId: string): Promise<JobRecord | null> {
    const job = await JobModel.findById(toObjectId(jobId, "jobId")).exec();
    return job ? mapJob(job) : null;
  }

  async requireJobById(jobId: string): Promise<JobRecord> {
    const job = await this.getJobById(jobId);

    if (!job) {
      throw new NotFoundError({
        message: `Job not found: ${jobId}`,
        code: "JOB_NOT_FOUND",
        details: { jobId },
      });
    }

    return job;
  }

  async updateJob(jobId: string, updates: UpdateJobInput): Promise<JobRecord> {
    const updatePayload: Partial<JobModelType> = {};

    if (typeof updates.state === "string") {
      updatePayload.state = updates.state;
    }

    if (typeof updates.currentTaskId === "string") {
      updatePayload.currentTaskId = toObjectId(
        updates.currentTaskId,
        "currentTaskId",
      );
    }

    if (typeof updates.error === "string") {
      updatePayload.error = updates.error.trim();
    }

    const updated = await JobModel.findByIdAndUpdate(
      toObjectId(jobId, "jobId"),
      updatePayload,
      {
        new: true,
        runValidators: true,
      },
    ).exec();

    if (!updated) {
      throw new NotFoundError({
        message: `Job not found: ${jobId}`,
        code: "JOB_NOT_FOUND",
        details: { jobId },
      });
    }

    return mapJob(updated);
  }

  async setCurrentTask(jobId: string, taskId: string): Promise<JobRecord> {
    return this.updateJob(jobId, {
      currentTaskId: taskId,
    });
  }

  async advanceState(jobId: string, state: JobState): Promise<JobRecord> {
    return this.updateJob(jobId, { state });
  }

  async markFailed(jobId: string, error: string): Promise<JobRecord> {
    return this.updateJob(jobId, {
      state: "FAILED",
      error,
    });
  }

  async clearError(jobId: string): Promise<JobRecord> {
    const updated = await JobModel.findByIdAndUpdate(
      toObjectId(jobId, "jobId"),
      { $unset: { error: 1 } },
      {
        new: true,
        runValidators: true,
      },
    ).exec();

    if (!updated) {
      throw new NotFoundError({
        message: `Job not found: ${jobId}`,
        code: "JOB_NOT_FOUND",
        details: { jobId },
      });
    }

    return mapJob(updated);
  }

  async listJobs(input: ListJobsInput = {}): Promise<JobRecord[]> {
    const filter = this.buildFilter(input);
    const limit = Math.min(Math.max(input.limit ?? 25, 1), 100);
    const skip = Math.max(input.skip ?? 0, 0);

    const jobs = await JobModel.find(filter)
      .sort({ updatedAt: -1 })
      .skip(skip)
      .limit(limit)
      .exec();

    return jobs.map(mapJob);
  }

  async countJobs(input: CountJobsInput = {}): Promise<number> {
    const filter = this.buildFilter(input);
    return JobModel.countDocuments(filter).exec();
  }

  private buildFilter(
    input: CountJobsInput | ListJobsInput,
  ): QueryFilter<JobModelType> {
    const filter: QueryFilter<JobModelType> = {};

    if (typeof input.projectId === "string") {
      filter.projectId = toObjectId(input.projectId, "projectId");
    }

    if (input.state) {
      filter.state = input.state;
    }

    if (input.type) {
      filter.type = input.type;
    }

    return filter;
  }
}

export default JobService;
