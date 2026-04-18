import type { Request, Response } from "express";
import { successResponse } from "../../shared/http/api-response";
import {
  buildPaginatedResult,
  normalizePagination,
} from "../../shared/http/pagination";
import { ServiceError } from "../../shared/errors/service-error";
import type { JobsServicePort } from "./job.service";
import type {
  CreateJobRequest,
  JobIdParams,
  ListJobsQuery,
  UpdateJobRequest,
} from "./job.schemas";
import { presentJob, presentPaginatedJobs } from "./job-presenter";

type CreateJobLocals = {
  validated?: {
    body?: CreateJobRequest;
  };
};

type GetJobByIdLocals = {
  validated?: {
    params?: JobIdParams;
  };
};

type UpdateJobLocals = {
  validated?: {
    params?: JobIdParams;
    body?: UpdateJobRequest;
  };
};

type ListJobsLocals = {
  validated?: {
    query?: ListJobsQuery;
  };
};

export type JobsController = {
  create: (_req: Request, res: Response<any, CreateJobLocals>) => Promise<void>;
  getById: (
    _req: Request,
    res: Response<any, GetJobByIdLocals>,
  ) => Promise<void>;
  update: (_req: Request, res: Response<any, UpdateJobLocals>) => Promise<void>;
  list: (_req: Request, res: Response<any, ListJobsLocals>) => Promise<void>;
};

function requireValidatedBody<T>(
  res: Response<any, { validated?: { body?: T } }>,
): T {
  const body = res.locals.validated?.body;

  if (body === undefined) {
    throw new ServiceError({
      code: "MISSING_VALIDATED_BODY",
      message: "Validated request body was not found.",
      statusCode: 500,
      expose: false,
    });
  }

  return body;
}

function requireValidatedParams<T>(
  res: Response<any, { validated?: { params?: T } }>,
): T {
  const params = res.locals.validated?.params;

  if (params === undefined) {
    throw new ServiceError({
      code: "MISSING_VALIDATED_PARAMS",
      message: "Validated route params were not found.",
      statusCode: 500,
      expose: false,
    });
  }

  return params;
}

function requireValidatedQuery<T>(
  res: Response<any, { validated?: { query?: T } }>,
): T {
  const query = res.locals.validated?.query;

  if (query === undefined) {
    throw new ServiceError({
      code: "MISSING_VALIDATED_QUERY",
      message: "Validated query params were not found.",
      statusCode: 500,
      expose: false,
    });
  }

  return query;
}

export function createJobsController(
  jobsService: JobsServicePort,
): JobsController {
  return {
    create: async (
      _req: Request,
      res: Response<any, CreateJobLocals>,
    ): Promise<void> => {
      const input = requireValidatedBody(res);
      const job = await jobsService.createJob(input);

      res.status(201).json(successResponse(presentJob(job)));
    },

    getById: async (
      _req: Request,
      res: Response<any, GetJobByIdLocals>,
    ): Promise<void> => {
      const { jobId } = requireValidatedParams(res);
      const job = await jobsService.requireJobById(jobId);

      res.status(200).json(successResponse(presentJob(job)));
    },

    update: async (
      _req: Request,
      res: Response<any, UpdateJobLocals>,
    ): Promise<void> => {
      const { jobId } = requireValidatedParams(res);
      const updates = requireValidatedBody(res);
      const job = await jobsService.updateJob(jobId, updates);

      res.status(200).json(successResponse(presentJob(job)));
    },

    list: async (
      _req: Request,
      res: Response<any, ListJobsLocals>,
    ): Promise<void> => {
      const query = requireValidatedQuery(res);
      const { page, pageSize, skip, limit } = normalizePagination({
        page: query.page,
        pageSize: query.pageSize,
      });

      const filters = {
        ...(typeof query.projectId === "string"
          ? { projectId: query.projectId }
          : {}),
        ...(query.state ? { state: query.state } : {}),
        ...(query.type ? { type: query.type } : {}),
      };

      const [items, totalItems] = await Promise.all([
        jobsService.listJobs({
          ...filters,
          skip,
          limit,
        }),
        jobsService.countJobs(filters),
      ]);

      res.status(200).json(
        successResponse(
          presentPaginatedJobs(
            buildPaginatedResult({
              items,
              page,
              pageSize,
              totalItems,
            }),
          ),
        ),
      );
    },
  };
}

export default createJobsController;
