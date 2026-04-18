import type { Request, Response } from "express";
import { successResponse } from "../../shared/http/api-response";
import {
  buildPaginatedResult,
  normalizePagination,
} from "../../shared/http/pagination";
import { ServiceError } from "../../shared/errors/service-error";
import type { ProjectsServicePort } from "./project.service";
import type {
  CreateProjectRequest,
  ListProjectsQuery,
  ProjectIdParams,
  ProjectSlugParams,
  UpdateProjectRequest,
} from "./project.schemas";

type CreateProjectLocals = {
  validated?: {
    body?: CreateProjectRequest;
  };
};

type GetProjectByIdLocals = {
  validated?: {
    params?: ProjectIdParams;
  };
};

type GetProjectBySlugLocals = {
  validated?: {
    params?: ProjectSlugParams;
  };
};

type UpdateProjectLocals = {
  validated?: {
    params?: ProjectIdParams;
    body?: UpdateProjectRequest;
  };
};

type ListProjectsLocals = {
  validated?: {
    query?: ListProjectsQuery;
  };
};

export type ProjectsController = {
  create: (
    _req: Request,
    res: Response<any, CreateProjectLocals>,
  ) => Promise<void>;
  getById: (
    _req: Request,
    res: Response<any, GetProjectByIdLocals>,
  ) => Promise<void>;
  getBySlug: (
    _req: Request,
    res: Response<any, GetProjectBySlugLocals>,
  ) => Promise<void>;
  update: (
    _req: Request,
    res: Response<any, UpdateProjectLocals>,
  ) => Promise<void>;
  list: (
    _req: Request,
    res: Response<any, ListProjectsLocals>,
  ) => Promise<void>;
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

export function createProjectsController(
  projectService: ProjectsServicePort,
): ProjectsController {
  return {
    create: async (
      _req: Request,
      res: Response<any, CreateProjectLocals>,
    ): Promise<void> => {
      const input = requireValidatedBody(res);
      const project = await projectService.createProject(input);

      res.status(201).json(successResponse(project));
    },

    getById: async (
      _req: Request,
      res: Response<any, GetProjectByIdLocals>,
    ): Promise<void> => {
      const { projectId } = requireValidatedParams(res);
      const project = await projectService.requireProjectById(projectId);

      res.status(200).json(successResponse(project));
    },

    getBySlug: async (
      _req: Request,
      res: Response<any, GetProjectBySlugLocals>,
    ): Promise<void> => {
      const { slug } = requireValidatedParams(res);
      const project = await projectService.requireProjectBySlug(slug);

      res.status(200).json(successResponse(project));
    },

    update: async (
      _req: Request,
      res: Response<any, UpdateProjectLocals>,
    ): Promise<void> => {
      const { projectId } = requireValidatedParams(res);
      const updates = requireValidatedBody(res);
      const project = await projectService.updateProject(projectId, updates);

      res.status(200).json(successResponse(project));
    },

    list: async (
      _req: Request,
      res: Response<any, ListProjectsLocals>,
    ): Promise<void> => {
      const query = requireValidatedQuery(res);
      const { page, pageSize, skip, limit } = normalizePagination({
        page: query.page,
        pageSize: query.pageSize,
      });

      const filters = {
        ...(query.status ? { status: query.status } : {}),
        ...(query.appType ? { appType: query.appType } : {}),
      };

      const [items, totalItems] = await Promise.all([
        projectService.listProjects({
          ...filters,
          skip,
          limit,
        }),
        projectService.countProjects(filters),
      ]);

      res.status(200).json(
        successResponse(
          buildPaginatedResult({
            items,
            page,
            pageSize,
            totalItems,
          }),
        ),
      );
    },
  };
}

export default createProjectsController;
