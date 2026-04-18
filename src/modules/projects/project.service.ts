import { type QueryFilter } from "mongoose";
import { ConflictError } from "../../shared/errors/conflict-error";
import { NotFoundError } from "../../shared/errors/not-found-error";
import type {
  CreateProjectRequest,
  ProjectStatus,
  RepoMode,
  UpdateProjectRequest,
} from "./project.schemas";
import ProjectModel, { type ProjectModelType } from "./project.model";

export type CreateProjectInput = CreateProjectRequest;

export type ProjectRecord = {
  _id: string;
  name: string;
  slug: string;
  appType: "internal-crud";
  stack: {
    frontend: "react";
    backend: "node";
    database: "mongodb";
  };
  repoMode: "local" | "github";
  repoUrl?: string;
  status: "active" | "archived";
  createdAt: Date;
  updatedAt: Date;
};

export type UpdateProjectInput = UpdateProjectRequest;

export type ListProjectsInput = {
  status?: ProjectStatus;
  appType?: "internal-crud";
  limit?: number;
  skip?: number;
};

export type CountProjectsInput = Pick<ListProjectsInput, "status" | "appType">;

export interface ProjectsServicePort {
  createProject(input: CreateProjectInput): Promise<ProjectRecord>;
  getProjectById(projectId: string): Promise<ProjectRecord | null>;
  getProjectBySlug(slug: string): Promise<ProjectRecord | null>;
  requireProjectById(projectId: string): Promise<ProjectRecord>;
  requireProjectBySlug(slug: string): Promise<ProjectRecord>;
  updateProject(
    projectId: string,
    updates: UpdateProjectInput,
  ): Promise<ProjectRecord>;
  listProjects(input?: ListProjectsInput): Promise<ProjectRecord[]>;
  countProjects(input?: CountProjectsInput): Promise<number>;
}

type ProjectDocumentLike = {
  id: string;
  name: string;
  slug: string;
  appType: "internal-crud";
  stack: {
    frontend: "react";
    backend: "node";
    database: "mongodb";
  };
  repoMode: RepoMode;
  repoUrl?: string | null;
  status: ProjectStatus;
  createdAt: Date;
  updatedAt: Date;
};

function mapProject(document: ProjectDocumentLike): ProjectRecord {
  return {
    _id: document.id,
    name: document.name,
    slug: document.slug,
    appType: document.appType,
    stack: {
      frontend: document.stack.frontend,
      backend: document.stack.backend,
      database: document.stack.database,
    },
    repoMode: document.repoMode,
    ...(document.repoUrl ? { repoUrl: document.repoUrl } : {}),
    status: document.status,
    createdAt: document.createdAt,
    updatedAt: document.updatedAt,
  };
}

export class ProjectService implements ProjectsServicePort {
  async createProject(input: CreateProjectInput): Promise<ProjectRecord> {
    try {
      const created = await ProjectModel.create({
        name: input.name.trim(),
        slug: input.slug.trim().toLowerCase(),
        appType: input.appType,
        stack: input.stack,
        repoMode: input.repoMode,
        ...(typeof input.repoUrl === "string" && input.repoUrl.trim().length > 0
          ? { repoUrl: input.repoUrl.trim() }
          : {}),
      });

      return mapProject(created);
    } catch (error: unknown) {
      if (this.isDuplicateKeyError(error)) {
        throw new ConflictError({
          message: `A project with slug "${input.slug}" already exists.`,
          code: "PROJECT_SLUG_ALREADY_EXISTS",
          details: {
            slug: input.slug,
          },
          cause: error,
        });
      }

      throw error;
    }
  }

  async getProjectById(projectId: string): Promise<ProjectRecord | null> {
    const project = await ProjectModel.findById(projectId).exec();
    return project ? mapProject(project) : null;
  }

  async getProjectBySlug(slug: string): Promise<ProjectRecord | null> {
    const project = await ProjectModel.findOne({
      slug: slug.trim().toLowerCase(),
    }).exec();

    return project ? mapProject(project) : null;
  }

  async requireProjectById(projectId: string): Promise<ProjectRecord> {
    const project = await this.getProjectById(projectId);

    if (!project) {
      throw new NotFoundError({
        message: `Project not found: ${projectId}`,
        code: "PROJECT_NOT_FOUND",
        details: {
          projectId,
        },
      });
    }

    return project;
  }

  async requireProjectBySlug(slug: string): Promise<ProjectRecord> {
    const project = await this.getProjectBySlug(slug);

    if (!project) {
      throw new NotFoundError({
        message: `Project not found for slug "${slug}"`,
        code: "PROJECT_NOT_FOUND",
        details: {
          slug,
        },
      });
    }

    return project;
  }

  async updateProject(
    projectId: string,
    updates: UpdateProjectInput,
  ): Promise<ProjectRecord> {
    const updatePayload: Partial<ProjectModelType> = {};

    if (typeof updates.name === "string") {
      updatePayload.name = updates.name.trim();
    }

    if (typeof updates.repoMode === "string") {
      updatePayload.repoMode = updates.repoMode;
    }

    if (typeof updates.repoUrl === "string") {
      updatePayload.repoUrl = updates.repoUrl.trim();
    }

    if (typeof updates.status === "string") {
      updatePayload.status = updates.status;
    }

    const updated = await ProjectModel.findByIdAndUpdate(
      projectId,
      updatePayload,
      {
        new: true,
        runValidators: true,
      },
    ).exec();

    if (!updated) {
      throw new NotFoundError({
        message: `Project not found: ${projectId}`,
        code: "PROJECT_NOT_FOUND",
        details: {
          projectId,
        },
      });
    }

    return mapProject(updated);
  }

  async listProjects(input: ListProjectsInput = {}): Promise<ProjectRecord[]> {
    const filter = this.buildFilter(input);
    const limit = Math.min(Math.max(input.limit ?? 25, 1), 100);
    const skip = Math.max(input.skip ?? 0, 0);

    const projects = await ProjectModel.find(filter)
      .sort({ updatedAt: -1 })
      .skip(skip)
      .limit(limit)
      .exec();

    return projects.map(mapProject);
  }

  async countProjects(input: CountProjectsInput = {}): Promise<number> {
    const filter = this.buildFilter(input);
    return ProjectModel.countDocuments(filter).exec();
  }

  private buildFilter(
    input: CountProjectsInput | ListProjectsInput,
  ): QueryFilter<ProjectModelType> {
    const filter: QueryFilter<ProjectModelType> = {};

    if (input.status) {
      filter.status = input.status;
    }

    if (input.appType) {
      filter.appType = input.appType;
    }

    return filter;
  }

  private isDuplicateKeyError(error: unknown): boolean {
    if (
      typeof error === "object" &&
      error !== null &&
      "name" in error &&
      "code" in error
    ) {
      const candidate = error as {
        name?: unknown;
        code?: unknown;
      };

      return candidate.name === "MongoServerError" && candidate.code === 11000;
    }

    return false;
  }
}

export default ProjectService;
