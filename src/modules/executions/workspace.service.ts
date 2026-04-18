import fs from "node:fs/promises";
import path from "node:path";
import { env } from "../../config/env";
import { createLogger } from "../../config/logger";

const logger = createLogger({
  module: "execution",
  component: "workspace-service",
});

type ServiceError = Error & {
  statusCode?: number;
  code?: string;
  details?: unknown;
};

function createServiceError(input: {
  message: string;
  code: string;
  statusCode: number;
  details?: unknown;
}): ServiceError {
  return Object.assign(new Error(input.message), {
    code: input.code,
    statusCode: input.statusCode,
    details: input.details,
  });
}

export type EnsureWorkspaceInput = {
  projectId: string;
  jobId: string;
  slug?: string;
};

export type EnsureWorkspaceResult = {
  workspacePath: string;
};

export type WorkspaceFileInput = {
  path: string;
  content: string;
};

export class WorkspaceService {
  private readonly workspaceRoot: string;

  constructor(workspaceRoot?: string) {
    this.workspaceRoot = path.resolve(
      workspaceRoot ?? env.execution.workspaceRoot,
    );
  }

  async ensureWorkspace(
    input: EnsureWorkspaceInput,
  ): Promise<EnsureWorkspaceResult> {
    const workspacePath = this.buildWorkspacePath(input);

    await fs.mkdir(workspacePath, { recursive: true });

    logger.info(
      {
        projectId: input.projectId,
        jobId: input.jobId,
        workspacePath,
      },
      "Workspace ensured.",
    );

    return {
      workspacePath,
    };
  }

  async getWorkspacePath(input: {
    projectId: string;
    jobId: string;
  }): Promise<string> {
    const workspacePath = this.buildWorkspacePath(input);

    try {
      const stats = await fs.stat(workspacePath);

      if (!stats.isDirectory()) {
        throw createServiceError({
          message: `Workspace path exists but is not a directory: ${workspacePath}`,
          code: "WORKSPACE_NOT_A_DIRECTORY",
          statusCode: 500,
          details: {
            workspacePath,
            projectId: input.projectId,
            jobId: input.jobId,
          },
        });
      }

      return workspacePath;
    } catch (error: unknown) {
      if (this.isNotFoundError(error)) {
        throw createServiceError({
          message: `Workspace not found for project "${input.projectId}" and job "${input.jobId}".`,
          code: "WORKSPACE_NOT_FOUND",
          statusCode: 404,
          details: {
            workspacePath,
            projectId: input.projectId,
            jobId: input.jobId,
          },
        });
      }

      throw error;
    }
  }

  async writeFiles(
    workspacePath: string,
    files: WorkspaceFileInput[],
  ): Promise<void> {
    const resolvedWorkspacePath = path.resolve(workspacePath);
    await this.ensureDirectoryExists(resolvedWorkspacePath);

    for (const file of files) {
      const destinationPath = this.resolveWorkspaceFilePath(
        resolvedWorkspacePath,
        file.path,
      );

      await fs.mkdir(path.dirname(destinationPath), { recursive: true });
      await fs.writeFile(destinationPath, file.content, "utf8");
    }

    logger.info(
      {
        workspacePath: resolvedWorkspacePath,
        fileCount: files.length,
      },
      "Files written to workspace.",
    );
  }

  async readFile(input: {
    projectId: string;
    jobId: string;
    filePath: string;
  }): Promise<string> {
    const workspacePath = await this.getWorkspacePath({
      projectId: input.projectId,
      jobId: input.jobId,
    });

    const targetPath = this.resolveWorkspaceFilePath(
      workspacePath,
      input.filePath,
    );

    try {
      return await fs.readFile(targetPath, "utf8");
    } catch (error: unknown) {
      if (this.isNotFoundError(error)) {
        throw createServiceError({
          message: `File not found in workspace: ${input.filePath}`,
          code: "WORKSPACE_FILE_NOT_FOUND",
          statusCode: 404,
          details: {
            projectId: input.projectId,
            jobId: input.jobId,
            workspacePath,
            filePath: input.filePath,
          },
        });
      }

      throw error;
    }
  }

  async fileExists(input: {
    projectId: string;
    jobId: string;
    filePath: string;
  }): Promise<boolean> {
    const workspacePath = await this.getWorkspacePath({
      projectId: input.projectId,
      jobId: input.jobId,
    });

    const targetPath = this.resolveWorkspaceFilePath(
      workspacePath,
      input.filePath,
    );

    try {
      const stats = await fs.stat(targetPath);
      return stats.isFile();
    } catch (error: unknown) {
      if (this.isNotFoundError(error)) {
        return false;
      }

      throw error;
    }
  }

  async listFiles(input: {
    projectId: string;
    jobId: string;
  }): Promise<string[]> {
    const workspacePath = await this.getWorkspacePath({
      projectId: input.projectId,
      jobId: input.jobId,
    });

    const files = await this.walkDirectory(workspacePath, workspacePath);

    return files.sort();
  }

  async removeWorkspace(input: {
    projectId: string;
    jobId: string;
  }): Promise<void> {
    const workspacePath = this.buildWorkspacePath(input);

    await fs.rm(workspacePath, { recursive: true, force: true });

    logger.info(
      {
        projectId: input.projectId,
        jobId: input.jobId,
        workspacePath,
      },
      "Workspace removed.",
    );
  }

  private buildWorkspacePath(input: {
    projectId: string;
    jobId: string;
  }): string {
    const safeProjectId = this.toSafeSegment(input.projectId);
    const safeJobId = this.toSafeSegment(input.jobId);

    return path.join(this.workspaceRoot, safeProjectId, safeJobId);
  }

  private toSafeSegment(value: string): string {
    const normalized = value.trim().replace(/[^a-zA-Z0-9_-]+/g, "-");

    if (!normalized) {
      throw createServiceError({
        message: "Workspace path segment cannot be empty.",
        code: "INVALID_WORKSPACE_SEGMENT",
        statusCode: 400,
        details: {
          value,
        },
      });
    }

    return normalized;
  }

  private resolveWorkspaceFilePath(
    workspacePath: string,
    relativeFilePath: string,
  ): string {
    const trimmedPath = relativeFilePath.trim();

    if (!trimmedPath) {
      throw createServiceError({
        message: "File path cannot be empty.",
        code: "INVALID_WORKSPACE_FILE_PATH",
        statusCode: 400,
      });
    }

    const normalizedRelativePath = trimmedPath.replace(/\\/g, "/");
    const targetPath = path.resolve(workspacePath, normalizedRelativePath);

    const relativeToWorkspace = path.relative(workspacePath, targetPath);

    if (
      relativeToWorkspace.startsWith("..") ||
      path.isAbsolute(relativeToWorkspace)
    ) {
      throw createServiceError({
        message: `File path escapes workspace boundary: ${relativeFilePath}`,
        code: "WORKSPACE_PATH_TRAVERSAL_BLOCKED",
        statusCode: 400,
        details: {
          workspacePath,
          relativeFilePath,
          resolvedPath: targetPath,
        },
      });
    }

    return targetPath;
  }

  private async ensureDirectoryExists(directoryPath: string): Promise<void> {
    try {
      const stats = await fs.stat(directoryPath);

      if (!stats.isDirectory()) {
        throw createServiceError({
          message: `Expected a directory but found something else: ${directoryPath}`,
          code: "WORKSPACE_NOT_A_DIRECTORY",
          statusCode: 500,
          details: {
            directoryPath,
          },
        });
      }
    } catch (error: unknown) {
      if (this.isNotFoundError(error)) {
        await fs.mkdir(directoryPath, { recursive: true });
        return;
      }

      throw error;
    }
  }

  private async walkDirectory(
    basePath: string,
    currentPath: string,
  ): Promise<string[]> {
    const entries = await fs.readdir(currentPath, { withFileTypes: true });
    const files: string[] = [];

    for (const entry of entries) {
      const fullPath = path.join(currentPath, entry.name);

      if (entry.isDirectory()) {
        files.push(...(await this.walkDirectory(basePath, fullPath)));
        continue;
      }

      if (entry.isFile()) {
        files.push(path.relative(basePath, fullPath).replace(/\\/g, "/"));
      }
    }

    return files;
  }

  private isNotFoundError(error: unknown): boolean {
    return (
      typeof error === "object" &&
      error !== null &&
      "code" in error &&
      (error as { code?: unknown }).code === "ENOENT"
    );
  }
}

export default WorkspaceService;
