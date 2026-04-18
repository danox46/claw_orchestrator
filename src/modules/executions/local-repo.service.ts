import fs from "node:fs/promises";
import path from "node:path";
import { spawn } from "node:child_process";
import { env } from "../../config/env";
import { createLogger } from "../../config/logger";

const logger = createLogger({
  module: "execution",
  component: "local-repo-service",
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

export type EnsureRepoInput = {
  projectId: string;
  slug: string;
};

export type EnsureRepoResult = {
  repoPath: string;
  created: boolean;
};

export class LocalRepoService {
  private readonly repoRoot: string;
  private readonly gitBinary: string;
  private readonly defaultBranch: string;
  private readonly gitUserName: string;
  private readonly gitUserEmail: string;

  constructor(input?: {
    repoRoot?: string;
    gitBinary?: string;
    defaultBranch?: string;
    gitUserName?: string;
    gitUserEmail?: string;
  }) {
    this.repoRoot = path.resolve(input?.repoRoot ?? env.execution.repoRoot);
    this.gitBinary = input?.gitBinary ?? "git";
    this.defaultBranch = input?.defaultBranch ?? "main";
    this.gitUserName =
      input?.gitUserName ??
      process.env.GIT_AUTHOR_NAME?.trim() ??
      "App Factory Orchestrator";
    this.gitUserEmail =
      input?.gitUserEmail ??
      process.env.GIT_AUTHOR_EMAIL?.trim() ??
      "orchestrator@local";
  }

  async ensureRepo(input: EnsureRepoInput): Promise<EnsureRepoResult> {
    const repoPath = this.buildRepoPath(input);

    await fs.mkdir(repoPath, { recursive: true });

    const hasGitDirectory = await this.pathExists(path.join(repoPath, ".git"));

    if (hasGitDirectory) {
      logger.info(
        {
          projectId: input.projectId,
          slug: input.slug,
          repoPath,
        },
        "Local repository already exists.",
      );

      return {
        repoPath,
        created: false,
      };
    }

    await this.ensureGitAvailable();
    await this.initializeRepository(repoPath);

    logger.info(
      {
        projectId: input.projectId,
        slug: input.slug,
        repoPath,
      },
      "Local repository initialized.",
    );

    return {
      repoPath,
      created: true,
    };
  }

  async getRepoPath(input: {
    projectId: string;
    slug: string;
  }): Promise<string> {
    const repoPath = this.buildRepoPath(input);

    if (!(await this.pathExists(path.join(repoPath, ".git")))) {
      throw createServiceError({
        message: `Repository not found for project "${input.projectId}".`,
        code: "LOCAL_REPO_NOT_FOUND",
        statusCode: 404,
        details: {
          projectId: input.projectId,
          slug: input.slug,
          repoPath,
        },
      });
    }

    return repoPath;
  }

  async commitAll(input: {
    repoPath: string;
    message: string;
  }): Promise<string | null> {
    const repoPath = path.resolve(input.repoPath);

    await this.ensureGitAvailable();
    await this.ensureRepoInitialized(repoPath);

    await this.runGit(["add", "-A"], { cwd: repoPath });

    const hasChanges = await this.hasPendingChanges(repoPath);

    if (!hasChanges) {
      logger.info(
        {
          repoPath,
        },
        "No git changes detected. Skipping commit.",
      );

      return null;
    }

    await this.runGit(["commit", "-m", input.message.trim()], {
      cwd: repoPath,
      envVars: {
        GIT_AUTHOR_NAME: this.gitUserName,
        GIT_AUTHOR_EMAIL: this.gitUserEmail,
        GIT_COMMITTER_NAME: this.gitUserName,
        GIT_COMMITTER_EMAIL: this.gitUserEmail,
      },
    });

    const commitHash = await this.getHeadCommitHash(repoPath);

    logger.info(
      {
        repoPath,
        commitHash,
      },
      "Local repository commit created.",
    );

    return commitHash;
  }

  async getHeadCommitHash(repoPath: string): Promise<string> {
    await this.ensureRepoInitialized(repoPath);

    const result = await this.runGit(["rev-parse", "HEAD"], {
      cwd: repoPath,
    });

    return result.stdout.trim();
  }

  private async initializeRepository(repoPath: string): Promise<void> {
    await this.runGit(["init", "-b", this.defaultBranch], {
      cwd: repoPath,
    });

    await this.runGit(["config", "user.name", this.gitUserName], {
      cwd: repoPath,
    });

    await this.runGit(["config", "user.email", this.gitUserEmail], {
      cwd: repoPath,
    });
  }

  private async hasPendingChanges(repoPath: string): Promise<boolean> {
    const result = await this.runGit(["status", "--porcelain"], {
      cwd: repoPath,
    });

    return result.stdout.trim().length > 0;
  }

  private async ensureGitAvailable(): Promise<void> {
    try {
      await this.runGit(["--version"], {
        timeoutMs: 15_000,
      });
    } catch (error) {
      throw createServiceError({
        message: "Git is not available or not responding.",
        code: "GIT_UNAVAILABLE",
        statusCode: 503,
        details: {
          gitBinary: this.gitBinary,
          originalError:
            error instanceof Error ? error.message : "Unknown git error",
        },
      });
    }
  }

  private async ensureRepoInitialized(repoPath: string): Promise<void> {
    const hasGitDirectory = await this.pathExists(path.join(repoPath, ".git"));

    if (!hasGitDirectory) {
      throw createServiceError({
        message: `Directory is not a git repository: ${repoPath}`,
        code: "LOCAL_REPO_NOT_INITIALIZED",
        statusCode: 400,
        details: {
          repoPath,
        },
      });
    }
  }

  private buildRepoPath(input: { projectId: string; slug: string }): string {
    const safeProjectId = this.toSafeSegment(input.projectId);
    const safeSlug = this.toSafeSegment(input.slug);

    return path.join(this.repoRoot, safeProjectId, safeSlug);
  }

  private toSafeSegment(value: string): string {
    const normalized = value.trim().replace(/[^a-zA-Z0-9_-]+/g, "-");

    if (!normalized) {
      throw createServiceError({
        message: "Repository path segment cannot be empty.",
        code: "INVALID_REPO_PATH_SEGMENT",
        statusCode: 400,
        details: {
          value,
        },
      });
    }

    return normalized;
  }

  private async pathExists(targetPath: string): Promise<boolean> {
    try {
      await fs.access(targetPath);
      return true;
    } catch {
      return false;
    }
  }

  private async runGit(
    args: string[],
    input?: {
      cwd?: string;
      timeoutMs?: number;
      envVars?: NodeJS.ProcessEnv;
    },
  ): Promise<{
    exitCode: number;
    stdout: string;
    stderr: string;
  }> {
    return await new Promise((resolve, reject) => {
      const child = spawn(this.gitBinary, args, {
        cwd: input?.cwd,
        env: {
          ...process.env,
          ...input?.envVars,
        },
        stdio: ["ignore", "pipe", "pipe"],
      });

      let stdout = "";
      let stderr = "";
      let timedOut = false;

      const timeout = setTimeout(() => {
        timedOut = true;
        child.kill("SIGKILL");
      }, input?.timeoutMs ?? 30_000);

      child.stdout.on("data", (chunk: Buffer | string) => {
        stdout += chunk.toString();
      });

      child.stderr.on("data", (chunk: Buffer | string) => {
        stderr += chunk.toString();
      });

      child.on("error", (error) => {
        clearTimeout(timeout);

        reject(
          createServiceError({
            message: `Failed to start git command "${this.gitBinary} ${args.join(" ")}".`,
            code: "GIT_COMMAND_START_FAILED",
            statusCode: 500,
            details: {
              gitBinary: this.gitBinary,
              args,
              cwd: input?.cwd,
              error: error.message,
            },
          }),
        );
      });

      child.on("close", (exitCode) => {
        clearTimeout(timeout);

        if (timedOut) {
          reject(
            createServiceError({
              message: `Git command timed out: ${this.gitBinary} ${args.join(" ")}`,
              code: "GIT_COMMAND_TIMEOUT",
              statusCode: 504,
              details: {
                gitBinary: this.gitBinary,
                args,
                cwd: input?.cwd,
                stdout,
                stderr,
              },
            }),
          );
          return;
        }

        const safeExitCode = exitCode ?? 1;

        if (safeExitCode !== 0) {
          reject(
            createServiceError({
              message: `Git command failed with exit code ${safeExitCode}: ${this.gitBinary} ${args.join(" ")}`,
              code: "GIT_COMMAND_FAILED",
              statusCode: 500,
              details: {
                gitBinary: this.gitBinary,
                args,
                cwd: input?.cwd,
                exitCode: safeExitCode,
                stdout,
                stderr,
              },
            }),
          );
          return;
        }

        resolve({
          exitCode: safeExitCode,
          stdout,
          stderr,
        });
      });
    });
  }
}

export default LocalRepoService;
