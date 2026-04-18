import { spawn } from "node:child_process";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { env } from "../../config/env";
import { createLogger } from "../../config/logger";
import type { JobRecord } from "../jobs/job.service";

const logger = createLogger({
  module: "execution",
  component: "docker-runner-service",
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

export type CommandResult = {
  exitCode: number;
  stdout: string;
  stderr: string;
};

export interface WorkspaceServicePort {
  ensureWorkspace(input: {
    projectId: string;
    jobId: string;
    slug?: string;
  }): Promise<{
    workspacePath: string;
  }>;

  getWorkspacePath(input: {
    projectId: string;
    jobId: string;
  }): Promise<string>;

  writeFiles(
    workspacePath: string,
    files: Array<{
      path: string;
      content: string;
    }>,
  ): Promise<void>;
}

export interface LocalRepoServicePort {
  ensureRepo(input: { projectId: string; slug: string }): Promise<{
    repoPath: string;
  }>;

  commitAll(input: {
    repoPath: string;
    message: string;
  }): Promise<string | null>;
}

export interface TemplateRegistryPort {
  getTemplate(name: string): Promise<{
    name: string;
    files: Array<{
      path: string;
      content: string;
    }>;
  }>;
}

export type DockerRunnerServiceDependencies = {
  workspaceService: WorkspaceServicePort;
  localRepoService: LocalRepoServicePort;
  templateRegistry: TemplateRegistryPort;
  dockerBinary?: string;
  nodeImage?: string;
  dockerNetwork?: string;
  buildTimeoutMs?: number;
  runTimeoutMs?: number;
};

export class DockerRunnerService {
  private readonly workspaceService: WorkspaceServicePort;
  private readonly localRepoService: LocalRepoServicePort;
  private readonly templateRegistry: TemplateRegistryPort;

  private readonly dockerBinary: string;
  private readonly nodeImage: string;
  private readonly dockerNetwork: string;
  private readonly buildTimeoutMs: number;
  private readonly runTimeoutMs: number;

  constructor(dependencies: DockerRunnerServiceDependencies) {
    this.workspaceService = dependencies.workspaceService;
    this.localRepoService = dependencies.localRepoService;
    this.templateRegistry = dependencies.templateRegistry;

    this.dockerBinary = dependencies.dockerBinary ?? "docker";
    this.nodeImage = dependencies.nodeImage ?? "node:20-alpine";
    this.dockerNetwork =
      dependencies.dockerNetwork ?? env.execution.dockerNetwork;
    this.buildTimeoutMs =
      dependencies.buildTimeoutMs ?? env.execution.dockerBuildTimeoutMs;
    this.runTimeoutMs =
      dependencies.runTimeoutMs ?? env.execution.dockerRunTimeoutMs;
  }

  /**
   * Creates the initial CRUD scaffold in a job workspace.
   *
   * v1 behavior:
   * - ensures the local repo exists
   * - ensures the workspace exists
   * - loads the base template
   * - writes template files into the workspace
   * - optionally makes an initial local commit
   */
  async scaffoldProject(job: JobRecord): Promise<void> {
    const jobLogger = logger.child({
      jobId: job._id,
      projectId: job.projectId,
      state: job.state,
    });

    const slug = this.inferProjectSlug(job);
    const templateName = this.resolveTemplateName(job);

    jobLogger.info(
      {
        slug,
        templateName,
      },
      "Starting scaffold generation.",
    );

    await this.ensureDockerAvailable();

    const repo = await this.localRepoService.ensureRepo({
      projectId: job.projectId,
      slug,
    });

    const workspace = await this.workspaceService.ensureWorkspace({
      projectId: job.projectId,
      jobId: job._id,
      slug,
    });

    const template = await this.templateRegistry.getTemplate(templateName);

    await this.workspaceService.writeFiles(
      workspace.workspacePath,
      template.files,
    );

    await this.writeOrchestratorMetadata({
      workspacePath: workspace.workspacePath,
      job,
      templateName: template.name,
    });

    await this.safeCommit(
      repo.repoPath,
      `chore(scaffold): initialize ${template.name}`,
    );

    jobLogger.info(
      {
        workspacePath: workspace.workspacePath,
        repoPath: repo.repoPath,
        fileCount: template.files.length,
      },
      "Project scaffold created successfully.",
    );
  }

  /**
   * Runs the basic validation/test pipeline in an isolated Docker container.
   *
   * v1 pipeline:
   * - npm install
   * - npm run lint (if available)
   * - npm run test (if available)
   * - npm run build
   *
   * This uses a Node container mounting the generated workspace.
   */
  async runTests(job: JobRecord): Promise<void> {
    const workspacePath = await this.workspaceService.getWorkspacePath({
      projectId: job.projectId,
      jobId: job._id,
    });

    const imageTag = this.buildImageTag(job);
    const jobLogger = logger.child({
      jobId: job._id,
      projectId: job.projectId,
      workspacePath,
      imageTag,
    });

    jobLogger.info("Starting Docker-based test pipeline.");

    await this.ensureDockerAvailable();

    /**
     * First run dependency install and project scripts inside a disposable Node container.
     * The shell intentionally tolerates missing lint/test scripts in early scaffolds.
     */
    const testCommand = [
      "run",
      "--rm",
      "--network",
      this.dockerNetwork,
      "-v",
      `${workspacePath}:/workspace`,
      "-w",
      "/workspace",
      this.nodeImage,
      "sh",
      "-lc",
      [
        "npm install",
        "npm run lint --if-present",
        "npm run test --if-present",
        "npm run build",
      ].join(" && "),
    ];

    const testResult = await this.runDockerCommand(testCommand, {
      timeoutMs: this.runTimeoutMs,
      logLabel: "docker_test_pipeline",
      job,
    });

    jobLogger.info(
      {
        exitCode: testResult.exitCode,
      },
      "Docker test pipeline completed.",
    );

    /**
     * If a Dockerfile exists in the workspace, also verify that the app image builds.
     */
    const buildResult = await this.runDockerCommand(
      ["build", "-t", imageTag, workspacePath],
      {
        timeoutMs: this.buildTimeoutMs,
        logLabel: "docker_build_image",
        job,
      },
    );

    jobLogger.info(
      {
        exitCode: buildResult.exitCode,
        imageTag,
      },
      "Docker image build completed.",
    );
  }

  async buildImage(job: JobRecord): Promise<{ imageTag: string }> {
    const workspacePath = await this.workspaceService.getWorkspacePath({
      projectId: job.projectId,
      jobId: job._id,
    });

    const imageTag = this.buildImageTag(job);

    await this.ensureDockerAvailable();

    await this.runDockerCommand(["build", "-t", imageTag, workspacePath], {
      timeoutMs: this.buildTimeoutMs,
      logLabel: "docker_build_image",
      job,
    });

    return { imageTag };
  }

  private async writeOrchestratorMetadata(input: {
    workspacePath: string;
    job: JobRecord;
    templateName: string;
  }): Promise<void> {
    const metadataPath = path.join(".orchestrator", "scaffold-metadata.json");

    await this.workspaceService.writeFiles(input.workspacePath, [
      {
        path: metadataPath,
        content: JSON.stringify(
          {
            projectId: input.job.projectId,
            jobId: input.job._id,
            templateName: input.templateName,
            generatedAt: new Date().toISOString(),
            stack: input.job.metadata.stack,
            deployment: input.job.metadata.deployment,
          },
          null,
          2,
        ),
      },
    ]);
  }

  private resolveTemplateName(job: JobRecord): string {
    const { frontend, backend, database } = job.metadata.stack;

    if (frontend === "react" && backend === "node" && database === "mongodb") {
      return "react-node-mongo-crud";
    }

    throw createServiceError({
      message: "No scaffold template is configured for this stack.",
      code: "TEMPLATE_NOT_SUPPORTED",
      statusCode: 400,
      details: {
        stack: job.metadata.stack,
      },
    });
  }

  private inferProjectSlug(job: JobRecord): string {
    const promptSlug = job.prompt
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "-")
      .replace(/^-+|-+$/g, "")
      .slice(0, 50);

    return promptSlug.length > 0
      ? promptSlug
      : `app-${job.projectId.slice(-8)}`;
  }

  private buildImageTag(job: JobRecord): string {
    const safeProject = job.projectId.toLowerCase().replace(/[^a-z0-9]+/g, "");
    const safeJob = job._id.toLowerCase().replace(/[^a-z0-9]+/g, "");

    return `app-factory/${safeProject}:${safeJob || randomUUID().slice(0, 8)}`;
  }

  private async safeCommit(repoPath: string, message: string): Promise<void> {
    try {
      await this.localRepoService.commitAll({
        repoPath,
        message,
      });
    } catch (error) {
      logger.warn(
        {
          err: error,
          repoPath,
        },
        "Initial scaffold commit failed. Continuing without commit.",
      );
    }
  }

  private async ensureDockerAvailable(): Promise<void> {
    try {
      await this.runCommand(
        this.dockerBinary,
        ["version", "--format", "json"],
        {
          timeoutMs: 15_000,
        },
      );
    } catch (error) {
      throw createServiceError({
        message: "Docker is not available or not responding.",
        code: "DOCKER_UNAVAILABLE",
        statusCode: 503,
        details: {
          dockerBinary: this.dockerBinary,
          originalError:
            error instanceof Error ? error.message : "Unknown Docker error",
        },
      });
    }
  }

  private async runDockerCommand(
    args: string[],
    input: {
      timeoutMs: number;
      logLabel: string;
      job: Pick<JobRecord, "_id" | "projectId">;
    },
  ): Promise<CommandResult> {
    const result = await this.runCommand(this.dockerBinary, args, {
      timeoutMs: input.timeoutMs,
    });

    logger.info(
      {
        jobId: input.job._id,
        projectId: input.job.projectId,
        logLabel: input.logLabel,
        exitCode: result.exitCode,
        args,
      },
      "Docker command completed.",
    );

    return result;
  }

  private async runCommand(
    command: string,
    args: string[],
    input: {
      cwd?: string;
      timeoutMs: number;
      envVars?: NodeJS.ProcessEnv;
    },
  ): Promise<CommandResult> {
    return await new Promise<CommandResult>((resolve, reject) => {
      const child = spawn(command, args, {
        cwd: input.cwd,
        env: {
          ...process.env,
          ...input.envVars,
        },
        stdio: ["ignore", "pipe", "pipe"],
      });

      let stdout = "";
      let stderr = "";
      let timedOut = false;

      const timeout = setTimeout(() => {
        timedOut = true;
        child.kill("SIGKILL");
      }, input.timeoutMs);

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
            message: `Failed to start command "${command}".`,
            code: "COMMAND_START_FAILED",
            statusCode: 500,
            details: {
              command,
              args,
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
              message: `Command "${command}" timed out after ${input.timeoutMs}ms.`,
              code: "COMMAND_TIMEOUT",
              statusCode: 504,
              details: {
                command,
                args,
                timeoutMs: input.timeoutMs,
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
              message: `Command "${command}" failed with exit code ${safeExitCode}.`,
              code: "COMMAND_FAILED",
              statusCode: 500,
              details: {
                command,
                args,
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

export default DockerRunnerService;
