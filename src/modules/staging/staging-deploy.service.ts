import { spawn } from "node:child_process";
import path from "node:path";
import { env } from "../../config/env";
import { createLogger } from "../../config/logger";
import type { JobRecord } from "../jobs/job.service";
import { JobState } from "../jobs/job.schemas";

const logger = createLogger({
  module: "staging",
  component: "staging-deploy-service",
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
  getWorkspacePath(input: {
    projectId: string;
    jobId: string;
  }): Promise<string>;
  fileExists(input: {
    projectId: string;
    jobId: string;
    filePath: string;
  }): Promise<boolean>;
  readFile(input: {
    projectId: string;
    jobId: string;
    filePath: string;
  }): Promise<string>;
}

export interface JobsServicePort {
  advanceState(jobId: string, state: JobState): Promise<JobRecord>;
}

export interface ArtifactServicePort {
  upsertArtifact(input: {
    projectId: string;
    jobId: string;
    taskId?: string;
    name: string;
    type:
      | "spec"
      | "acceptance-criteria"
      | "architecture"
      | "data-model"
      | "routes"
      | "implementation-plan"
      | "patchset"
      | "source-file"
      | "test-report"
      | "security-report"
      | "deployment-manifest"
      | "docker-compose"
      | "run-summary"
      | "log"
      | "other";
    format:
      | "json"
      | "md"
      | "txt"
      | "diff"
      | "js"
      | "ts"
      | "tsx"
      | "jsx"
      | "yml"
      | "yaml"
      | "dockerfile"
      | "log"
      | "other";
    storage: "inline" | "filesystem";
    relativePath?: string;
    content?: string;
    status?: "pending" | "ready" | "invalid" | "archived";
    metadata?: {
      mimeType?: string;
      sizeBytes?: number;
      checksum?: string;
      generator?: string;
      sourceTaskIntent?:
        | "draft_spec"
        | "design_architecture"
        | "generate_scaffold"
        | "implement_feature"
        | "run_tests"
        | "review_security"
        | "prepare_staging";
      tags?: string[];
    };
  }): Promise<unknown>;
}

export interface StagingHealthServicePort {
  waitForHealthy(input: { url: string; timeoutMs?: number }): Promise<void>;
}

export type StagingDeployServiceDependencies = {
  workspaceService: WorkspaceServicePort;
  jobsService: JobsServicePort;
  artifactService: ArtifactServicePort;
  stagingHealthService: StagingHealthServicePort;
  dockerBinary?: string;
  dockerComposeFileName?: string;
  composeProjectPrefix?: string;
  deployTimeoutMs?: number;
};

export class StagingDeployService {
  private readonly workspaceService: WorkspaceServicePort;
  private readonly jobsService: JobsServicePort;
  private readonly artifactService: ArtifactServicePort;
  private readonly stagingHealthService: StagingHealthServicePort;

  private readonly dockerBinary: string;
  private readonly dockerComposeFileName: string;
  private readonly composeProjectPrefix: string;
  private readonly deployTimeoutMs: number;

  constructor(dependencies: StagingDeployServiceDependencies) {
    this.workspaceService = dependencies.workspaceService;
    this.jobsService = dependencies.jobsService;
    this.artifactService = dependencies.artifactService;
    this.stagingHealthService = dependencies.stagingHealthService;

    this.dockerBinary = dependencies.dockerBinary ?? "docker";
    this.dockerComposeFileName =
      dependencies.dockerComposeFileName ?? "docker-compose.staging.yml";
    this.composeProjectPrefix =
      dependencies.composeProjectPrefix ?? "app-factory-staging";
    this.deployTimeoutMs =
      dependencies.deployTimeoutMs ?? env.execution.dockerRunTimeoutMs;
  }

  /**
   * Deploys the job workspace to staging using docker compose.
   *
   * Success behavior:
   * - validates the compose file exists
   * - runs `docker compose up -d --build`
   * - runs a staging health check
   * - writes/updates deployment artifacts
   * - advances the job to STAGING_READY
   */
  async deploy(job: JobRecord): Promise<void> {
    const workspacePath = await this.workspaceService.getWorkspacePath({
      projectId: job.projectId,
      jobId: job._id,
    });

    const composeExists = await this.workspaceService.fileExists({
      projectId: job.projectId,
      jobId: job._id,
      filePath: this.dockerComposeFileName,
    });

    if (!composeExists) {
      throw createServiceError({
        message: `Missing staging compose file: ${this.dockerComposeFileName}`,
        code: "STAGING_COMPOSE_FILE_MISSING",
        statusCode: 409,
        details: {
          projectId: job.projectId,
          jobId: job._id,
          workspacePath,
          fileName: this.dockerComposeFileName,
        },
      });
    }

    const composePath = path.join(workspacePath, this.dockerComposeFileName);
    const composeProjectName = this.buildComposeProjectName(job);
    const stagingUrl = this.buildStagingUrl(job);

    const jobLogger = logger.child({
      jobId: job._id,
      projectId: job.projectId,
      workspacePath,
      composePath,
      composeProjectName,
      stagingUrl,
    });

    jobLogger.info("Starting staging deployment.");

    await this.ensureDockerAvailable();

    const composeContent = await this.workspaceService.readFile({
      projectId: job.projectId,
      jobId: job._id,
      filePath: this.dockerComposeFileName,
    });

    const deployResult = await this.runDockerCommand(
      [
        "compose",
        "-p",
        composeProjectName,
        "-f",
        composePath,
        "up",
        "-d",
        "--build",
      ],
      {
        cwd: workspacePath,
        timeoutMs: this.deployTimeoutMs,
        logLabel: "docker_compose_up",
        job,
      },
    );

    await this.stagingHealthService.waitForHealthy({
      url: stagingUrl,
      timeoutMs: env.staging.healthcheckTimeoutMs,
    });

    await this.artifactService.upsertArtifact({
      projectId: job.projectId,
      jobId: job._id,
      name: this.dockerComposeFileName,
      type: "docker-compose",
      format: "yml",
      storage: "filesystem",
      relativePath: this.dockerComposeFileName,
      status: "ready",
      metadata: {
        generator: "staging-deploy-service",
        sourceTaskIntent: "prepare_staging",
        mimeType: "application/yaml",
        sizeBytes: Buffer.byteLength(composeContent, "utf8"),
        tags: ["staging", "deploy", "docker-compose"],
      },
    });

    const runSummary = {
      projectId: job.projectId,
      jobId: job._id,
      deployedAt: new Date().toISOString(),
      environment: "staging",
      provider: "docker-compose",
      workspacePath,
      composeFile: this.dockerComposeFileName,
      composeProjectName,
      stagingUrl,
      stack: job.metadata.stack,
      deployment: job.metadata.deployment,
      command: `${this.dockerBinary} compose -p ${composeProjectName} -f ${this.dockerComposeFileName} up -d --build`,
      logs: {
        stdout: deployResult.stdout,
        stderr: deployResult.stderr,
      },
    };

    await this.artifactService.upsertArtifact({
      projectId: job.projectId,
      jobId: job._id,
      name: "run-summary.json",
      type: "run-summary",
      format: "json",
      storage: "inline",
      content: JSON.stringify(runSummary, null, 2),
      status: "ready",
      metadata: {
        generator: "staging-deploy-service",
        sourceTaskIntent: "prepare_staging",
        mimeType: "application/json",
        sizeBytes: Buffer.byteLength(JSON.stringify(runSummary), "utf8"),
        tags: ["staging", "deploy", "summary"],
      },
    });

    await this.jobsService.advanceState(job._id, "STAGING_READY");

    jobLogger.info("Staging deployment completed successfully.");
  }

  private buildComposeProjectName(
    job: Pick<JobRecord, "_id" | "projectId">,
  ): string {
    const safeProjectId = job.projectId
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "");
    const safeJobId = job._id.toLowerCase().replace(/[^a-z0-9]+/g, "");

    return `${this.composeProjectPrefix}-${safeProjectId.slice(-8)}-${safeJobId.slice(-8)}`;
  }

  private buildStagingUrl(job: Pick<JobRecord, "_id" | "projectId">): string {
    const baseUrl = env.staging.publicBaseUrl.replace(/\/+$/, "");
    const suffix = `${job.projectId.slice(-6)}-${job._id.slice(-6)}`;

    if (baseUrl === "http://localhost" || baseUrl === "https://localhost") {
      return `${baseUrl}/${suffix}`;
    }

    return `${baseUrl}/${suffix}`;
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
      cwd?: string;
      timeoutMs: number;
      logLabel: string;
      job: Pick<JobRecord, "_id" | "projectId">;
    },
  ): Promise<CommandResult> {
    const result = await this.runCommand(this.dockerBinary, args, {
      ...(input.cwd ? { cwd: input.cwd } : {}),
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
              cwd: input.cwd,
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
                cwd: input.cwd,
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
                cwd: input.cwd,
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

export default StagingDeployService;
