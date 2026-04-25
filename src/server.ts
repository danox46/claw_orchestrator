import mongoose from "mongoose";
import { env } from "./config/env";
import { createApp } from "./app";
import { IntakeService } from "./modules/intake/intake.service";
import { JobRunner } from "./modules/jobs/job-runner";
import { JobService } from "./modules/jobs/job.service";
import { StateMachine } from "./modules/jobs/state-machine";
import { MilestoneService } from "./modules/milestones/milestone.service";
import { ProjectService } from "./modules/projects/project.service";
import { TaskService } from "./modules/tasks/task.service";
import { PromptService } from "./modules/prompts/prompt.service";
import OpenClawClient from "./modules/agents/openclaw.client";
import AgentDispatchService from "./modules/agents/agent-dispatch.service";

async function connectToMongo(): Promise<void> {
  await mongoose.connect(env.database.mongoUri, {
    dbName: env.database.mongoDbName,
    serverSelectionTimeoutMS: 5_000,
  });

  console.log(
    JSON.stringify({
      level: "info",
      event: "mongo_connected",
      mongoUri: env.database.mongoUri,
      readyState: mongoose.connection.readyState,
      dbName: mongoose.connection.name,
      timestamp: new Date().toISOString(),
    }),
  );
}

async function bootstrap(): Promise<void> {
  await connectToMongo();

  const projectsService = new ProjectService();
  const jobsService = new JobService();
  const milestonesService = new MilestoneService();
  const tasksService = new TaskService();
  const promptService = new PromptService();

  const intakeService = new IntakeService({
    promptService,
    projectsService,
    jobsService,
    milestonesService,
    tasksService,
  });

  const stateMachine = new StateMachine();
  const openClawClient = new OpenClawClient({ promptService });

  const agentDispatchService = new AgentDispatchService({
    openClawClient,
    tasksService,
    jobsService,
    projectsService,
    milestonesService,
  });

  const jobRunner = new JobRunner({
    jobsService,
    stateMachine,
    agentDispatchService,
    milestonesService,
    tasksService,
    executionService: {
      async scaffoldProject(job) {
        console.log(
          JSON.stringify({
            level: "info",
            event: "job_scaffold_requested",
            jobId: job._id,
            projectId: job.projectId,
            state: job.state,
            timestamp: new Date().toISOString(),
          }),
        );
      },
      async runTests(job) {
        console.log(
          JSON.stringify({
            level: "info",
            event: "job_tests_requested",
            jobId: job._id,
            projectId: job.projectId,
            state: job.state,
            timestamp: new Date().toISOString(),
          }),
        );
      },
    },
    stagingDeployService: {
      async deploy(job) {
        console.log(
          JSON.stringify({
            level: "info",
            event: "job_staging_deploy_requested",
            jobId: job._id,
            projectId: job.projectId,
            state: job.state,
            timestamp: new Date().toISOString(),
          }),
        );
      },
    },
    pollIntervalMs: 3_000,
    maxJobsPerTick: 10,
  });

  const app = createApp({
    intakeService,
    jobsService,
    milestonesService,
    tasksService,
  });

  const server = app.listen(env.server.port, env.server.host, () => {
    console.log(
      JSON.stringify({
        level: "info",
        event: "server_started",
        service: env.app.name,
        host: env.server.host,
        port: env.server.port,
        timestamp: new Date().toISOString(),
      }),
    );

    jobRunner.start();
  });

  server.on("error", (error: NodeJS.ErrnoException) => {
    switch (error.code) {
      case "EACCES":
        console.error(
          JSON.stringify({
            level: "error",
            event: "server_listen_failed",
            message: `Port ${env.server.port} requires elevated privileges.`,
            code: error.code,
            timestamp: new Date().toISOString(),
          }),
        );
        process.exit(1);
        return;

      case "EADDRINUSE":
        console.error(
          JSON.stringify({
            level: "error",
            event: "server_listen_failed",
            message: `Port ${env.server.port} is already in use.`,
            code: error.code,
            timestamp: new Date().toISOString(),
          }),
        );
        process.exit(1);
        return;

      default:
        console.error(
          JSON.stringify({
            level: "error",
            event: "server_listen_failed",
            message: error.message,
            ...(typeof error.code === "string" ? { code: error.code } : {}),
            ...(typeof error.stack === "string" ? { stack: error.stack } : {}),
            timestamp: new Date().toISOString(),
          }),
        );
        process.exit(1);
    }
  });

  let isShuttingDown = false;

  const shutdown = async (signal: NodeJS.Signals) => {
    if (isShuttingDown) {
      return;
    }

    isShuttingDown = true;

    console.log(
      JSON.stringify({
        level: "info",
        event: "server_shutdown_started",
        signal,
        timestamp: new Date().toISOString(),
      }),
    );

    try {
      await jobRunner.stop();

      await new Promise<void>((resolve, reject) => {
        server.close((error?: Error) => {
          if (error) {
            reject(error);
            return;
          }

          resolve();
        });
      });

      await mongoose.disconnect();

      console.log(
        JSON.stringify({
          level: "info",
          event: "server_shutdown_completed",
          signal,
          timestamp: new Date().toISOString(),
        }),
      );

      process.exit(0);
    } catch (error) {
      console.error(
        JSON.stringify({
          level: "error",
          event: "server_shutdown_failed",
          signal,
          message:
            error instanceof Error ? error.message : "Unknown shutdown error",
          ...(error instanceof Error && typeof error.stack === "string"
            ? { stack: error.stack }
            : {}),
          timestamp: new Date().toISOString(),
        }),
      );

      process.exit(1);
    }
  };

  process.on("SIGINT", () => {
    void shutdown("SIGINT");
  });

  process.on("SIGTERM", () => {
    void shutdown("SIGTERM");
  });
}

void bootstrap().catch((error: unknown) => {
  console.error(
    JSON.stringify({
      level: "error",
      event: "bootstrap_failed",
      message:
        error instanceof Error ? error.message : "Unknown bootstrap error",
      ...(error instanceof Error && typeof error.stack === "string"
        ? { stack: error.stack }
        : {}),
      timestamp: new Date().toISOString(),
    }),
  );

  process.exit(1);
});
