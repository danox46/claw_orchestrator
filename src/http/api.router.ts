import { Router, type Request, type Response } from "express";
import { createIntakeRoutes } from "../modules/intake/intake.routes";
import type { IntakeServicePort } from "../modules/intake/intake.service";
import { createJobsRoutes } from "../modules/jobs/job.routes";
import type { JobsServicePort } from "../modules/jobs/job.service";
import { createTasksRoutes } from "../modules/tasks/task.routes";
import type { TasksServicePort } from "../modules/tasks/task.service";
import { ROUTE_PATHS } from "./route-paths";
import { MilestonesServicePort } from "../modules/milestones/milestone.service";

export type ApiRouterDependencies = {
  intakeService: IntakeServicePort;
  jobsService: JobsServicePort;
  tasksService: TasksServicePort;
  milestonesService: MilestonesServicePort;
};

export function createApiRouter(dependencies: ApiRouterDependencies): Router {
  const router = Router();

  router.get(ROUTE_PATHS.root, (_req: Request, res: Response) => {
    res.status(200).json({
      ok: true,
      message: "API router is up.",
      availableModules: {
        intake: ROUTE_PATHS.intake,
        jobs: ROUTE_PATHS.jobs,
        tasks: ROUTE_PATHS.tasks,
        artifacts: ROUTE_PATHS.artifacts,
      },
      timestamp: new Date().toISOString(),
    });
  });

  router.get(ROUTE_PATHS.health, (_req: Request, res: Response) => {
    res.status(200).json({
      ok: true,
      scope: "api",
      status: "healthy",
      timestamp: new Date().toISOString(),
    });
  });

  router.use(
    ROUTE_PATHS.intake,
    createIntakeRoutes(dependencies.intakeService),
  );

  router.use(ROUTE_PATHS.jobs, createJobsRoutes(dependencies.jobsService));

  router.use(ROUTE_PATHS.tasks, createTasksRoutes(dependencies.tasksService));

  return router;
}

export default createApiRouter;
