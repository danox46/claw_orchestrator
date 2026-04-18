import { Router } from "express";
import { asyncHandler } from "../../http/middleware/async-handler";
import { validateRequest } from "../../http/middleware/validate-requests";
import { createJobsController } from "./job.controller";
import {
  createJobSchema,
  jobIdParamsSchema,
  listJobsQuerySchema,
  updateJobSchema,
} from "./job.schemas";
import type { JobsServicePort } from "./job.service";

export function createJobsRoutes(jobsService: JobsServicePort): Router {
  const router = Router();
  const controller = createJobsController(jobsService);

  router.get(
    "/",
    validateRequest({
      query: listJobsQuerySchema,
    }),
    asyncHandler(controller.list),
  );

  router.get(
    "/:jobId",
    validateRequest({
      params: jobIdParamsSchema,
    }),
    asyncHandler(controller.getById),
  );

  router.post(
    "/",
    validateRequest({
      body: createJobSchema,
    }),
    asyncHandler(controller.create),
  );

  router.patch(
    "/:jobId",
    validateRequest({
      params: jobIdParamsSchema,
      body: updateJobSchema,
    }),
    asyncHandler(controller.update),
  );

  return router;
}

export const createJobsRouter = createJobsRoutes;

export default createJobsRoutes;
