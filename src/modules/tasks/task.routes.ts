import { Router } from "express";
import { asyncHandler } from "../../http/middleware/async-handler";
import { validateRequest } from "../../http/middleware/validate-requests";
import { createTasksController } from "./task.controller";
import { createTaskSchema, updateTaskSchema } from "./task.schemas";
import type { TasksServicePort } from "./task.service";
import { listTasksQuerySchema, taskIdParamsSchema } from "./task.query.schemas";

export function createTasksRoutes(tasksService: TasksServicePort): Router {
  const router = Router();
  const controller = createTasksController(tasksService);

  router.get(
    "/",
    validateRequest({
      query: listTasksQuerySchema,
    }),
    asyncHandler(controller.list),
  );

  router.get(
    "/:taskId",
    validateRequest({
      params: taskIdParamsSchema,
    }),
    asyncHandler(controller.getById),
  );

  router.post(
    "/",
    validateRequest({
      body: createTaskSchema,
    }),
    asyncHandler(controller.create),
  );

  router.patch(
    "/:taskId",
    validateRequest({
      params: taskIdParamsSchema,
      body: updateTaskSchema,
    }),
    asyncHandler(controller.update),
  );

  return router;
}

export const createTasksRouter = createTasksRoutes;

export default createTasksRoutes;
