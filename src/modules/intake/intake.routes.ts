import { Router } from "express";
import { asyncHandler } from "../../http/middleware/async-handler";
import { validateRequest } from "../../http/middleware/validate-requests";
import { createIntakeController } from "./intake.controller";
import { createIntakeRequestSchema } from "./intake.schemas";
import type { IntakeServicePort } from "./intake.service";

export function createIntakeRoutes(intakeService: IntakeServicePort): Router {
  const router = Router();
  const controller = createIntakeController(intakeService);

  router.get("/health", controller.health);

  router.post(
    "/",
    validateRequest({
      body: createIntakeRequestSchema,
    }),
    asyncHandler(controller.create),
  );

  return router;
}

export const createIntakeRouter = createIntakeRoutes;

export default createIntakeRoutes;
