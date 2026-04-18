import { z } from "zod";
import { taskIntentSchema, taskStatusSchema } from "./task.schemas";

export const objectIdSchema = z
  .string()
  .trim()
  .regex(/^[a-f\d]{24}$/i, "Must be a valid Mongo ObjectId");

export const taskIdParamsSchema = z.object({
  taskId: objectIdSchema,
});

export const listTasksQuerySchema = z.object({
  page: z.coerce
    .number()
    .int("page must be an integer")
    .min(1, "page must be at least 1")
    .default(1),

  pageSize: z.coerce
    .number()
    .int("pageSize must be an integer")
    .min(1, "pageSize must be at least 1")
    .max(100, "pageSize must be at most 100")
    .default(20),

  jobId: objectIdSchema.optional(),
  projectId: objectIdSchema.optional(),
  milestoneId: objectIdSchema.optional(),
  parentTaskId: objectIdSchema.optional(),
  status: taskStatusSchema.optional(),
  intent: taskIntentSchema.optional(),

  agentId: z
    .string()
    .trim()
    .min(1, "agentId cannot be empty")
    .max(120, "agentId must be at most 120 characters long")
    .optional(),
});

export const listRunnableTasksQuerySchema = z.object({
  jobId: objectIdSchema.optional(),
  milestoneId: objectIdSchema.optional(),

  agentId: z
    .string()
    .trim()
    .min(1, "agentId cannot be empty")
    .max(120, "agentId must be at most 120 characters long")
    .optional(),

  limit: z.coerce
    .number()
    .int("limit must be an integer")
    .min(1, "limit must be at least 1")
    .max(100, "limit must be at most 100")
    .default(25),
});

export const listNextRunnableTaskQuerySchema = z.object({
  jobId: objectIdSchema.optional(),
  milestoneId: objectIdSchema.optional(),

  agentId: z
    .string()
    .trim()
    .min(1, "agentId cannot be empty")
    .max(120, "agentId must be at most 120 characters long")
    .optional(),
});

export type TaskIdParams = z.infer<typeof taskIdParamsSchema>;
export type ListTasksQuery = z.infer<typeof listTasksQuerySchema>;
export type ListRunnableTasksQuery = z.infer<
  typeof listRunnableTasksQuerySchema
>;
export type ListNextRunnableTaskQuery = z.infer<
  typeof listNextRunnableTaskQuerySchema
>;
