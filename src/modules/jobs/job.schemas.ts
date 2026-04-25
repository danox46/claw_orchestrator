import { z } from "zod";
import {
  appTypeSchema,
  deploymentSchema,
  stackSchema,
} from "../intake/intake.schemas";

export const objectIdSchema = z
  .string()
  .trim()
  .regex(/^[a-f\d]{24}$/i, "Must be a valid Mongo ObjectId");

export const jobTypeSchema = z.enum(["create-app", "update-app", "deploy"]);

export const jobStateSchema = z.enum([
  "INTAKE",
  "SPEC_READY",
  "ARCH_READY",
  "SCAFFOLD_READY",
  "CODE_READY",
  "TEST_READY",
  "SECURITY_READY",
  "STAGING_READY",
  "DEPLOYED",
  "FAILED",
  "PLAN_READY",
  "COMPLETED",
]);

export const jobMetadataSchema = z.object({
  requestedBy: z
    .string()
    .trim()
    .min(1, "requestedBy cannot be empty")
    .max(120, "requestedBy must be at most 120 characters long")
    .optional(),
  appType: appTypeSchema.default("internal-crud"),
  stack: stackSchema.default({
    frontend: "react",
    backend: "node",
    database: "mongodb",
  }),
  deployment: deploymentSchema.default({
    target: "docker",
    environment: "staging",
  }),
  requestType: z.enum(["fix", "feature", "patch", "cleanup"]).optional(),
  isProjectUpdate: z.boolean().optional(),
  sourceJobId: objectIdSchema.optional(),
  previousSuccessfulJobId: objectIdSchema.optional(),
  canonicalProjectRoot: z
    .string()
    .trim()
    .min(1, "canonicalProjectRoot cannot be empty")
    .max(1000, "canonicalProjectRoot must be at most 1000 characters long")
    .optional(),
});

export const jobIdParamsSchema = z.object({
  jobId: objectIdSchema,
});

export const createJobSchema = z.object({
  projectId: objectIdSchema,
  type: jobTypeSchema,
  state: jobStateSchema,
  prompt: z
    .string()
    .trim()
    .min(1, "prompt cannot be empty")
    .max(10000, "prompt must be at most 10000 characters long"),
  metadata: jobMetadataSchema,
  currentTaskId: objectIdSchema.optional(),
  error: z
    .string()
    .trim()
    .min(1, "error cannot be empty")
    .max(5000, "error must be at most 5000 characters long")
    .optional(),
});

export const updateJobSchema = z
  .object({
    state: jobStateSchema.optional(),
    currentTaskId: objectIdSchema.optional(),
    error: z
      .string()
      .trim()
      .min(1, "error cannot be empty")
      .max(5000, "error must be at most 5000 characters long")
      .optional(),
  })
  .refine((value) => Object.keys(value).length > 0, {
    message: "At least one field must be provided for update",
  });

export const listJobsQuerySchema = z.object({
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

  projectId: objectIdSchema.optional(),
  state: jobStateSchema.optional(),
  type: jobTypeSchema.optional(),
});

export type JobType = z.infer<typeof jobTypeSchema>;
export type JobState = z.infer<typeof jobStateSchema>;
export type JobMetadata = z.infer<typeof jobMetadataSchema>;

export type JobIdParams = z.infer<typeof jobIdParamsSchema>;
export type CreateJobRequest = z.infer<typeof createJobSchema>;
export type UpdateJobRequest = z.infer<typeof updateJobSchema>;
export type ListJobsQuery = z.infer<typeof listJobsQuerySchema>;
