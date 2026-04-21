import { z } from "zod";

export const taskStatusSchema = z.enum([
  "queued",
  "running",
  "qa",
  "succeeded",
  "failed",
  "canceled",
]);

export const taskIntentSchema = z.enum([
  "draft_spec",
  "design_architecture",
  "generate_scaffold",
  "implement_feature",
  "run_tests",
  "review_security",
  "prepare_staging",
  "plan_project_phases",
  "plan_phase_tasks",
  "plan_next_tasks",
  "review_milestone",
]);

export const sandboxModeSchema = z.enum(["off", "non-main", "all"]);

export const sessionNameSchema = z
  .string()
  .trim()
  .min(1, "sessionName cannot be empty")
  .max(255, "sessionName must be at most 255 characters long");

export const sessionCountSchema = z
  .number()
  .int("sessionCount must be an integer")
  .min(1, "sessionCount must be at least 1");

export const maxSessionsSchema = z
  .number()
  .int("maxSessions must be an integer")
  .min(1, "maxSessions must be at least 1");

export const issuerSchema = z.object({
  kind: z.enum(["system", "agent"]),
  id: z
    .string()
    .trim()
    .min(1, "issuer.id is required")
    .max(120, "issuer.id must be at most 120 characters long"),
  sessionId: z
    .string()
    .trim()
    .min(1, "issuer.sessionId cannot be empty")
    .max(255, "issuer.sessionId must be at most 255 characters long")
    .optional(),
  role: z
    .string()
    .trim()
    .min(1, "issuer.role cannot be empty")
    .max(120, "issuer.role must be at most 120 characters long")
    .optional(),
});

export const targetSchema = z.object({
  agentId: z
    .string()
    .trim()
    .min(1, "target.agentId is required")
    .max(120, "target.agentId must be at most 120 characters long"),
});

export const constraintsSchema = z.object({
  toolProfile: z
    .string()
    .trim()
    .min(1, "constraints.toolProfile is required")
    .max(120, "constraints.toolProfile must be at most 120 characters long"),
  sandbox: sandboxModeSchema,
  maxTokens: z
    .number()
    .int("constraints.maxTokens must be an integer")
    .positive("constraints.maxTokens must be greater than 0")
    .optional(),
  maxCost: z
    .number()
    .nonnegative("constraints.maxCost must be greater than or equal to 0")
    .optional(),
});

export const createTaskSchema = z.object({
  jobId: z.string().trim().min(1, "jobId is required"),
  projectId: z.string().trim().min(1, "projectId is required"),
  milestoneId: z.string().trim().min(1, "milestoneId is required"),

  parentTaskId: z
    .string()
    .trim()
    .min(1, "parentTaskId cannot be empty")
    .optional(),

  dependencies: z.array(z.string().trim().min(1)).default([]),

  issuer: issuerSchema,
  target: targetSchema,
  intent: taskIntentSchema,
  inputs: z.record(z.string(), z.unknown()),
  constraints: constraintsSchema,
  requiredArtifacts: z.array(z.string().trim().min(1)).default([]),
  acceptanceCriteria: z.array(z.string().trim().min(1)).default([]),

  idempotencyKey: z
    .string()
    .trim()
    .min(1, "idempotencyKey is required")
    .max(255, "idempotencyKey must be at most 255 characters long"),

  status: taskStatusSchema.default("queued"),

  attemptCount: z
    .number()
    .int("attemptCount must be an integer")
    .nonnegative("attemptCount must be greater than or equal to 0")
    .default(0),

  maxAttempts: z
    .number()
    .int("maxAttempts must be an integer")
    .positive("maxAttempts must be greater than 0")
    .default(3),

  sessionName: sessionNameSchema.optional(),

  sessionCount: sessionCountSchema.default(1),

  maxSessions: maxSessionsSchema.default(2),

  nextRetryAt: z.coerce.date().optional(),

  lastError: z
    .string()
    .trim()
    .min(1, "lastError cannot be empty")
    .max(4000, "lastError must be at most 4000 characters long")
    .optional(),

  retryable: z.boolean().default(true),

  sequence: z
    .number()
    .int("sequence must be an integer")
    .nonnegative("sequence must be greater than or equal to 0")
    .default(0),

  outputs: z.record(z.string(), z.unknown()).optional(),
  artifacts: z.array(z.string().trim().min(1)).default([]),
  errors: z.array(z.string().trim().min(1)).default([]),
});

export const updateTaskSchema = z
  .object({
    milestoneId: z
      .string()
      .trim()
      .min(1, "milestoneId cannot be empty")
      .optional(),

    parentTaskId: z
      .string()
      .trim()
      .min(1, "parentTaskId cannot be empty")
      .optional(),

    dependencies: z.array(z.string().trim().min(1)).optional(),

    status: taskStatusSchema.optional(),

    attemptCount: z
      .number()
      .int("attemptCount must be an integer")
      .nonnegative("attemptCount must be greater than or equal to 0")
      .optional(),

    maxAttempts: z
      .number()
      .int("maxAttempts must be an integer")
      .positive("maxAttempts must be greater than 0")
      .optional(),

    sessionName: sessionNameSchema.optional(),

    sessionCount: sessionCountSchema.optional(),

    maxSessions: maxSessionsSchema.optional(),

    nextRetryAt: z.coerce.date().optional(),

    lastError: z
      .string()
      .trim()
      .min(1, "lastError cannot be empty")
      .max(4000, "lastError must be at most 4000 characters long")
      .optional(),

    retryable: z.boolean().optional(),

    sequence: z
      .number()
      .int("sequence must be an integer")
      .nonnegative("sequence must be greater than or equal to 0")
      .optional(),

    outputs: z.record(z.string(), z.unknown()).optional(),
    artifacts: z.array(z.string().trim().min(1)).optional(),
    errors: z.array(z.string().trim().min(1)).optional(),
    target: targetSchema.optional(),
  })
  .refine((value) => Object.keys(value).length > 0, {
    message: "At least one field must be provided to update a task",
  });

export const taskResultSchema = z.object({
  taskId: z.string().trim().min(1),
  status: taskStatusSchema,
  summary: z.string().trim().min(1),
  outputs: z.record(z.string(), z.unknown()).optional(),
  artifacts: z.array(z.string().trim().min(1)).default([]),
  errors: z.array(z.string().trim().min(1)).default([]),
});

export type TaskStatus = z.infer<typeof taskStatusSchema>;
export type TaskIntent = z.infer<typeof taskIntentSchema>;
export type SandboxMode = z.infer<typeof sandboxModeSchema>;
export type TaskIssuer = z.infer<typeof issuerSchema>;
export type TaskTarget = z.infer<typeof targetSchema>;
export type TaskConstraints = z.infer<typeof constraintsSchema>;
export type CreateTaskInput = z.infer<typeof createTaskSchema>;
export type UpdateTaskInput = z.infer<typeof updateTaskSchema>;
export type TaskResult = z.infer<typeof taskResultSchema>;
