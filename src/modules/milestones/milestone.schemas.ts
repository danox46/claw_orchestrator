import { z } from "zod";

export const milestoneStatusSchema = z.enum([
  "draft",
  "planned",
  "ready",
  "in_progress",
  "blocked",
  "review",
  "completed",
  "cancelled",
]);

export const createMilestoneSchema = z.object({
  projectId: z.string().trim().min(1, "projectId is required"),

  title: z
    .string()
    .trim()
    .min(1, "title is required")
    .max(255, "title must be at most 255 characters long"),

  description: z
    .string()
    .trim()
    .min(1, "description cannot be empty")
    .max(4000, "description must be at most 4000 characters long")
    .optional(),

  order: z
    .number()
    .int("order must be an integer")
    .nonnegative("order must be greater than or equal to 0"),

  status: milestoneStatusSchema.default("draft"),

  goal: z
    .string()
    .trim()
    .min(1, "goal cannot be empty")
    .max(4000, "goal must be at most 4000 characters long")
    .optional(),

  scope: z.array(z.string().trim().min(1)).default([]),

  acceptanceCriteria: z.array(z.string().trim().min(1)).default([]),

  dependsOnMilestoneId: z
    .string()
    .trim()
    .min(1, "dependsOnMilestoneId cannot be empty")
    .optional(),

  startedAt: z.coerce.date().optional(),
  completedAt: z.coerce.date().optional(),
  confirmedAt: z.coerce.date().optional(),
});

export const updateMilestoneSchema = z
  .object({
    title: z
      .string()
      .trim()
      .min(1, "title cannot be empty")
      .max(255, "title must be at most 255 characters long")
      .optional(),

    description: z
      .string()
      .trim()
      .min(1, "description cannot be empty")
      .max(4000, "description must be at most 4000 characters long")
      .nullable()
      .optional(),

    order: z
      .number()
      .int("order must be an integer")
      .nonnegative("order must be greater than or equal to 0")
      .optional(),

    status: milestoneStatusSchema.optional(),

    goal: z
      .string()
      .trim()
      .min(1, "goal cannot be empty")
      .max(4000, "goal must be at most 4000 characters long")
      .nullable()
      .optional(),

    scope: z.array(z.string().trim().min(1)).optional(),

    acceptanceCriteria: z.array(z.string().trim().min(1)).optional(),

    dependsOnMilestoneId: z
      .string()
      .trim()
      .min(1, "dependsOnMilestoneId cannot be empty")
      .nullable()
      .optional(),

    startedAt: z.coerce.date().nullable().optional(),
    completedAt: z.coerce.date().nullable().optional(),
    confirmedAt: z.coerce.date().nullable().optional(),
  })
  .refine((value) => Object.keys(value).length > 0, {
    message: "At least one field must be provided to update a milestone",
  });

export const milestoneResultSchema = z.object({
  milestoneId: z.string().trim().min(1),
  status: milestoneStatusSchema,
  summary: z.string().trim().min(1),
});

export type MilestoneStatus = z.infer<typeof milestoneStatusSchema>;
export type CreateMilestoneInput = z.infer<typeof createMilestoneSchema>;
export type UpdateMilestoneInput = z.infer<typeof updateMilestoneSchema>;
export type MilestoneResult = z.infer<typeof milestoneResultSchema>;
