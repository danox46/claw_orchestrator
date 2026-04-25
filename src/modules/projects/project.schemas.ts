import { z } from "zod";
import { appTypeSchema, stackSchema } from "../intake/intake.schemas";

export const projectStatusSchema = z.enum([
  "active",
  "archived",
  "ready_for_review",
]);

export const repoModeSchema = z.enum(["local", "github"]);

export const projectIdParamsSchema = z.object({
  projectId: z.string().trim().min(1, "projectId is required"),
});

export const projectSlugParamsSchema = z.object({
  slug: z
    .string()
    .trim()
    .min(3, "slug must be at least 3 characters long")
    .max(160, "slug must be at most 160 characters long")
    .regex(
      /^[a-z0-9]+(?:-[a-z0-9]+)*$/,
      "slug must contain only lowercase letters, numbers, and hyphens",
    ),
});

export const createProjectSchema = z.object({
  name: z
    .string()
    .trim()
    .min(3, "Project name must be at least 3 characters long")
    .max(120, "Project name must be at most 120 characters long"),

  slug: z
    .string()
    .trim()
    .toLowerCase()
    .min(3, "slug must be at least 3 characters long")
    .max(160, "slug must be at most 160 characters long")
    .regex(
      /^[a-z0-9]+(?:-[a-z0-9]+)*$/,
      "slug must contain only lowercase letters, numbers, and hyphens",
    ),

  appType: appTypeSchema.default("internal-crud"),

  stack: stackSchema.default({
    frontend: "react",
    backend: "node",
    database: "mongodb",
  }),

  repoMode: repoModeSchema.default("local"),

  repoUrl: z
    .string()
    .trim()
    .min(1, "repoUrl cannot be empty")
    .max(500, "repoUrl must be at most 500 characters long")
    .optional(),

  canonicalProjectRoot: z
    .string()
    .trim()
    .min(1, "canonicalProjectRoot cannot be empty")
    .max(1000, "canonicalProjectRoot must be at most 1000 characters long"),
});

export const updateProjectSchema = z
  .object({
    name: z
      .string()
      .trim()
      .min(3, "Project name must be at least 3 characters long")
      .max(120, "Project name must be at most 120 characters long")
      .optional(),

    repoMode: repoModeSchema.optional(),

    repoUrl: z
      .string()
      .trim()
      .min(1, "repoUrl cannot be empty")
      .max(500, "repoUrl must be at most 500 characters long")
      .optional(),

    canonicalProjectRoot: z
      .string()
      .trim()
      .min(1, "canonicalProjectRoot cannot be empty")
      .max(1000, "canonicalProjectRoot must be at most 1000 characters long")
      .optional(),

    status: projectStatusSchema.optional(),
  })
  .refine((value) => Object.keys(value).length > 0, {
    message: "At least one field must be provided for update",
  });

export const listProjectsQuerySchema = z.object({
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

  status: projectStatusSchema.optional(),
  appType: appTypeSchema.optional(),
});

export type ProjectStatus = z.infer<typeof projectStatusSchema>;
export type RepoMode = z.infer<typeof repoModeSchema>;

export type ProjectIdParams = z.infer<typeof projectIdParamsSchema>;
export type ProjectSlugParams = z.infer<typeof projectSlugParamsSchema>;
export type CreateProjectRequest = z.infer<typeof createProjectSchema>;
export type UpdateProjectRequest = z.infer<typeof updateProjectSchema>;
export type ListProjectsQuery = z.infer<typeof listProjectsQuerySchema>;
