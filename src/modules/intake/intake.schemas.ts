import { z } from "zod";

export const appTypeSchema = z.literal("internal-crud");

export const stackSchema = z.object({
  frontend: z.literal("react"),
  backend: z.literal("node"),
  database: z.literal("mongodb"),
});

export const deploymentSchema = z.object({
  target: z.literal("docker"),
  environment: z.literal("staging"),
});

export const createIntakeRequestSchema = z.object({
  name: z
    .string()
    .trim()
    .min(3, "Project name must be at least 3 characters long")
    .max(120, "Project name must be at most 120 characters long"),

  prompt: z
    .string()
    .trim()
    .min(20, "Prompt must be at least 20 characters long")
    .max(10000, "Prompt must be at most 10000 characters long"),

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

  requestedBy: z
    .string()
    .trim()
    .min(1, "requestedBy cannot be empty")
    .max(120, "requestedBy must be at most 120 characters long")
    .optional(),
});

export type CreateIntakeRequest = z.infer<typeof createIntakeRequestSchema>;
export type AppType = z.infer<typeof appTypeSchema>;
export type Stack = z.infer<typeof stackSchema>;
export type Deployment = z.infer<typeof deploymentSchema>;
