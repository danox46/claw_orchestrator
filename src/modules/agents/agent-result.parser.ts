import { createLogger } from "../../config/logger";
import { taskResultSchema, type TaskResult } from "../tasks/task.schemas";

const logger = createLogger({
  module: "agents",
  component: "agent-result-parser",
});

type ServiceError = Error & {
  statusCode?: number;
  code?: string;
  details?: unknown;
};

function createServiceError(input: {
  message: string;
  code: string;
  statusCode: number;
  details?: unknown;
}): ServiceError {
  return Object.assign(new Error(input.message), {
    code: input.code,
    statusCode: input.statusCode,
    details: input.details,
  });
}

export type ParsedAgentResult = TaskResult & {
  openclawTaskId?: string;
  sessionId?: string;
  agentId?: string;
  raw: unknown;
};

export type ParseAgentResultInput = {
  raw: unknown;
  fallbackTaskId?: string;
  fallbackStatus?: TaskResult["status"];
  fallbackSummary?: string;
};

export class AgentResultParser {
  parse(input: ParseAgentResultInput): ParsedAgentResult {
    const rawRecord = this.unwrapToRecord(input.raw);

    const taskId =
      this.readString(rawRecord, [
        "taskId",
        "orchestratorTaskId",
        "clientTaskId",
      ]) ?? input.fallbackTaskId;

    if (!taskId) {
      throw createServiceError({
        message: "Agent result is missing taskId.",
        code: "AGENT_RESULT_TASK_ID_MISSING",
        statusCode: 502,
        details: {
          raw: input.raw,
        },
      });
    }

    const status =
      this.readStatus(rawRecord, ["status", "state", "resultStatus"]) ??
      input.fallbackStatus ??
      "succeeded";

    const errors = this.readErrors(rawRecord);
    const summary =
      this.readSummary(rawRecord) ??
      input.fallbackSummary ??
      this.buildDefaultSummary(status, errors);

    const outputs = this.readOutputs(rawRecord);
    const artifacts = this.readArtifacts(rawRecord);

    const parsed = taskResultSchema.safeParse({
      taskId,
      status,
      summary,
      ...(outputs ? { outputs } : {}),
      artifacts,
      errors,
    });

    if (!parsed.success) {
      logger.error(
        {
          issues: parsed.error.flatten(),
          raw: input.raw,
        },
        "Failed to validate parsed agent result.",
      );

      throw createServiceError({
        message: "Parsed agent result failed validation.",
        code: "AGENT_RESULT_INVALID",
        statusCode: 502,
        details: {
          issues: parsed.error.flatten(),
        },
      });
    }

    const openclawTaskId = this.readString(rawRecord, [
      "openclawTaskId",
      "remoteTaskId",
    ]);
    const sessionId = this.readString(rawRecord, ["sessionId", "sessionName"]);
    const agentId = this.readString(rawRecord, ["agentId"]);

    return {
      ...parsed.data,
      ...(openclawTaskId ? { openclawTaskId } : {}),
      ...(sessionId ? { sessionId } : {}),
      ...(agentId ? { agentId } : {}),
      raw: input.raw,
    };
  }

  private unwrapToRecord(value: unknown): Record<string, unknown> {
    const parsedValue = this.coerceToRecord(value);

    if (!parsedValue) {
      throw createServiceError({
        message: "Agent result payload must be an object.",
        code: "AGENT_RESULT_NOT_AN_OBJECT",
        statusCode: 502,
        details: {
          rawType: typeof value,
        },
      });
    }

    const outputTextRecord = this.readJsonRecordString(parsedValue.output_text);

    if (outputTextRecord) {
      return {
        ...parsedValue,
        ...outputTextRecord,
      };
    }

    for (const key of ["payload", "result", "data", "raw"] as const) {
      const nested = this.coerceToRecord(parsedValue[key]);

      if (nested) {
        return {
          ...parsedValue,
          ...nested,
        };
      }
    }

    return parsedValue;
  }

  private readSummary(source: Record<string, unknown>): string | undefined {
    const directSummary = this.readString(source, [
      "summary",
      "message",
      "resultMessage",
    ]);

    if (directSummary) {
      return directSummary;
    }

    if (this.isRecord(source.error)) {
      return this.readString(source.error, ["message"]);
    }

    return undefined;
  }

  private readOutputs(
    source: Record<string, unknown>,
  ): Record<string, unknown> | undefined {
    const directOutputs = this.readRecord(source, ["outputs"]);

    if (directOutputs) {
      return directOutputs;
    }

    for (const key of ["data", "result", "output"] as const) {
      const candidate = this.readRecord(source, [key]);

      if (candidate && !this.looksLikeAgentEnvelope(candidate)) {
        return candidate;
      }
    }

    return undefined;
  }

  private readArtifacts(source: Record<string, unknown>): string[] {
    const candidates = [
      source.artifacts,
      source.files,
      source.generatedFiles,
      source.createdFiles,
    ];

    for (const candidate of candidates) {
      const artifacts = this.normalizeStringArray(candidate, [
        "path",
        "name",
        "url",
        "id",
        "artifact",
      ]);

      if (artifacts.length > 0) {
        return artifacts;
      }
    }

    return [];
  }

  private readErrors(source: Record<string, unknown>): string[] {
    const explicitErrors = this.normalizeStringArray(source.errors, [
      "message",
      "error",
      "code",
      "reason",
      "details",
    ]);

    if (explicitErrors.length > 0) {
      return explicitErrors;
    }

    if (this.isRecord(source.error)) {
      const message = this.readString(source.error, [
        "message",
        "error",
        "code",
        "reason",
      ]);

      if (message) {
        return [message];
      }
    }

    if (typeof source.error === "string" && source.error.trim().length > 0) {
      return [source.error.trim()];
    }

    const status = this.readStatus(source, ["status", "state", "resultStatus"]);

    if (status === "failed" || status === "canceled") {
      const message = this.readString(source, ["message", "summary"]);

      if (message) {
        return [message];
      }
    }

    return [];
  }

  private readStatus(
    source: Record<string, unknown>,
    keys: string[],
  ): TaskResult["status"] | undefined {
    const value = this.readString(source, keys)?.toLowerCase();

    if (
      value === "queued" ||
      value === "running" ||
      value === "qa" ||
      value === "succeeded" ||
      value === "failed" ||
      value === "canceled"
    ) {
      return value;
    }

    if (value === "success" || value === "completed" || value === "done") {
      return "succeeded";
    }

    if (value === "error" || value === "cancelled") {
      return value === "error" ? "failed" : "canceled";
    }

    return undefined;
  }

  private buildDefaultSummary(
    status: TaskResult["status"],
    errors: string[],
  ): string {
    switch (status) {
      case "queued":
        return "Task queued.";
      case "running":
        return "Task is running.";
      case "qa":
        return "Task is in QA review.";
      case "succeeded":
        return "Task completed successfully.";
      case "failed":
        return errors[0] ?? "Task failed.";
      case "canceled":
        return errors[0] ?? "Task was canceled.";
      default:
        return String(status);
    }
  }

  private readString(
    source: Record<string, unknown>,
    keys: string[],
  ): string | undefined {
    for (const key of keys) {
      const value = source[key];

      if (typeof value === "string" && value.trim().length > 0) {
        return value.trim();
      }
    }

    return undefined;
  }

  private readRecord(
    source: Record<string, unknown>,
    keys: string[],
  ): Record<string, unknown> | undefined {
    for (const key of keys) {
      const value = source[key];

      if (this.isRecord(value)) {
        return value;
      }
    }

    return undefined;
  }

  private normalizeStringArray(value: unknown, objectKeys: string[]): string[] {
    if (!Array.isArray(value)) {
      return [];
    }

    const normalized: string[] = [];

    for (const item of value) {
      if (typeof item === "string" && item.trim().length > 0) {
        normalized.push(item.trim());
        continue;
      }

      if (this.isRecord(item)) {
        const mapped = this.readString(item, objectKeys);

        if (mapped) {
          normalized.push(mapped);
        }
      }
    }

    return normalized;
  }

  private coerceToRecord(value: unknown): Record<string, unknown> | undefined {
    if (this.isRecord(value)) {
      return value;
    }

    return this.readJsonRecordString(value);
  }

  private readJsonRecordString(
    value: unknown,
  ): Record<string, unknown> | undefined {
    if (typeof value !== "string") {
      return undefined;
    }

    const trimmed = value.trim();

    if (trimmed.length === 0) {
      return undefined;
    }

    const jsonText = this.extractJsonObjectText(trimmed);

    if (!jsonText) {
      return undefined;
    }

    try {
      const parsed = JSON.parse(jsonText) as unknown;
      return this.isRecord(parsed) ? parsed : undefined;
    } catch {
      return undefined;
    }
  }

  private extractJsonObjectText(value: string): string | undefined {
    const unfenced = value
      .replace(/^```(?:json)?\s*/i, "")
      .replace(/\s*```$/i, "")
      .trim();

    if (unfenced.startsWith("{") && unfenced.endsWith("}")) {
      return unfenced;
    }

    const start = unfenced.indexOf("{");
    const end = unfenced.lastIndexOf("}");

    if (start >= 0 && end > start) {
      return unfenced.slice(start, end + 1);
    }

    return undefined;
  }

  private looksLikeAgentEnvelope(value: Record<string, unknown>): boolean {
    return [
      "taskId",
      "orchestratorTaskId",
      "clientTaskId",
      "status",
      "state",
      "resultStatus",
      "summary",
      "errors",
      "artifacts",
      "outputs",
    ].some((key) => key in value);
  }

  private isRecord(value: unknown): value is Record<string, unknown> {
    return typeof value === "object" && value !== null && !Array.isArray(value);
  }
}

export default AgentResultParser;
