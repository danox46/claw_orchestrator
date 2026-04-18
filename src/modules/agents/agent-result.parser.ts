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

type ParsedAgentResult = TaskResult & {
  openclawTaskId?: string;
  sessionId?: string;
  agentId?: string;
  raw: unknown;
};

type ParseAgentResultInput = {
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
    const sessionId = this.readString(rawRecord, ["sessionId"]);
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
    if (this.isRecord(value)) {
      if (this.isRecord(value.payload)) {
        return {
          ...value,
          ...value.payload,
        };
      }

      if (this.isRecord(value.result)) {
        return {
          ...value,
          ...value.result,
        };
      }

      if (this.isRecord(value.data)) {
        return {
          ...value,
          ...value.data,
        };
      }

      return value;
    }

    throw createServiceError({
      message: "Agent result payload must be an object.",
      code: "AGENT_RESULT_NOT_AN_OBJECT",
      statusCode: 502,
      details: {
        rawType: typeof value,
      },
    });
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

    const dataOutputs = this.readRecord(source, ["data"]);

    if (dataOutputs) {
      return dataOutputs;
    }

    const resultOutputs = this.readRecord(source, ["result"]);

    if (resultOutputs) {
      return resultOutputs;
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
      const artifacts = this.normalizeStringArray(candidate);

      if (artifacts.length > 0) {
        return artifacts;
      }
    }

    return [];
  }

  private readErrors(source: Record<string, unknown>): string[] {
    const explicitErrors = this.normalizeStringArray(source.errors);

    if (explicitErrors.length > 0) {
      return explicitErrors;
    }

    if (this.isRecord(source.error)) {
      const message = this.readString(source.error, ["message"]);

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
      default: {
        const exhaustiveCheck: never = status;
        return String(exhaustiveCheck);
      }
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

  private normalizeStringArray(value: unknown): string[] {
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
        const mapped = this.readString(item, [
          "path",
          "name",
          "url",
          "id",
          "artifact",
        ]);

        if (mapped) {
          normalized.push(mapped);
        }
      }
    }

    return normalized;
  }

  private isRecord(value: unknown): value is Record<string, unknown> {
    return typeof value === "object" && value !== null && !Array.isArray(value);
  }
}

export default AgentResultParser;
