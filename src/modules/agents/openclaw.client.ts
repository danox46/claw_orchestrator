import axios, {
  AxiosError,
  type AxiosInstance,
  type AxiosRequestConfig,
} from "axios";
import { randomUUID } from "node:crypto";
import { env } from "../../config/env";
import { createLogger } from "../../config/logger";
import { AgentResultParser } from "./agent-result.parser";

const logger = createLogger({
  module: "agents",
  component: "openclaw-client",
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

const OPENCLAW_ROUTES = {
  health: "/health",
  toolsInvoke: "/tools/invoke",
  responses: "/v1/responses",
} as const;

export type OpenClawSessionRecord = {
  sessionId: string;
  agentId: string;
  status?: string;
  createdAt?: string;
  metadata?: Record<string, unknown>;
};

export type OpenClawCreateSessionInput = {
  agentId: string;
  title?: string;
  metadata?: Record<string, unknown>;
};

export type OpenClawTaskPayload = {
  projectId: string;
  jobId: string;
  milestoneId: string;
  taskId: string;
  intent: string;
  inputs: Record<string, unknown>;
  constraints?: {
    toolProfile?: string;
    sandbox?: "off" | "non-main" | "all";
    maxTokens?: number;
    maxCost?: number;
  };
  requiredArtifacts?: string[];
  acceptanceCriteria?: string[];
  idempotencyKey?: string;
  attemptNumber?: number;
  maxAttempts?: number;
  errors?: string[];
  lastError?: string;
  outputs?: Record<string, unknown>;
  artifacts?: string[];
};

export type OpenClawSendTaskInput = {
  agentId: string;
  sessionId?: string;
  payload: OpenClawTaskPayload;
};

export type OpenClawTaskResult = {
  openclawTaskId?: string;
  sessionId?: string;
  agentId: string;
  status: "queued" | "running" | "qa" | "succeeded" | "failed" | "canceled";
  summary?: string;
  outputs?: Record<string, unknown>;
  artifacts?: string[];
  errors?: string[];
  raw?: unknown;
};

export type OpenClawTaskStatusResponse = {
  openclawTaskId?: string;
  sessionId?: string;
  agentId?: string;
  status: "queued" | "running" | "qa" | "succeeded" | "failed" | "canceled";
  summary?: string;
  outputs?: Record<string, unknown>;
  artifacts?: string[];
  errors?: string[];
  raw?: unknown;
};

export type OpenClawWaitForTaskInput = {
  openclawTaskId: string;
  pollIntervalMs?: number;
  timeoutMs?: number;
};

export type OpenClawPromptBuildInput = {
  agentId: string;
  payload: OpenClawTaskPayload;
};

export interface OpenClawPromptServicePort {
  buildResponsesPrompt(input: OpenClawPromptBuildInput): string;
}

type OpenClawPlanningPromptServicePort = OpenClawPromptServicePort & {
  buildProjectPhasePlanningPromptFromPayload?: (
    input: OpenClawPromptBuildInput,
  ) => string;
  buildMilestoneTaskPlanningPromptFromPayload?: (
    input: OpenClawPromptBuildInput,
  ) => string;
};

export type OpenClawClientDependencies = {
  promptService: OpenClawPromptServicePort;
  httpClient?: AxiosInstance;
  resultParser?: AgentResultParser;
};

type ToolsInvokeResponse<T = unknown> = {
  ok?: boolean;
  result?: T;
  error?: {
    type?: string;
    message?: string;
  };
};

export class OpenClawClient {
  private readonly http: AxiosInstance;
  private readonly resultParser: AgentResultParser;
  private readonly promptService: OpenClawPromptServicePort;

  constructor(dependencies: OpenClawClientDependencies) {
    this.promptService = dependencies.promptService;

    this.http =
      dependencies.httpClient ??
      axios.create({
        baseURL: env.openclaw.baseUrl,
        timeout: env.openclaw.timeoutMs,
        headers: {
          "Content-Type": "application/json",
          Accept: "application/json",
          ...(env.openclaw.apiKey
            ? {
                Authorization: `Bearer ${env.openclaw.apiKey}`,
              }
            : {}),
        },
      });

    this.resultParser = dependencies.resultParser ?? new AgentResultParser();
  }

  async healthcheck(): Promise<{
    ok: boolean;
    baseUrl: string;
    statusCode: number;
  }> {
    try {
      const response = await this.http.get(OPENCLAW_ROUTES.health);

      logger.debug(
        {
          statusCode: response.status,
        },
        "OpenClaw healthcheck succeeded.",
      );

      return {
        ok: true,
        baseUrl: env.openclaw.baseUrl,
        statusCode: response.status,
      };
    } catch (error) {
      throw this.toServiceError(error, {
        operation: "healthcheck",
      });
    }
  }

  async createSession(
    input: OpenClawCreateSessionInput,
  ): Promise<OpenClawSessionRecord> {
    const sessionId = this.buildAdhocSessionKey(input);

    return {
      sessionId,
      agentId: input.agentId,
      status: "virtual",
      metadata: {
        ...(input.metadata ? { ...input.metadata } : {}),
        createdBy: env.app.name,
      },
    };
  }

  async getSession(sessionId: string): Promise<OpenClawSessionRecord> {
    const sessions = await this.listSessions();

    const matched = sessions.find((session) => session.sessionId === sessionId);

    if (!matched) {
      throw createServiceError({
        message: `Session not found: ${sessionId}`,
        code: "OPENCLAW_SESSION_NOT_FOUND",
        statusCode: 404,
        details: {
          sessionId,
        },
      });
    }

    return matched;
  }

  async ensureSession(
    agentId: string,
    sessionId?: string,
    payload?: OpenClawTaskPayload,
  ): Promise<string> {
    if (typeof sessionId === "string" && sessionId.trim().length > 0) {
      return sessionId.trim();
    }

    if (payload) {
      return this.buildTaskSessionKey(agentId, payload);
    }

    const session = await this.createSession({
      agentId,
      metadata: {
        createdBy: env.app.name,
      },
    });

    return session.sessionId;
  }

  async sendTask(input: OpenClawSendTaskInput): Promise<OpenClawTaskResult> {
    const sessionId = await this.ensureSession(
      input.agentId,
      input.sessionId,
      input.payload,
    );

    try {
      const response = await this.http.post(
        OPENCLAW_ROUTES.responses,
        {
          model: input.agentId ? `openclaw/${input.agentId}` : "openclaw",
          user: sessionId,
          input: this.buildPromptForPayload({
            agentId: input.agentId,
            payload: input.payload,
          }),
        },
        {
          headers: {},
        },
      );

      const data = this.asRecord(response.data);
      const openclawTaskId = this.readOptionalString(data, [
        "id",
        "response_id",
        "openclawTaskId",
        "taskId",
      ]);

      const responseText = this.extractResponseText(data);

      const parsed = this.resultParser.parse({
        raw: this.coerceAgentResultPayload(responseText, response.data),
        fallbackTaskId: input.payload.taskId,
        fallbackStatus: "succeeded",
        fallbackSummary:
          responseText ?? "Task completed successfully through OpenClaw.",
      });

      const result: OpenClawTaskResult = {
        ...(openclawTaskId ? { openclawTaskId } : {}),
        sessionId,
        agentId: input.agentId,
        status: parsed.status,
        summary: parsed.summary,
        ...(parsed.outputs ? { outputs: parsed.outputs } : {}),
        ...(parsed.artifacts.length > 0 ? { artifacts: parsed.artifacts } : {}),
        ...(parsed.errors.length > 0 ? { errors: parsed.errors } : {}),
        raw: response.data,
      };

      logger.info(
        {
          agentId: result.agentId,
          sessionId: result.sessionId,
          ...(result.openclawTaskId
            ? { openclawTaskId: result.openclawTaskId }
            : {}),
          status: result.status,
          intent: input.payload.intent,
          orchestratorTaskId: input.payload.taskId,
          milestoneId: input.payload.milestoneId,
        },
        "Task dispatched to OpenClaw via /v1/responses.",
      );

      return result;
    } catch (error) {
      throw this.toServiceError(error, {
        operation: "sendTask",
        details: {
          agentId: input.agentId,
          sessionId,
          intent: input.payload.intent,
          orchestratorTaskId: input.payload.taskId,
          milestoneId: input.payload.milestoneId,
        },
      });
    }
  }

  async getTaskStatus(
    openclawTaskId: string,
  ): Promise<OpenClawTaskStatusResponse> {
    throw createServiceError({
      message:
        "getTaskStatus is not supported in the /v1/responses adapter. Use the immediate sendTask result instead.",
      code: "OPENCLAW_TASK_STATUS_UNSUPPORTED",
      statusCode: 501,
      details: {
        openclawTaskId,
      },
    });
  }

  async waitForTaskCompletion(
    input: OpenClawWaitForTaskInput,
  ): Promise<OpenClawTaskStatusResponse> {
    throw createServiceError({
      message:
        "waitForTaskCompletion is not supported in the /v1/responses adapter. Responses are handled synchronously.",
      code: "OPENCLAW_TASK_WAIT_UNSUPPORTED",
      statusCode: 501,
      details: {
        openclawTaskId: input.openclawTaskId,
      },
    });
  }

  private buildPromptForPayload(input: OpenClawPromptBuildInput): string {
    const intent = this.readIntent(input.payload.intent);

    if (intent === "plan_project_phases") {
      return (
        this.invokeSpecificPlanningPrompt(
          "buildProjectPhasePlanningPromptFromPayload",
          input,
        ) ?? this.promptService.buildResponsesPrompt(input)
      );
    }

    if (intent === "plan_phase_tasks") {
      return (
        this.invokeSpecificPlanningPrompt(
          "buildMilestoneTaskPlanningPromptFromPayload",
          input,
        ) ?? this.promptService.buildResponsesPrompt(input)
      );
    }

    return this.promptService.buildResponsesPrompt(input);
  }

  private invokeSpecificPlanningPrompt(
    methodName:
      | "buildProjectPhasePlanningPromptFromPayload"
      | "buildMilestoneTaskPlanningPromptFromPayload",
    input: OpenClawPromptBuildInput,
  ): string | undefined {
    const planningPromptService = this
      .promptService as unknown as OpenClawPlanningPromptServicePort;
    const candidate = planningPromptService[methodName];

    if (typeof candidate !== "function") {
      return undefined;
    }

    try {
      return candidate.call(this.promptService, input);
    } catch (error) {
      logger.warn(
        {
          methodName,
          intent: input.payload.intent,
          agentId: input.agentId,
          error:
            error instanceof Error
              ? {
                  name: error.name,
                  message: error.message,
                }
              : error,
        },
        "Specific planning prompt builder failed. Falling back to generic prompt routing.",
      );

      return undefined;
    }
  }

  private readIntent(intent: string | undefined): string | undefined {
    if (typeof intent !== "string") {
      return undefined;
    }

    const trimmed = intent.trim();

    return trimmed.length > 0 ? trimmed : undefined;
  }

  async request<TResponse = unknown>(
    config: AxiosRequestConfig,
  ): Promise<TResponse> {
    try {
      const response = await this.http.request<TResponse>(config);
      return response.data;
    } catch (error) {
      throw this.toServiceError(error, {
        operation: "request",
        details: {
          method: config.method,
          url: config.url,
        },
      });
    }
  }

  async listSessions(): Promise<OpenClawSessionRecord[]> {
    const result = await this.invokeTool<unknown>({
      tool: "sessions_list",
      action: "json",
      args: {},
    });

    const items = this.unwrapSessionList(result);

    return items.map((item) => {
      const record = this.asRecord(item);

      return {
        sessionId:
          this.readOptionalString(record, ["sessionKey", "key", "id"]) ??
          randomUUID(),
        agentId:
          this.readOptionalString(record, ["agentId", "agent"]) ?? "unknown",
        ...(typeof record.status === "string" ? { status: record.status } : {}),
        ...(typeof record.createdAt === "string"
          ? { createdAt: record.createdAt }
          : {}),
        ...(this.isRecord(record.metadata)
          ? { metadata: record.metadata }
          : {}),
      };
    });
  }

  async invokeTool<TResponse = unknown>(input: {
    tool: string;
    action?: string;
    args?: Record<string, unknown>;
    sessionKey?: string;
    dryRun?: boolean;
  }): Promise<TResponse> {
    try {
      const response = await this.http.post<ToolsInvokeResponse<TResponse>>(
        OPENCLAW_ROUTES.toolsInvoke,
        {
          tool: input.tool,
          ...(typeof input.action === "string" ? { action: input.action } : {}),
          ...(input.args ? { args: input.args } : {}),
          ...(typeof input.sessionKey === "string"
            ? { sessionKey: input.sessionKey }
            : {}),
          ...(typeof input.dryRun === "boolean"
            ? { dryRun: input.dryRun }
            : {}),
        },
      );

      const data = this.asRecord(
        response.data,
      ) as ToolsInvokeResponse<TResponse>;

      if (data.ok === false) {
        throw createServiceError({
          message:
            data.error?.message ??
            `Tool invocation failed for "${input.tool}".`,
          code: "OPENCLAW_TOOL_INVOKE_FAILED",
          statusCode: 502,
          details: {
            tool: input.tool,
            error: data.error,
          },
        });
      }

      return (data.result ?? response.data) as TResponse;
    } catch (error) {
      throw this.toServiceError(error, {
        operation: "invokeTool",
        details: {
          tool: input.tool,
          sessionKey: input.sessionKey,
        },
      });
    }
  }

  private buildTaskSessionKey(
    agentId: string,
    payload: OpenClawTaskPayload,
  ): string {
    return `orchestrator:${env.app.name}:agent:${agentId}:task:${payload.taskId}`;
  }

  private buildAdhocSessionKey(input: OpenClawCreateSessionInput): string {
    const titlePart =
      typeof input.title === "string" && input.title.trim().length > 0
        ? this.slugify(input.title)
        : "session";

    return `orchestrator:${env.app.name}:agent:${input.agentId}:${titlePart}:${randomUUID()}`;
  }

  private extractResponseText(
    source: Record<string, unknown>,
  ): string | undefined {
    const direct = this.readOptionalString(source, [
      "output_text",
      "text",
      "summary",
      "message",
    ]);

    if (direct) {
      return direct;
    }

    const output = source.output;

    if (Array.isArray(output)) {
      const parts: string[] = [];

      for (const item of output) {
        const itemRecord = this.asRecord(item);

        const itemText = this.readOptionalString(itemRecord, ["text"]);
        if (itemText) {
          parts.push(itemText);
        }

        const content = itemRecord.content;
        if (Array.isArray(content)) {
          for (const part of content) {
            const partRecord = this.asRecord(part);
            const text =
              this.readOptionalString(partRecord, ["text"]) ??
              this.readOptionalString(this.asRecord(partRecord.text), [
                "value",
              ]);

            if (text) {
              parts.push(text);
            }
          }
        }
      }

      if (parts.length > 0) {
        return parts.join("\n").trim();
      }
    }

    return undefined;
  }

  private coerceAgentResultPayload(
    responseText: string | undefined,
    rawResponse: unknown,
  ): unknown {
    if (typeof responseText === "string" && responseText.trim().length > 0) {
      const parsedJson = this.tryParseJsonObject(responseText);

      if (parsedJson) {
        return parsedJson;
      }

      return {
        summary: responseText.trim(),
      };
    }

    return rawResponse;
  }

  private tryParseJsonObject(value: string): Record<string, unknown> | null {
    const trimmed = value.trim();

    try {
      const parsed = JSON.parse(trimmed);

      return this.isRecord(parsed) ? parsed : null;
    } catch {
      // continue
    }

    const firstBrace = trimmed.indexOf("{");
    const lastBrace = trimmed.lastIndexOf("}");

    if (firstBrace >= 0 && lastBrace > firstBrace) {
      const candidate = trimmed.slice(firstBrace, lastBrace + 1);

      try {
        const parsed = JSON.parse(candidate);
        return this.isRecord(parsed) ? parsed : null;
      } catch {
        return null;
      }
    }

    return null;
  }

  private unwrapSessionList(result: unknown): unknown[] {
    if (Array.isArray(result)) {
      return result;
    }

    const record = this.asRecord(result);

    if (Array.isArray(record.sessions)) {
      return record.sessions;
    }

    if (Array.isArray(record.items)) {
      return record.items;
    }

    return [];
  }

  private toServiceError(
    error: unknown,
    context: {
      operation: string;
      details?: unknown;
    },
  ): ServiceError {
    if (axios.isAxiosError(error)) {
      const axiosError = error as AxiosError<unknown>;
      const statusCode = axiosError.response?.status ?? 502;
      const responseData = axiosError.response?.data;

      logger.error(
        {
          operation: context.operation,
          statusCode,
          details: context.details,
          responseData,
          err: error,
        },
        "OpenClaw HTTP request failed.",
      );

      return createServiceError({
        message:
          this.extractAxiosMessage(axiosError) ??
          `OpenClaw request failed during ${context.operation}.`,
        code: "OPENCLAW_REQUEST_FAILED",
        statusCode,
        details: {
          ...this.asRecord(context.details),
          responseData,
        },
      });
    }

    const message =
      error instanceof Error
        ? error.message
        : `Unknown OpenClaw client error during ${context.operation}.`;

    logger.error(
      {
        operation: context.operation,
        details: context.details,
        err: error,
      },
      "OpenClaw client error.",
    );

    return createServiceError({
      message,
      code: "OPENCLAW_CLIENT_ERROR",
      statusCode: 500,
      details: context.details,
    });
  }

  private extractAxiosMessage(error: AxiosError<unknown>): string | null {
    const data = error.response?.data;

    if (this.isRecord(data)) {
      if (typeof data.message === "string") {
        return data.message;
      }

      if (this.isRecord(data.error) && typeof data.error.message === "string") {
        return data.error.message;
      }
    }

    if (typeof data === "string" && data.trim().length > 0) {
      return data.trim();
    }

    return error.message ?? null;
  }

  private readOptionalString(
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

  private isRecord(value: unknown): value is Record<string, unknown> {
    return typeof value === "object" && value !== null && !Array.isArray(value);
  }

  private asRecord(value: unknown): Record<string, unknown> {
    return this.isRecord(value) ? value : {};
  }

  private slugify(value: string): string {
    const slug = value
      .trim()
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "-")
      .replace(/^-+|-+$/g, "")
      .replace(/-{2,}/g, "-");

    return slug.length > 0 ? slug : "session";
  }
}

export default OpenClawClient;
