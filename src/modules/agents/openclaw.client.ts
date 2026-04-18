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

const SUPPORTED_PLANNED_TASK_INTENTS = [
  "design_architecture",
  "generate_scaffold",
  "implement_feature",
  "run_tests",
  "review_security",
  "prepare_staging",
] as const;

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

  constructor(httpClient?: AxiosInstance, resultParser?: AgentResultParser) {
    this.http =
      httpClient ??
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

    this.resultParser = resultParser ?? new AgentResultParser();
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
          input: this.buildResponsesPrompt(input.payload),
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

  private buildResponsesPrompt(payload: OpenClawTaskPayload): string {
    switch (payload.intent) {
      case "plan_project_phases":
        return this.buildProjectPhasePlanningPrompt(payload);
      case "plan_phase_tasks":
      case "plan_next_tasks":
        return this.buildMilestoneTaskPlanningPrompt(payload);
      default:
        return this.buildStandardExecutionPrompt(payload);
    }
  }

  private buildProjectPhasePlanningPrompt(
    payload: OpenClawTaskPayload,
  ): string {
    const projectRequest = this.extractProjectRequest(payload.inputs);
    const projectName = this.readOptionalString(payload.inputs, [
      "projectName",
    ]);
    const appType = this.readOptionalString(payload.inputs, ["appType"]);
    const stack = this.asRecord(payload.inputs.stack);
    const deployment = this.asRecord(payload.inputs.deployment);

    const contextLines: string[] = [];

    if (projectName) {
      contextLines.push(`- Project name: ${projectName}`);
    }

    if (appType) {
      contextLines.push(`- App type: ${appType}`);
    }

    if (
      typeof stack.frontend === "string" &&
      stack.frontend.trim().length > 0
    ) {
      contextLines.push(`- Frontend: ${stack.frontend}`);
    }

    if (typeof stack.backend === "string" && stack.backend.trim().length > 0) {
      contextLines.push(`- Backend: ${stack.backend}`);
    }

    if (
      typeof stack.database === "string" &&
      stack.database.trim().length > 0
    ) {
      contextLines.push(`- Database: ${stack.database}`);
    }

    if (
      typeof deployment.target === "string" &&
      deployment.target.trim().length > 0
    ) {
      contextLines.push(`- Deployment target: ${deployment.target}`);
    }

    if (
      typeof deployment.environment === "string" &&
      deployment.environment.trim().length > 0
    ) {
      contextLines.push(`- Environment: ${deployment.environment}`);
    }

    return [
      "Return only valid JSON.",
      "",
      `Use this exact envelope: {"taskId":"${payload.taskId}","status":"succeeded|failed","summary":"","outputs":{},"artifacts":[],"errors":[]}`,
      "",
      ...this.buildRetryContextSection(payload),
      ...(this.hasRetryContext(payload) ? [""] : []),
      "You are planning the ordered project phases for a software project.",
      "",
      "Your job:",
      "- define the milestone/phase plan for this project",
      "- return phases only",
      "- do not create execution tasks yet",
      "- keep the phases practical, ordered, and implementation-ready",
      "",
      "Project request:",
      projectRequest ?? "No explicit project request was provided.",
      "",
      ...(contextLines.length > 0
        ? ["Relevant context:", ...contextLines, ""]
        : []),
      "Planning requirements:",
      "- outputs.phases must be a non-empty ordered array",
      "- phases must be sequential and practical",
      "- each phase must have phaseId, name, goal, description, dependsOn, deliverables, and exitCriteria",
      "- do not create tasks",
      "- do not include unrelated phases",
      "- make the phases ready for downstream task planning",
      "",
      "Phase format:",
      JSON.stringify(
        {
          phaseId: "phase-1",
          name: "Foundation",
          goal: "Short goal for this milestone",
          description: "Short description",
          dependsOn: [],
          deliverables: ["deliverable-1"],
          exitCriteria: ["criterion-1"],
        },
        null,
        2,
      ),
    ].join("\n");
  }

  private buildMilestoneTaskPlanningPrompt(
    payload: OpenClawTaskPayload,
  ): string {
    const projectName = this.readOptionalString(payload.inputs, [
      "projectName",
    ]);
    const milestoneProjectRequest = this.extractMilestoneProjectRequest(
      payload.inputs,
    );
    const phaseName = this.readOptionalString(payload.inputs, ["phaseName"]);
    const phaseGoal = this.readOptionalString(payload.inputs, ["phaseGoal"]);
    const phaseRecord = this.asRecord(payload.inputs.phase);
    const phaseDescription =
      this.readOptionalString(phaseRecord, ["description"]) ??
      this.readOptionalString(payload.inputs, ["phaseDescription"]);

    const contextLines: string[] = [];

    if (projectName) {
      contextLines.push(`- Project name: ${projectName}`);
    }

    if (phaseName) {
      contextLines.push(`- Milestone: ${phaseName}`);
    }

    if (phaseGoal) {
      contextLines.push(`- Milestone goal: ${phaseGoal}`);
    }

    if (phaseDescription) {
      contextLines.push(`- Milestone description: ${phaseDescription}`);
    }

    return [
      "Return only valid JSON.",
      "",
      `Use this exact envelope: {"taskId":"${payload.taskId}","status":"succeeded|failed","summary":"","outputs":{},"artifacts":[],"errors":[]}`,
      "",
      ...this.buildRetryContextSection(payload),
      ...(this.hasRetryContext(payload) ? [""] : []),
      "You are planning the executable tasks for one milestone.",
      "",
      "Your job:",
      "- break the milestone into ordered executable tasks",
      "- include dependencies only where needed",
      "- every task must include acceptanceCriteria",
      "- every task must include inputs.testingCriteria for QA review",
      "- do not plan outside the current milestone",
      "- do not return phases or milestone definitions",
      "- do not repeat the project-level planning step",
      "",
      ...(milestoneProjectRequest
        ? ["Original project request:", milestoneProjectRequest, ""]
        : []),
      ...(contextLines.length > 0
        ? ["Relevant context:", ...contextLines, ""]
        : []),
      "Planning requirements:",
      "- outputs.tasks must be a non-empty ordered array",
      `- use only supported intents: ${SUPPORTED_PLANNED_TASK_INTENTS.join(", ")}`,
      "- each task must include localId",
      "- each task must include inputs.prompt",
      "- each task must include inputs.testingCriteria as a non-empty array",
      "- each task must include acceptanceCriteria as a non-empty array",
      "- use dependsOn with prior localId values when needed",
      "- produce only executable tasks for this milestone based on the milestone context above",
      "",
      "Task format:",
      JSON.stringify(
        {
          localId: "task-1",
          intent: "implement_feature",
          target: {
            agentId: "implementer",
          },
          inputs: {
            prompt: "Implement the requested milestone work.",
            testingCriteria: ["expected behavior 1", "expected behavior 2"],
          },
          constraints: {
            toolProfile: "implementer-safe",
            sandbox: "non-main",
          },
          requiredArtifacts: [],
          acceptanceCriteria: ["criterion-1"],
          dependsOn: [],
        },
        null,
        2,
      ),
    ].join("\n");
  }

  private hasRetryContext(payload: OpenClawTaskPayload): boolean {
    return (
      (Array.isArray(payload.errors) && payload.errors.length > 0) ||
      (typeof payload.lastError === "string" &&
        payload.lastError.trim().length > 0) ||
      typeof payload.attemptNumber === "number" ||
      (this.isRecord(payload.outputs) &&
        Object.keys(payload.outputs).length > 0) ||
      (Array.isArray(payload.artifacts) && payload.artifacts.length > 0)
    );
  }

  private buildRetryContextSection(payload: OpenClawTaskPayload): string[] {
    if (!this.hasRetryContext(payload)) {
      return [];
    }

    const lines: string[] = [
      "Retry context:",
      "- This task may be a retry. Use the previous attempt context below to avoid repeating the same failure.",
    ];

    if (typeof payload.attemptNumber === "number") {
      if (typeof payload.maxAttempts === "number") {
        lines.push(
          `- Current attempt: ${payload.attemptNumber} of ${payload.maxAttempts}.`,
        );
      } else {
        lines.push(`- Current attempt: ${payload.attemptNumber}.`);
      }
    }

    const previousErrors = this.normalizeStringList(payload.errors);
    const lastError =
      typeof payload.lastError === "string" ? payload.lastError.trim() : "";

    if (lastError.length > 0) {
      lines.push(`- Last error: ${lastError}`);
    }

    if (previousErrors.length > 0) {
      lines.push("- Previous error messages:");
      lines.push(...previousErrors.map((message) => `  - ${message}`));
    }

    const previousOutputs = this.isRecord(payload.outputs)
      ? payload.outputs
      : undefined;

    if (previousOutputs && Object.keys(previousOutputs).length > 0) {
      lines.push("- Previous outputs:");
      lines.push(this.toPrettyJson(previousOutputs, 2));
    }

    const previousArtifacts = this.normalizeStringList(payload.artifacts);

    if (previousArtifacts.length > 0) {
      lines.push("- Previous artifacts:");
      lines.push(...previousArtifacts.map((artifact) => `  - ${artifact}`));
    }

    lines.push(
      "- Fix the underlying issue from the prior attempt. Do not ignore the previous errors.",
    );

    return lines;
  }

  private buildStandardExecutionPrompt(payload: OpenClawTaskPayload): string {
    const authoredPrompt = this.extractAuthoredPrompt(payload.inputs);
    const acceptanceCriteria =
      payload.acceptanceCriteria?.filter((item) => item.trim().length > 0) ??
      [];
    const testingCriteria = this.extractTestingCriteria(payload.inputs);

    const sections: string[] = [
      "Return only valid JSON.",
      "",
      "Always keep in mind that the project path is /home/danox/.openclaw/workspace-shared",
      "",
      `Use this exact envelope: {"taskId":"${payload.taskId}","status":"succeeded|failed","summary":"","outputs":{},"artifacts":[],"errors":[]}`,
      "",
      ...this.buildRetryContextSection(payload),
      ...(this.hasRetryContext(payload) ? [""] : []),
      "Complete the assigned task described below.",
      "",
      "Task instruction:",
      authoredPrompt ?? "No explicit task instruction was provided.",
    ];

    if (acceptanceCriteria.length > 0) {
      sections.push(
        "",
        "Acceptance criteria:",
        ...acceptanceCriteria.map((criterion) => `- ${criterion}`),
      );
    }

    if (testingCriteria.length > 0) {
      sections.push(
        "",
        "Testing criteria for QA review:",
        ...testingCriteria.map((criterion) => `- ${criterion}`),
      );
    }

    sections.push(
      "",
      "Output rules:",
      "- put structured result details in outputs",
      "- include artifact references only if something was actually created",
      '- if the task cannot be completed, set status to "failed" and explain why in errors',
    );

    return sections.join("\n");
  }

  private extractProjectRequest(
    inputs: Record<string, unknown>,
  ): string | undefined {
    const dedicatedKeys = [
      "projectRequest",
      "request",
      "userRequest",
      "productRequest",
      "spec",
      "brief",
    ];

    for (const key of dedicatedKeys) {
      const value = inputs[key];

      if (typeof value === "string" && value.trim().length > 0) {
        return value.trim();
      }
    }

    return this.extractAuthoredPrompt(inputs);
  }

  private extractMilestoneProjectRequest(
    inputs: Record<string, unknown>,
  ): string | undefined {
    const safeKeys = [
      "projectRequest",
      "request",
      "userRequest",
      "productRequest",
      "spec",
      "brief",
    ];

    for (const key of safeKeys) {
      const value = inputs[key];

      if (typeof value === "string" && value.trim().length > 0) {
        return value.trim();
      }
    }

    return undefined;
  }

  private extractAuthoredPrompt(
    inputs: Record<string, unknown>,
  ): string | undefined {
    const promptKeys = [
      "prompt",
      "taskPrompt",
      "instruction",
      "instructions",
      "ownerPrompt",
      "pmPrompt",
      "phasePrompt",
      "implementationPrompt",
      "qaPrompt",
      "description",
      "summary",
    ];

    for (const key of promptKeys) {
      const value = inputs[key];

      if (typeof value === "string" && value.trim().length > 0) {
        return value.trim();
      }
    }

    return undefined;
  }

  private extractTestingCriteria(inputs: Record<string, unknown>): string[] {
    const direct = inputs.testingCriteria;

    if (Array.isArray(direct)) {
      return direct
        .filter((item): item is string => typeof item === "string")
        .map((item) => item.trim())
        .filter((item) => item.length > 0);
    }

    return [];
  }

  private normalizeStringList(value: unknown): string[] {
    if (!Array.isArray(value)) {
      return [];
    }

    return value
      .filter((item): item is string => typeof item === "string")
      .map((item) => item.trim())
      .filter((item) => item.length > 0);
  }

  private toPrettyJson(value: unknown, indent = 0): string {
    const normalizedIndent =
      Number.isInteger(indent) && indent > 0 ? indent : 0;
    const prefix = " ".repeat(normalizedIndent);

    try {
      const json = JSON.stringify(value, null, 2);
      return json
        .split("\n")
        .map((line) => `${prefix}${line}`)
        .join(",");
    } catch {
      return `${prefix}${String(value)}`;
    }
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
