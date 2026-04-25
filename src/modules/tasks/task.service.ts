import { type ClientSession, type QueryFilter, Types } from "mongoose";
import { ConflictError } from "../../shared/errors/conflict-error";
import { NotFoundError } from "../../shared/errors/not-found-error";
import { ValidationError } from "../../shared/errors/validation-error";
import MilestoneModel from "../milestones/milestone.model";
import TaskModel, { type TaskModelType } from "./task.model";
import type {
  TaskConstraints,
  TaskIntent,
  TaskIssuer,
  TaskStatus,
  TaskTarget,
  UpdateTaskInput as UpdateTaskRequest,
} from "./task.schemas";

export type CreateTaskInput = {
  jobId: string;
  projectId: string;
  milestoneId: string;
  parentTaskId?: string;
  dependencies?: string[];
  issuer: TaskIssuer;
  target: TaskTarget;
  intent: TaskIntent;
  inputs: Record<string, unknown>;
  constraints: TaskConstraints;
  requiredArtifacts?: string[];
  acceptanceCriteria?: string[];
  idempotencyKey: string;
  status?: TaskStatus;
  attemptCount?: number;
  maxAttempts?: number;
  sessionName?: string;
  sessionCount?: number;
  maxSessions?: number;
  nextRetryAt?: Date;
  lastError?: string;
  retryable?: boolean;
  sequence?: number;
  outputs?: Record<string, unknown>;
  artifacts?: string[];
  errors?: string[];
};

export type UpdateTaskInput = UpdateTaskRequest & {
  sessionName?: string;
  sessionCount?: number;
  maxSessions?: number;
  intent?: TaskIntent;
  inputs?: Record<string, unknown>;
  prompt?: string;
  testingCriteria?: string[];
  inputAcceptanceCriteria?: string[];
};

export type UpdateConcreteTaskExecutionInput = {
  prompt?: string;
  testingCriteria?: string[];
  acceptanceCriteria?: string[];
  requiredArtifacts?: string[];
  enrichment?: unknown;
  outputs?: unknown;
};

export type TaskRecord = {
  _id: string;
  jobId: string;
  projectId: string;
  milestoneId: string;
  parentTaskId?: string;
  dependencies: string[];
  issuer: TaskIssuer;
  target: TaskTarget;
  intent: TaskIntent;
  inputs: Record<string, unknown>;
  constraints: TaskConstraints;
  requiredArtifacts: string[];
  acceptanceCriteria: string[];
  idempotencyKey: string;
  status: TaskStatus;
  attemptCount: number;
  maxAttempts: number;
  sessionName?: string;
  sessionCount: number;
  maxSessions: number;
  nextRetryAt?: Date;
  lastError?: string;
  retryable: boolean;
  sequence: number;
  outputs?: Record<string, unknown>;
  artifacts: string[];
  errors: string[];
  createdAt: Date;
  updatedAt: Date;
};

export type ListTasksInput = {
  jobId?: string;
  projectId?: string;
  milestoneId?: string;
  parentTaskId?: string;
  status?: TaskStatus;
  intent?: TaskIntent;
  agentId?: string;
  sessionName?: string;
  sessionCount?: number;
  maxSessions?: number;
  limit?: number;
  skip?: number;
};

export type CountTasksInput = Pick<
  ListTasksInput,
  | "jobId"
  | "projectId"
  | "milestoneId"
  | "parentTaskId"
  | "status"
  | "intent"
  | "agentId"
  | "sessionName"
  | "sessionCount"
  | "maxSessions"
>;

export type MilestonePlanningBatchMutation = {
  referenceKeys: string[];
  dependencyTaskIds: string[];
  dependencyRefs: string[];
};

export type MilestonePlanningBatchCreate = MilestonePlanningBatchMutation & {
  kind: "create";
  task: CreateTaskInput;
};

export type MilestonePlanningBatchUpdate = MilestonePlanningBatchMutation & {
  kind: "update";
  taskId: string;
  patch: Record<string, unknown>;
};

export type CommitMilestonePlanningBatchInput = {
  plannerTaskId: string;
  jobId: string;
  projectId: string;
  milestoneId: string;
  creates: MilestonePlanningBatchCreate[];
  updates: MilestonePlanningBatchUpdate[];
};

export type MilestonePlanningBatchOperationStage =
  | "validation"
  | "seed_updates"
  | "create"
  | "update"
  | "finalize";

export type MilestonePlanningBatchOperationKind = "create" | "update";

export type CommittedMilestonePlanningBatchTask = {
  stage: MilestonePlanningBatchOperationStage;
  operationKind: MilestonePlanningBatchOperationKind;
  operationIndex: number;
  taskId: string;
  ownerLabel: string;
  referenceKeys: string[];
  dependencyTaskIds: string[];
  dependencyRefs: string[];
  taskIntent?: TaskIntent;
  idempotencyKey?: string;
};

export type CommitMilestonePlanningBatchResult = {
  createdTaskIds: string[];
  updatedTaskIds: string[];
  reviewTaskId: string;
  reviewTaskCreated: boolean;
  reviewTaskUpdated: boolean;
  createdTasks: CommittedMilestonePlanningBatchTask[];
  updatedTasks: CommittedMilestonePlanningBatchTask[];
};

export type MilestonePlanningBatchValidationIssue = {
  code: string;
  message: string;
  taskId?: string;
  referenceKey?: string;
  field?: string;
  stage?: MilestonePlanningBatchOperationStage;
  operationKind?: MilestonePlanningBatchOperationKind;
  operationIndex?: number;
  ownerLabel?: string;
  taskIntent?: TaskIntent;
  idempotencyKey?: string;
  details?: Record<string, unknown>;
};

export type ValidateMilestonePlanningBatchResult = {
  ok: boolean;
  issues: MilestonePlanningBatchValidationIssue[];
};

export interface TasksServicePort {
  createTask(input: CreateTaskInput): Promise<TaskRecord>;
  getTaskById(taskId: string): Promise<TaskRecord | null>;
  requireTaskById(taskId: string): Promise<TaskRecord>;
  getTaskByIdempotencyKey(idempotencyKey: string): Promise<TaskRecord | null>;
  updateTask(taskId: string, updates: UpdateTaskInput): Promise<TaskRecord>;
  updateConcreteTaskExecution(
    taskId: string,
    updates: UpdateConcreteTaskExecutionInput,
  ): Promise<TaskRecord>;
  setStatus(taskId: string, status: TaskStatus): Promise<TaskRecord>;
  markRunning(taskId: string): Promise<TaskRecord>;
  markSucceeded(input: {
    taskId: string;
    outputs?: Record<string, unknown>;
    artifacts?: string[];
  }): Promise<TaskRecord>;
  markFailed(input: {
    taskId: string;
    errors: string[];
    outputs?: Record<string, unknown>;
    artifacts?: string[];
  }): Promise<TaskRecord>;
  markFailedExhausted(input: {
    taskId: string;
    errors: string[];
    outputs?: Record<string, unknown>;
    artifacts?: string[];
  }): Promise<TaskRecord>;
  requeueTask(input: {
    taskId: string;
    error: string;
    nextRetryAt?: Date;
    outputs?: Record<string, unknown>;
    artifacts?: string[];
  }): Promise<TaskRecord>;
  cancelTask(taskId: string, reason?: string): Promise<TaskRecord>;
  listTasks(input?: ListTasksInput): Promise<TaskRecord[]>;
  countTasks(input?: CountTasksInput): Promise<number>;
  listRunnableTasks(input?: {
    jobId?: string;
    agentId?: string;
    milestoneId?: string;
    limit?: number;
    ignoreRetryAt?: boolean;
  }): Promise<TaskRecord[]>;
  listNextRunnableTask(input?: {
    jobId?: string;
    agentId?: string;
    milestoneId?: string;
    ignoreRetryAt?: boolean;
  }): Promise<TaskRecord | null>;
  validateMilestonePlanningBatch(
    batch: CommitMilestonePlanningBatchInput,
  ): Promise<ValidateMilestonePlanningBatchResult>;
  commitMilestonePlanningBatch(
    batch: CommitMilestonePlanningBatchInput,
  ): Promise<CommitMilestonePlanningBatchResult>;
}

type TaskDocumentLike = {
  id: string;
  jobId: Types.ObjectId;
  projectId: Types.ObjectId;
  milestoneId: Types.ObjectId;
  parentTaskId?: Types.ObjectId | null;
  dependencies?: Types.ObjectId[] | null;
  issuer: {
    kind: TaskIssuer["kind"];
    id: string;
    sessionId?: string | null;
    role?: string | null;
  };
  target: {
    agentId: string;
  };
  intent: TaskIntent;
  inputs?: Record<string, unknown> | null;
  constraints: {
    toolProfile: string;
    sandbox: TaskConstraints["sandbox"];
    maxTokens?: number | null;
    maxCost?: number | null;
  };
  requiredArtifacts: string[];
  acceptanceCriteria: string[];
  idempotencyKey: string;
  status: TaskStatus;
  attemptCount: number;
  maxAttempts: number;
  sessionName?: string | null;
  sessionCount: number;
  maxSessions: number;
  nextRetryAt?: Date | null;
  lastError?: string | null;
  retryable: boolean;
  sequence: number;
  outputs?: Record<string, unknown> | null;
  artifacts?: string[] | null;
  errors?: string[] | null;
  createdAt: Date;
  updatedAt: Date;
};

type TaskInputValidationIssue = {
  code: string;
  field: string;
  message: string;
  details?: Record<string, unknown>;
};

function isPlainRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function validateRequiredTrimmedString(
  value: unknown,
  field: string,
  code: string,
): TaskInputValidationIssue[] {
  if (typeof value !== "string") {
    return [
      {
        code,
        field,
        message: `${field} must be a non-empty string.`,
      },
    ];
  }

  if (value.trim().length === 0) {
    return [
      {
        code,
        field,
        message: `${field} must be a non-empty string.`,
      },
    ];
  }

  return [];
}

const VALID_TASK_INTENTS = new Set<string>([
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
  "enrich_task",
]);

const VALID_TASK_STATUSES = new Set<string>([
  "queued",
  "running",
  "qa",
  "succeeded",
  "failed",
  "canceled",
]);

const VALID_TASK_ISSUER_KINDS = new Set<string>(["system", "user", "agent"]);

const VALID_TASK_SANDBOXES = new Set<string>(["off", "non-main", "all"]);

function validateRequiredEnumValue(
  value: unknown,
  field: string,
  code: string,
  allowedValues: ReadonlySet<string>,
): TaskInputValidationIssue[] {
  if (typeof value !== "string" || !allowedValues.has(value)) {
    return [
      {
        code,
        field,
        message: `${field} must be one of: ${[...allowedValues].join(", ")}.`,
        details: {
          receivedValue: value,
          allowedValues: [...allowedValues],
        },
      },
    ];
  }

  return [];
}

function validateOptionalEnumValue(
  value: unknown,
  field: string,
  code: string,
  allowedValues: ReadonlySet<string>,
): TaskInputValidationIssue[] {
  if (value === undefined) {
    return [];
  }

  return validateRequiredEnumValue(value, field, code, allowedValues);
}

function validateOptionalStringArray(
  value: unknown,
  field: string,
  code: string,
): TaskInputValidationIssue[] {
  if (value === undefined) {
    return [];
  }

  if (!Array.isArray(value)) {
    return [
      {
        code,
        field,
        message: `${field} must be an array of non-empty strings.`,
      },
    ];
  }

  const invalidItems = value
    .map((item, index) => ({ item, index }))
    .filter(({ item }) => typeof item !== "string" || item.trim().length === 0)
    .map(({ index }) => index);

  if (invalidItems.length > 0) {
    return [
      {
        code,
        field,
        message: `${field} must contain only non-empty strings.`,
        details: {
          invalidIndexes: invalidItems,
        },
      },
    ];
  }

  return [];
}

function getCreateTaskValidationIssues(
  input: CreateTaskInput,
): TaskInputValidationIssue[] {
  const issues: TaskInputValidationIssue[] = [];

  issues.push(
    ...validateRequiredEnumValue(
      input.intent,
      "intent",
      "TASK_CREATE_INTENT_INVALID",
      VALID_TASK_INTENTS,
    ),
    ...validateOptionalEnumValue(
      input.status,
      "status",
      "TASK_CREATE_STATUS_INVALID",
      VALID_TASK_STATUSES,
    ),
    ...validateRequiredEnumValue(
      input.issuer?.kind,
      "issuer.kind",
      "TASK_CREATE_ISSUER_KIND_INVALID",
      VALID_TASK_ISSUER_KINDS,
    ),
    ...validateRequiredEnumValue(
      input.constraints?.sandbox,
      "constraints.sandbox",
      "TASK_CREATE_SANDBOX_INVALID",
      VALID_TASK_SANDBOXES,
    ),
    ...validateRequiredTrimmedString(
      input.jobId,
      "jobId",
      "TASK_CREATE_JOB_ID_INVALID",
    ),
    ...validateRequiredTrimmedString(
      input.projectId,
      "projectId",
      "TASK_CREATE_PROJECT_ID_INVALID",
    ),
    ...validateRequiredTrimmedString(
      input.milestoneId,
      "milestoneId",
      "TASK_CREATE_MILESTONE_ID_INVALID",
    ),
    ...validateRequiredTrimmedString(
      input.idempotencyKey,
      "idempotencyKey",
      "TASK_CREATE_IDEMPOTENCY_KEY_INVALID",
    ),
    ...validateRequiredTrimmedString(
      input.issuer?.id,
      "issuer.id",
      "TASK_CREATE_ISSUER_ID_INVALID",
    ),
    ...validateRequiredTrimmedString(
      input.target?.agentId,
      "target.agentId",
      "TASK_CREATE_TARGET_AGENT_INVALID",
    ),
    ...validateRequiredTrimmedString(
      input.constraints?.toolProfile,
      "constraints.toolProfile",
      "TASK_CREATE_TOOL_PROFILE_INVALID",
    ),
  );

  if (!isPlainRecord(input.inputs)) {
    issues.push({
      code: "TASK_CREATE_INPUTS_INVALID",
      field: "inputs",
      message: "inputs must be a plain object.",
    });
  }

  if (input.outputs !== undefined && !isPlainRecord(input.outputs)) {
    issues.push({
      code: "TASK_CREATE_OUTPUTS_INVALID",
      field: "outputs",
      message: "outputs must be a plain object when provided.",
    });
  }

  issues.push(
    ...validateOptionalStringArray(
      input.dependencies,
      "dependencies",
      "TASK_CREATE_DEPENDENCIES_INVALID",
    ),
    ...validateOptionalStringArray(
      input.requiredArtifacts,
      "requiredArtifacts",
      "TASK_CREATE_REQUIRED_ARTIFACTS_INVALID",
    ),
    ...validateOptionalStringArray(
      input.acceptanceCriteria,
      "acceptanceCriteria",
      "TASK_CREATE_ACCEPTANCE_CRITERIA_INVALID",
    ),
    ...validateOptionalStringArray(
      input.artifacts,
      "artifacts",
      "TASK_CREATE_ARTIFACTS_INVALID",
    ),
    ...validateOptionalStringArray(
      input.errors,
      "errors",
      "TASK_CREATE_ERRORS_INVALID",
    ),
  );

  if (
    typeof input.attemptCount === "number" &&
    (!Number.isInteger(input.attemptCount) || input.attemptCount < 0)
  ) {
    issues.push({
      code: "TASK_CREATE_ATTEMPT_COUNT_INVALID",
      field: "attemptCount",
      message: "attemptCount must be an integer greater than or equal to 0.",
    });
  }

  if (
    typeof input.maxAttempts === "number" &&
    (!Number.isInteger(input.maxAttempts) || input.maxAttempts < 1)
  ) {
    issues.push({
      code: "TASK_CREATE_MAX_ATTEMPTS_INVALID",
      field: "maxAttempts",
      message: "maxAttempts must be an integer greater than or equal to 1.",
    });
  }

  if (
    typeof input.attemptCount === "number" &&
    typeof input.maxAttempts === "number" &&
    Number.isInteger(input.attemptCount) &&
    Number.isInteger(input.maxAttempts) &&
    input.attemptCount > input.maxAttempts
  ) {
    issues.push({
      code: "TASK_CREATE_ATTEMPT_WINDOW_INVALID",
      field: "attemptCount",
      message: "attemptCount cannot exceed maxAttempts.",
      details: {
        attemptCount: input.attemptCount,
        maxAttempts: input.maxAttempts,
      },
    });
  }

  if (
    typeof input.sessionCount === "number" &&
    (!Number.isInteger(input.sessionCount) || input.sessionCount < 1)
  ) {
    issues.push({
      code: "TASK_CREATE_SESSION_COUNT_INVALID",
      field: "sessionCount",
      message: "sessionCount must be an integer greater than or equal to 1.",
    });
  }

  if (
    typeof input.maxSessions === "number" &&
    (!Number.isInteger(input.maxSessions) || input.maxSessions < 1)
  ) {
    issues.push({
      code: "TASK_CREATE_MAX_SESSIONS_INVALID",
      field: "maxSessions",
      message: "maxSessions must be an integer greater than or equal to 1.",
    });
  }

  if (
    typeof input.sessionCount === "number" &&
    typeof input.maxSessions === "number" &&
    Number.isInteger(input.sessionCount) &&
    Number.isInteger(input.maxSessions) &&
    input.sessionCount > input.maxSessions
  ) {
    issues.push({
      code: "TASK_CREATE_SESSION_WINDOW_INVALID",
      field: "sessionCount",
      message: "sessionCount cannot exceed maxSessions.",
      details: {
        sessionCount: input.sessionCount,
        maxSessions: input.maxSessions,
      },
    });
  }

  if (
    typeof input.sequence === "number" &&
    (!Number.isInteger(input.sequence) || input.sequence < 0)
  ) {
    issues.push({
      code: "TASK_CREATE_SEQUENCE_INVALID",
      field: "sequence",
      message: "sequence must be an integer greater than or equal to 0.",
    });
  }

  return issues;
}

function assertValidCreateTaskInput(input: CreateTaskInput): void {
  const issues = getCreateTaskValidationIssues(input);

  if (issues.length === 0) {
    return;
  }

  throw new ValidationError({
    message: `Task creation input failed validation with ${issues.length} issue${issues.length === 1 ? "" : "s"}.`,
    code: "TASK_CREATE_INPUT_INVALID",
    statusCode: 400,
    details: {
      idempotencyKey: input.idempotencyKey,
      issues,
    },
  });
}

function getBatchShapeValidationIssues(
  batch: CommitMilestonePlanningBatchInput,
): MilestonePlanningBatchValidationIssue[] {
  const issues: MilestonePlanningBatchValidationIssue[] = [];

  if (!Array.isArray(batch.creates)) {
    issues.push({
      code: "TASK_BATCH_CREATES_INVALID",
      message: "Milestone planning batch creates must be an array.",
      field: "creates",
      stage: "validation",
      details: {
        receivedType: typeof batch.creates,
      },
    });

    return issues;
  }

  if (!Array.isArray(batch.updates)) {
    issues.push({
      code: "TASK_BATCH_UPDATES_INVALID",
      message: "Milestone planning batch updates must be an array.",
      field: "updates",
      stage: "validation",
      details: {
        receivedType: typeof batch.updates,
      },
    });

    return issues;
  }

  if (batch.creates.length === 0 && batch.updates.length === 0) {
    issues.push({
      code: "TASK_BATCH_EMPTY",
      message:
        "Milestone planning batch contains no task mutations. Refusing to commit an empty task batch.",
      stage: "validation",
      details: {
        plannerTaskId: batch.plannerTaskId,
        projectId: batch.projectId,
        milestoneId: batch.milestoneId,
      },
    });
  }

  return issues;
}

function toObjectId(value: string, fieldName: string): Types.ObjectId {
  if (!Types.ObjectId.isValid(value)) {
    throw new ValidationError({
      message: `Invalid ${fieldName}: ${value}`,
      code: "INVALID_OBJECT_ID",
      statusCode: 400,
      details: {
        fieldName,
        value,
      },
    });
  }

  return new Types.ObjectId(value);
}

function mapTask(document: TaskDocumentLike): TaskRecord {
  return {
    _id: document.id,
    jobId: document.jobId.toString(),
    projectId: document.projectId.toString(),
    milestoneId: document.milestoneId.toString(),
    ...(document.parentTaskId
      ? { parentTaskId: document.parentTaskId.toString() }
      : {}),
    dependencies: [...(document.dependencies ?? [])].map((dependencyId) =>
      dependencyId.toString(),
    ),
    issuer: {
      kind: document.issuer.kind,
      id: document.issuer.id,
      ...(document.issuer.sessionId
        ? { sessionId: document.issuer.sessionId }
        : {}),
      ...(document.issuer.role ? { role: document.issuer.role } : {}),
    },
    target: {
      agentId: document.target.agentId,
    },
    intent: document.intent,
    inputs: (document.inputs ?? {}) as Record<string, unknown>,
    constraints: {
      toolProfile: document.constraints.toolProfile,
      sandbox: document.constraints.sandbox,
      ...(typeof document.constraints.maxTokens === "number"
        ? { maxTokens: document.constraints.maxTokens }
        : {}),
      ...(typeof document.constraints.maxCost === "number"
        ? { maxCost: document.constraints.maxCost }
        : {}),
    },
    requiredArtifacts: [...document.requiredArtifacts],
    acceptanceCriteria: [...document.acceptanceCriteria],
    idempotencyKey: document.idempotencyKey,
    status: document.status,
    attemptCount: document.attemptCount,
    maxAttempts: document.maxAttempts,
    ...(typeof document.sessionName === "string" &&
    document.sessionName.length > 0
      ? { sessionName: document.sessionName }
      : {}),
    sessionCount:
      typeof document.sessionCount === "number" &&
      Number.isFinite(document.sessionCount)
        ? document.sessionCount
        : 1,
    maxSessions:
      typeof document.maxSessions === "number" &&
      Number.isFinite(document.maxSessions)
        ? document.maxSessions
        : 1,
    ...(document.nextRetryAt instanceof Date
      ? { nextRetryAt: document.nextRetryAt }
      : {}),
    ...(typeof document.lastError === "string" && document.lastError.length > 0
      ? { lastError: document.lastError }
      : {}),
    retryable: document.retryable,
    sequence: document.sequence,
    ...(document.outputs !== null && document.outputs !== undefined
      ? { outputs: document.outputs as Record<string, unknown> }
      : {}),
    artifacts: [...(document.artifacts ?? [])],
    errors: [...(document.errors ?? [])],
    createdAt: document.createdAt,
    updatedAt: document.updatedAt,
  };
}

function normalizeCreateTaskInput(input: CreateTaskInput): {
  requiredArtifacts: string[];
  acceptanceCriteria: string[];
  status: TaskStatus;
  attemptCount: number;
  maxAttempts: number;
  sessionName?: string;
  sessionCount: number;
  maxSessions: number;
  retryable: boolean;
  sequence: number;
  artifacts: string[];
  errors: string[];
  dependencies: string[];
} {
  const sessionName =
    typeof input.sessionName === "string" && input.sessionName.trim().length > 0
      ? input.sessionName.trim()
      : undefined;

  return {
    requiredArtifacts: [...(input.requiredArtifacts ?? [])],
    acceptanceCriteria: [...(input.acceptanceCriteria ?? [])],
    status: input.status ?? "queued",
    attemptCount: input.attemptCount ?? 0,
    maxAttempts: input.maxAttempts ?? 5,
    ...(sessionName ? { sessionName } : {}),
    sessionCount: input.sessionCount ?? 1,
    maxSessions: input.maxSessions ?? 4,
    retryable: input.retryable ?? true,
    sequence: input.sequence ?? 0,
    artifacts: [...(input.artifacts ?? [])],
    errors: [...(input.errors ?? [])],
    dependencies: dedupeStringIds(input.dependencies ?? []),
  };
}

function dedupeStringIds(values: string[]): string[] {
  return [...new Set(values.map((value) => value.trim()).filter(Boolean))];
}

function asRecordValue(value: unknown): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return {};
  }

  return value as Record<string, unknown>;
}

function readOptionalStringValue(
  record: Record<string, unknown>,
  keys: string[],
): string | undefined {
  for (const key of keys) {
    const value = record[key];
    if (typeof value !== "string") {
      continue;
    }

    const trimmed = value.trim();
    if (trimmed.length > 0) {
      return trimmed;
    }
  }

  return undefined;
}

function normalizeStringArrayValue(value: unknown): string[] {
  if (!Array.isArray(value)) {
    return [];
  }

  return value
    .filter((item): item is string => typeof item === "string")
    .map((item) => item.trim())
    .filter((item) => item.length > 0);
}

type TaskServiceErrorDetails = Record<string, unknown>;

type TaskServiceErrorLike = Error & {
  code?: string;
  statusCode?: number;
  details?: TaskServiceErrorDetails;
  cause?: unknown;
};

function getBatchOwnerLabel(input: {
  referenceKeys: string[];
  fallback: string;
}): string {
  return dedupeStringIds(input.referenceKeys)[0] ?? input.fallback;
}

export class TaskService implements TasksServicePort {
  async createTask(input: CreateTaskInput): Promise<TaskRecord> {
    try {
      assertValidCreateTaskInput(input);
      const normalized = normalizeCreateTaskInput(input);

      await this.assertMilestoneBelongsToProject({
        projectId: input.projectId,
        milestoneId: input.milestoneId,
      });

      if (normalized.dependencies.length > 0) {
        await this.assertDependenciesAreValid({
          projectId: input.projectId,
          milestoneId: input.milestoneId,
          dependencyIds: normalized.dependencies,
        });
      }

      const created = await TaskModel.create({
        jobId: toObjectId(input.jobId, "jobId"),
        projectId: toObjectId(input.projectId, "projectId"),
        milestoneId: toObjectId(input.milestoneId, "milestoneId"),
        ...(typeof input.parentTaskId === "string"
          ? { parentTaskId: toObjectId(input.parentTaskId, "parentTaskId") }
          : {}),
        dependencies: normalized.dependencies.map((dependencyId) =>
          toObjectId(dependencyId, "dependencyId"),
        ),
        issuer: {
          kind: input.issuer.kind,
          id: input.issuer.id.trim(),
          ...(typeof input.issuer.sessionId === "string"
            ? { sessionId: input.issuer.sessionId.trim() }
            : {}),
          ...(typeof input.issuer.role === "string"
            ? { role: input.issuer.role.trim() }
            : {}),
        },
        target: {
          agentId: input.target.agentId.trim(),
        },
        intent: input.intent,
        inputs: input.inputs,
        constraints: {
          toolProfile: input.constraints.toolProfile.trim(),
          sandbox: input.constraints.sandbox,
          ...(typeof input.constraints.maxTokens === "number"
            ? { maxTokens: input.constraints.maxTokens }
            : {}),
          ...(typeof input.constraints.maxCost === "number"
            ? { maxCost: input.constraints.maxCost }
            : {}),
        },
        requiredArtifacts: normalized.requiredArtifacts,
        acceptanceCriteria: normalized.acceptanceCriteria,
        idempotencyKey: input.idempotencyKey.trim(),
        status: normalized.status,
        attemptCount: normalized.attemptCount,
        maxAttempts: normalized.maxAttempts,
        ...(normalized.sessionName
          ? { sessionName: normalized.sessionName }
          : {}),
        sessionCount: normalized.sessionCount,
        maxSessions: normalized.maxSessions,
        ...(input.nextRetryAt instanceof Date
          ? { nextRetryAt: input.nextRetryAt }
          : {}),
        ...(typeof input.lastError === "string" &&
        input.lastError.trim().length > 0
          ? { lastError: input.lastError.trim() }
          : {}),
        retryable: normalized.retryable,
        sequence: normalized.sequence,
        ...(input.outputs !== undefined ? { outputs: input.outputs } : {}),
        artifacts: normalized.artifacts,
        errors: normalized.errors,
      });

      return mapTask(created);
    } catch (error: unknown) {
      if (this.isDuplicateKeyError(error)) {
        throw new ConflictError({
          message: `A task with idempotency key "${input.idempotencyKey}" already exists.`,
          code: "TASK_IDEMPOTENCY_KEY_ALREADY_EXISTS",
          details: {
            idempotencyKey: input.idempotencyKey,
          },
          cause: error,
        });
      }

      throw error;
    }
  }

  async getTaskById(taskId: string): Promise<TaskRecord | null> {
    const task = await TaskModel.findById(toObjectId(taskId, "taskId")).exec();
    return task ? mapTask(task) : null;
  }

  async requireTaskById(taskId: string): Promise<TaskRecord> {
    const task = await this.getTaskById(taskId);

    if (!task) {
      throw new NotFoundError({
        message: `Task not found: ${taskId}`,
        code: "TASK_NOT_FOUND",
        details: { taskId },
      });
    }

    return task;
  }

  async getTaskByIdempotencyKey(
    idempotencyKey: string,
  ): Promise<TaskRecord | null> {
    const task = await TaskModel.findOne({
      idempotencyKey: idempotencyKey.trim(),
    }).exec();

    return task ? mapTask(task) : null;
  }

  async updateTask(
    taskId: string,
    updates: UpdateTaskInput,
  ): Promise<TaskRecord> {
    const needsMilestoneValidation =
      typeof updates.milestoneId === "string" ||
      updates.dependencies !== undefined;
    const needsCurrentTask =
      needsMilestoneValidation ||
      updates.inputs !== undefined ||
      updates.prompt !== undefined ||
      updates.testingCriteria !== undefined ||
      updates.inputAcceptanceCriteria !== undefined;

    const current = needsCurrentTask
      ? await this.requireTaskById(taskId)
      : null;

    const updatePayload: Record<string, unknown> = {};

    if (typeof updates.milestoneId === "string" && current) {
      await this.assertMilestoneBelongsToProject({
        projectId: current.projectId,
        milestoneId: updates.milestoneId,
      });

      updatePayload.milestoneId = toObjectId(
        updates.milestoneId,
        "milestoneId",
      );
    }

    if (typeof updates.parentTaskId === "string") {
      updatePayload.parentTaskId = toObjectId(
        updates.parentTaskId,
        "parentTaskId",
      );
    }

    if (updates.dependencies !== undefined && current) {
      const nextMilestoneId =
        typeof updates.milestoneId === "string"
          ? updates.milestoneId
          : current.milestoneId;

      const nextDependencies = dedupeStringIds(updates.dependencies);

      await this.assertDependenciesAreValid({
        projectId: current.projectId,
        milestoneId: nextMilestoneId,
        dependencyIds: nextDependencies,
        currentTaskId: taskId,
      });

      updatePayload.dependencies = nextDependencies.map((dependencyId) =>
        toObjectId(dependencyId, "dependencyId"),
      );
    } else if (typeof updates.milestoneId === "string" && current) {
      await this.assertDependenciesAreValid({
        projectId: current.projectId,
        milestoneId: updates.milestoneId,
        dependencyIds: current.dependencies,
        currentTaskId: taskId,
      });
    }

    if (typeof updates.intent === "string") {
      updatePayload.intent = updates.intent;
    }

    if (typeof updates.status === "string") {
      updatePayload.status = updates.status;
    }

    if (typeof updates.attemptCount === "number") {
      updatePayload.attemptCount = updates.attemptCount;
    }

    if (typeof updates.maxAttempts === "number") {
      updatePayload.maxAttempts = updates.maxAttempts;
    }

    if (typeof updates.sessionName === "string") {
      const trimmedSessionName = updates.sessionName.trim();
      if (trimmedSessionName.length > 0) {
        updatePayload.sessionName = trimmedSessionName;
      }
    }

    if (typeof updates.sessionCount === "number") {
      updatePayload.sessionCount = updates.sessionCount;
    }

    if (typeof updates.maxSessions === "number") {
      updatePayload.maxSessions = updates.maxSessions;
    }

    if (updates.nextRetryAt instanceof Date) {
      updatePayload.nextRetryAt = updates.nextRetryAt;
    }

    if (typeof updates.lastError === "string") {
      updatePayload.lastError = updates.lastError.trim();
    }

    if (typeof updates.retryable === "boolean") {
      updatePayload.retryable = updates.retryable;
    }

    if (typeof updates.sequence === "number") {
      updatePayload.sequence = updates.sequence;
    }

    if (current) {
      let nextInputs: Record<string, unknown> | null =
        updates.inputs !== undefined ? asRecordValue(updates.inputs) : null;

      const ensureNextInputs = (): Record<string, unknown> => {
        if (!nextInputs) {
          nextInputs = { ...asRecordValue(current.inputs) };
        }

        return nextInputs;
      };

      if (updates.prompt !== undefined) {
        ensureNextInputs().prompt = updates.prompt;
      }

      if (updates.testingCriteria !== undefined) {
        ensureNextInputs().testingCriteria = [...updates.testingCriteria];
      }

      if (updates.inputAcceptanceCriteria !== undefined) {
        ensureNextInputs().acceptanceCriteria = [
          ...updates.inputAcceptanceCriteria,
        ];
      }

      if (nextInputs) {
        updatePayload.inputs = nextInputs;
      }
    } else if (updates.inputs !== undefined) {
      updatePayload.inputs = asRecordValue(updates.inputs);
    }

    if (updates.outputs !== undefined) {
      updatePayload.outputs = updates.outputs;
    }

    if (updates.artifacts !== undefined) {
      updatePayload.artifacts = updates.artifacts;
    }

    if (updates.errors !== undefined) {
      updatePayload.errors = updates.errors;
    }

    if (updates.target !== undefined) {
      updatePayload.target = {
        agentId: updates.target.agentId.trim(),
      };
    }

    const updated = await TaskModel.findByIdAndUpdate(
      toObjectId(taskId, "taskId"),
      updatePayload,
      {
        new: true,
        runValidators: true,
      },
    ).exec();

    if (!updated) {
      throw new NotFoundError({
        message: `Task not found: ${taskId}`,
        code: "TASK_NOT_FOUND",
        details: { taskId },
      });
    }

    return mapTask(updated);
  }

  async updateConcreteTaskExecution(
    taskId: string,
    updates: UpdateConcreteTaskExecutionInput,
  ): Promise<TaskRecord> {
    const resolved = this.resolveConcreteTaskExecutionUpdates(updates);

    if (
      resolved.prompt === undefined &&
      resolved.testingCriteria === undefined &&
      resolved.acceptanceCriteria === undefined &&
      resolved.requiredArtifacts === undefined
    ) {
      return this.requireTaskById(taskId);
    }

    return this.updateTask(taskId, {
      ...(resolved.prompt !== undefined ? { prompt: resolved.prompt } : {}),
      ...(resolved.testingCriteria !== undefined
        ? { testingCriteria: resolved.testingCriteria }
        : {}),
      ...(resolved.acceptanceCriteria !== undefined
        ? { inputAcceptanceCriteria: resolved.acceptanceCriteria }
        : {}),
      ...(resolved.acceptanceCriteria !== undefined
        ? { acceptanceCriteria: resolved.acceptanceCriteria }
        : {}),
      ...(resolved.requiredArtifacts !== undefined
        ? { requiredArtifacts: resolved.requiredArtifacts }
        : {}),
    });
  }

  async setStatus(taskId: string, status: TaskStatus): Promise<TaskRecord> {
    return this.updateTask(taskId, { status });
  }

  async markRunning(taskId: string): Promise<TaskRecord> {
    const updated = await TaskModel.findByIdAndUpdate(
      toObjectId(taskId, "taskId"),
      {
        $set: {
          status: "running",
        },
        $inc: {
          attemptCount: 1,
        },
        $unset: {
          nextRetryAt: 1,
        },
      },
      {
        new: true,
        runValidators: true,
      },
    ).exec();

    if (!updated) {
      throw new NotFoundError({
        message: `Task not found: ${taskId}`,
        code: "TASK_NOT_FOUND",
        details: { taskId },
      });
    }

    return mapTask(updated);
  }

  async markSucceeded(input: {
    taskId: string;
    outputs?: Record<string, unknown>;
    artifacts?: string[];
  }): Promise<TaskRecord> {
    const updated = await TaskModel.findByIdAndUpdate(
      toObjectId(input.taskId, "taskId"),
      {
        $set: {
          status: "succeeded",
          ...(input.outputs !== undefined ? { outputs: input.outputs } : {}),
          ...(input.artifacts !== undefined
            ? { artifacts: input.artifacts }
            : {}),
          errors: [],
        },
        $unset: {
          nextRetryAt: 1,
          lastError: 1,
        },
      },
      {
        new: true,
        runValidators: true,
      },
    ).exec();

    if (!updated) {
      throw new NotFoundError({
        message: `Task not found: ${input.taskId}`,
        code: "TASK_NOT_FOUND",
        details: { taskId: input.taskId },
      });
    }

    return mapTask(updated);
  }

  async markFailed(input: {
    taskId: string;
    errors: string[];
    outputs?: Record<string, unknown>;
    artifacts?: string[];
  }): Promise<TaskRecord> {
    return this.updateTask(input.taskId, {
      status: "failed",
      errors: input.errors,
      ...(input.outputs !== undefined ? { outputs: input.outputs } : {}),
      ...(input.artifacts !== undefined ? { artifacts: input.artifacts } : {}),
      ...(input.errors[0] ? { lastError: input.errors[0] } : {}),
    });
  }

  async markFailedExhausted(input: {
    taskId: string;
    errors: string[];
    outputs?: Record<string, unknown>;
    artifacts?: string[];
  }): Promise<TaskRecord> {
    const lastError = input.errors[input.errors.length - 1];

    const updated = await TaskModel.findByIdAndUpdate(
      toObjectId(input.taskId, "taskId"),
      {
        $set: {
          status: "failed",
          errors: input.errors,
          ...(typeof lastError === "string" && lastError.trim().length > 0
            ? { lastError: lastError.trim() }
            : {}),
          ...(input.outputs !== undefined ? { outputs: input.outputs } : {}),
          ...(input.artifacts !== undefined
            ? { artifacts: input.artifacts }
            : {}),
        },
        $unset: {
          nextRetryAt: 1,
        },
      },
      {
        new: true,
        runValidators: true,
      },
    ).exec();

    if (!updated) {
      throw new NotFoundError({
        message: `Task not found: ${input.taskId}`,
        code: "TASK_NOT_FOUND",
        details: { taskId: input.taskId },
      });
    }

    return mapTask(updated);
  }

  async requeueTask(input: {
    taskId: string;
    error: string;
    nextRetryAt?: Date;
    outputs?: Record<string, unknown>;
    artifacts?: string[];
  }): Promise<TaskRecord> {
    const current = await this.requireTaskById(input.taskId);

    const nextErrors = [...current.errors, input.error.trim()].filter(
      (value) => value.length > 0,
    );

    const updated = await TaskModel.findByIdAndUpdate(
      toObjectId(input.taskId, "taskId"),
      {
        $set: {
          status: "queued",
          errors: nextErrors,
          lastError: input.error.trim(),
          ...(input.nextRetryAt instanceof Date
            ? { nextRetryAt: input.nextRetryAt }
            : {}),
          ...(input.outputs !== undefined ? { outputs: input.outputs } : {}),
          ...(input.artifacts !== undefined
            ? { artifacts: input.artifacts }
            : {}),
        },
        ...(input.nextRetryAt instanceof Date
          ? {}
          : {
              $unset: {
                nextRetryAt: 1,
              },
            }),
      },
      {
        new: true,
        runValidators: true,
      },
    ).exec();

    if (!updated) {
      throw new NotFoundError({
        message: `Task not found: ${input.taskId}`,
        code: "TASK_NOT_FOUND",
        details: { taskId: input.taskId },
      });
    }

    return mapTask(updated);
  }

  async cancelTask(taskId: string, reason?: string): Promise<TaskRecord> {
    const updated = await TaskModel.findByIdAndUpdate(
      toObjectId(taskId, "taskId"),
      {
        $set: {
          status: "canceled",
          ...(typeof reason === "string" && reason.trim().length > 0
            ? {
                errors: [reason.trim()],
                lastError: reason.trim(),
              }
            : {}),
        },
        $unset: {
          nextRetryAt: 1,
        },
      },
      {
        new: true,
        runValidators: true,
      },
    ).exec();

    if (!updated) {
      throw new NotFoundError({
        message: `Task not found: ${taskId}`,
        code: "TASK_NOT_FOUND",
        details: { taskId },
      });
    }

    return mapTask(updated);
  }

  async listTasks(input: ListTasksInput = {}): Promise<TaskRecord[]> {
    const filter = this.buildFilter(input);
    const limit = Math.min(Math.max(input.limit ?? 25, 1), 100);
    const skip = Math.max(input.skip ?? 0, 0);

    const sort: [string, 1 | -1][] =
      input.status === "queued"
        ? [
            ["sequence", 1],
            ["createdAt", 1],
          ]
        : [["updatedAt", -1]];

    const tasks = await TaskModel.find(filter)
      .sort(sort)
      .skip(skip)
      .limit(limit)
      .exec();

    return tasks.map(mapTask);
  }

  async countTasks(input: CountTasksInput = {}): Promise<number> {
    const filter = this.buildFilter(input);
    return TaskModel.countDocuments(filter).exec();
  }

  async listRunnableTasks(input?: {
    jobId?: string;
    agentId?: string;
    milestoneId?: string;
    limit?: number;
    ignoreRetryAt?: boolean;
  }): Promise<TaskRecord[]> {
    const now = new Date();

    const filter: QueryFilter<TaskModelType> = {
      status: "queued",
      ...(!input?.ignoreRetryAt
        ? {
            $or: [
              { nextRetryAt: { $exists: false } },
              { nextRetryAt: { $lte: now } },
            ],
          }
        : {}),
    };

    if (typeof input?.jobId === "string") {
      filter.jobId = toObjectId(input.jobId, "jobId");
    }

    if (typeof input?.agentId === "string") {
      filter["target.agentId"] = input.agentId.trim();
    }

    if (typeof input?.milestoneId === "string") {
      filter.milestoneId = toObjectId(input.milestoneId, "milestoneId");
    }

    const limit = Math.min(Math.max(input?.limit ?? 25, 1), 100);
    const fetchLimit = Math.min(limit * 5, 200);

    const candidates = await TaskModel.find(filter)
      .sort({ sequence: 1, nextRetryAt: 1, createdAt: 1 })
      .limit(fetchLimit)
      .exec();

    const runnable: TaskRecord[] = [];

    for (const candidate of candidates) {
      const mapped = mapTask(candidate);

      if (await this.areDependenciesSatisfied(mapped)) {
        runnable.push(mapped);
      }

      if (runnable.length >= limit) {
        break;
      }
    }

    return runnable;
  }

  async listNextRunnableTask(input?: {
    jobId?: string;
    agentId?: string;
    milestoneId?: string;
    ignoreRetryAt?: boolean;
  }): Promise<TaskRecord | null> {
    const tasks = await this.listRunnableTasks({
      ...(typeof input?.jobId === "string" ? { jobId: input.jobId } : {}),
      ...(typeof input?.agentId === "string" ? { agentId: input.agentId } : {}),
      ...(typeof input?.milestoneId === "string"
        ? { milestoneId: input.milestoneId }
        : {}),
      ...(typeof input?.ignoreRetryAt === "boolean"
        ? { ignoreRetryAt: input.ignoreRetryAt }
        : {}),
      limit: 1,
    });

    return tasks[0] ?? null;
  }

  async validateMilestonePlanningBatch(
    batch: CommitMilestonePlanningBatchInput,
  ): Promise<ValidateMilestonePlanningBatchResult> {
    const issues: MilestonePlanningBatchValidationIssue[] = [];
    const batchShapeIssues = getBatchShapeValidationIssues(batch);

    if (batchShapeIssues.length > 0) {
      return {
        ok: false,
        issues: batchShapeIssues,
      };
    }

    try {
      await this.assertMilestoneBelongsToProject({
        projectId: batch.projectId,
        milestoneId: batch.milestoneId,
      });
    } catch (error: unknown) {
      issues.push(
        this.toMilestoneBatchValidationIssue(error, {
          code: "TASK_BATCH_MILESTONE_INVALID",
          message:
            "Milestone planning batch references an invalid milestone or project.",
          stage: "validation",
          details: {
            plannerTaskId: batch.plannerTaskId,
            projectId: batch.projectId,
            milestoneId: batch.milestoneId,
          },
        }),
      );

      return {
        ok: false,
        issues,
      };
    }

    const referenceOwner = new Map<string, string>();
    const createIdempotencyKeyOwner = new Map<string, string>();
    const updateTaskIds = dedupeStringIds(
      batch.updates.map((update) => update.taskId),
    );
    const existingUpdatedTasks = new Map<string, TaskRecord>();
    let reviewTaskPresent = false;

    for (const [createIndex, create] of batch.creates.entries()) {
      const ownerLabel = getBatchOwnerLabel({
        referenceKeys: create.referenceKeys,
        fallback: create.task.idempotencyKey,
      });

      const createTaskIssues = getCreateTaskValidationIssues(create.task);
      issues.push(
        ...createTaskIssues.map((issue) => ({
          code: issue.code,
          message: issue.message,
          field: issue.field,
          stage: "validation" as const,
          operationKind: "create" as const,
          operationIndex: createIndex,
          ownerLabel,
          taskIntent: create.task.intent,
          idempotencyKey: create.task.idempotencyKey.trim(),
          referenceKey: ownerLabel,
          ...(issue.details ? { details: issue.details } : {}),
        })),
      );
    }

    for (const [createIndex, create] of batch.creates.entries()) {
      const ownerLabel = getBatchOwnerLabel({
        referenceKeys: create.referenceKeys,
        fallback: create.task.idempotencyKey,
      });
      const normalizedReferenceKeys = dedupeStringIds(create.referenceKeys);

      for (const referenceKey of normalizedReferenceKeys) {
        const existingOwner = referenceOwner.get(referenceKey);
        if (existingOwner) {
          issues.push({
            code: "TASK_BATCH_REFERENCE_KEY_DUPLICATE",
            message: `Duplicate batch reference key "${referenceKey}".`,
            referenceKey,
            stage: "validation",
            operationKind: "create",
            operationIndex: createIndex,
            ownerLabel,
            taskIntent: create.task.intent,
            idempotencyKey: create.task.idempotencyKey,
            details: {
              firstOwner: existingOwner,
              secondOwner: ownerLabel,
            },
          });
          continue;
        }

        referenceOwner.set(referenceKey, ownerLabel);
      }

      const idempotencyKey = create.task.idempotencyKey.trim();
      const existingCreateOwner = createIdempotencyKeyOwner.get(idempotencyKey);
      if (existingCreateOwner) {
        issues.push({
          code: "TASK_BATCH_CREATE_IDEMPOTENCY_KEY_DUPLICATE",
          message: `Duplicate create idempotency key "${idempotencyKey}" in milestone planning batch.`,
          referenceKey: ownerLabel,
          field: "task.idempotencyKey",
          stage: "validation",
          operationKind: "create",
          operationIndex: createIndex,
          ownerLabel,
          taskIntent: create.task.intent,
          idempotencyKey,
          details: {
            firstOwner: existingCreateOwner,
            secondOwner: ownerLabel,
            idempotencyKey,
          },
        });
      } else {
        createIdempotencyKeyOwner.set(idempotencyKey, ownerLabel);
      }

      if (create.task.intent === "review_milestone") {
        reviewTaskPresent = true;
      }
    }

    for (const updateTaskId of updateTaskIds) {
      const existing = await this.getTaskById(updateTaskId);
      if (!existing) {
        issues.push({
          code: "TASK_BATCH_UPDATE_TARGET_NOT_FOUND",
          message: `Task not found: ${updateTaskId}`,
          taskId: updateTaskId,
          stage: "validation",
          operationKind: "update",
          details: { taskId: updateTaskId },
        });
        continue;
      }

      existingUpdatedTasks.set(updateTaskId, existing);
    }

    for (const [updateIndex, update] of batch.updates.entries()) {
      const existing = existingUpdatedTasks.get(update.taskId);
      const ownerLabel = getBatchOwnerLabel({
        referenceKeys: update.referenceKeys,
        fallback: update.taskId,
      });
      const normalizedReferenceKeys = dedupeStringIds(update.referenceKeys);

      for (const referenceKey of normalizedReferenceKeys) {
        const existingOwner = referenceOwner.get(referenceKey);
        if (existingOwner && existingOwner !== ownerLabel) {
          issues.push({
            code: "TASK_BATCH_REFERENCE_KEY_DUPLICATE",
            message: `Duplicate batch reference key "${referenceKey}".`,
            referenceKey,
            stage: "validation",
            operationKind: "update",
            operationIndex: updateIndex,
            ownerLabel,
            ...(existing?.intent ? { taskIntent: existing.intent } : {}),
            details: {
              firstOwner: existingOwner,
              secondOwner: ownerLabel,
            },
          });
          continue;
        }

        referenceOwner.set(referenceKey, ownerLabel);
      }

      if (existing && existing.intent === "review_milestone") {
        reviewTaskPresent = true;
      }
    }

    const referenceToTaskId = new Map<string, string>();
    for (const [updateIndex, update] of batch.updates.entries()) {
      const existing = existingUpdatedTasks.get(update.taskId);
      if (!existing) {
        continue;
      }

      for (const referenceKey of dedupeStringIds(update.referenceKeys)) {
        referenceToTaskId.set(referenceKey, existing._id);
      }

      if (!reviewTaskPresent && existing.intent === "review_milestone") {
        reviewTaskPresent = true;
      }

      if (
        typeof update.patch.idempotencyKey === "string" &&
        update.patch.idempotencyKey.trim().length > 0 &&
        update.patch.idempotencyKey.trim() !== existing.idempotencyKey
      ) {
        const existingByIdempotencyKey = await this.getTaskByIdempotencyKey(
          update.patch.idempotencyKey.trim(),
        );
        if (
          existingByIdempotencyKey &&
          existingByIdempotencyKey._id !== existing._id
        ) {
          issues.push({
            code: "TASK_BATCH_UPDATE_IDEMPOTENCY_KEY_ALREADY_EXISTS",
            message: `A task with idempotency key "${update.patch.idempotencyKey.trim()}" already exists.`,
            taskId: update.taskId,
            field: "patch.idempotencyKey",
            stage: "validation",
            operationKind: "update",
            operationIndex: updateIndex,
            ownerLabel: getBatchOwnerLabel({
              referenceKeys: update.referenceKeys,
              fallback: update.taskId,
            }),
            ...(existing.intent ? { taskIntent: existing.intent } : {}),
            idempotencyKey: update.patch.idempotencyKey.trim(),
            details: {
              idempotencyKey: update.patch.idempotencyKey.trim(),
              existingTaskId: existingByIdempotencyKey._id,
            },
          });
        }
      }
    }

    for (const [createIndex, create] of batch.creates.entries()) {
      const ownerLabel = getBatchOwnerLabel({
        referenceKeys: create.referenceKeys,
        fallback: create.task.idempotencyKey,
      });
      const trimmedIdempotencyKey = create.task.idempotencyKey.trim();
      const existingByIdempotencyKey = await this.getTaskByIdempotencyKey(
        trimmedIdempotencyKey,
      );
      if (existingByIdempotencyKey) {
        issues.push({
          code: "TASK_BATCH_IDEMPOTENCY_KEY_ALREADY_EXISTS",
          message: `A task with idempotency key "${trimmedIdempotencyKey}" already exists.`,
          referenceKey: ownerLabel,
          field: "task.idempotencyKey",
          stage: "validation",
          operationKind: "create",
          operationIndex: createIndex,
          ownerLabel,
          taskIntent: create.task.intent,
          idempotencyKey: trimmedIdempotencyKey,
          details: {
            idempotencyKey: trimmedIdempotencyKey,
            existingTaskId: existingByIdempotencyKey._id,
          },
        });
      }
    }

    for (const [createIndex, create] of batch.creates.entries()) {
      const ownerLabel = getBatchOwnerLabel({
        referenceKeys: create.referenceKeys,
        fallback: create.task.idempotencyKey,
      });
      const dependencyIssues = await this.validateBatchDependencyTargets({
        explicitTaskIds: create.dependencyTaskIds,
        dependencyRefs: create.dependencyRefs,
        projectId: batch.projectId,
        milestoneId: batch.milestoneId,
        ownerLabel,
        referenceToTaskId,
        operationIndex: createIndex,
        operationKind: "create",
        taskIntent: create.task.intent,
        idempotencyKey: create.task.idempotencyKey,
      });
      issues.push(...dependencyIssues);

      for (const referenceKey of dedupeStringIds(create.referenceKeys)) {
        referenceToTaskId.set(referenceKey, `pending:${ownerLabel}`);
      }
    }

    for (const [updateIndex, update] of batch.updates.entries()) {
      const ownerLabel = getBatchOwnerLabel({
        referenceKeys: update.referenceKeys,
        fallback: update.taskId,
      });
      const existingUpdatedIntent = existingUpdatedTasks.get(
        update.taskId,
      )?.intent;
      const dependencyIssues = await this.validateBatchDependencyTargets({
        explicitTaskIds: update.dependencyTaskIds,
        dependencyRefs: update.dependencyRefs,
        projectId: batch.projectId,
        milestoneId: batch.milestoneId,
        ownerLabel,
        referenceToTaskId,
        currentTaskId: update.taskId,
        operationIndex: updateIndex,
        operationKind: "update",
        ...(existingUpdatedIntent ? { taskIntent: existingUpdatedIntent } : {}),
        ...(typeof update.patch.idempotencyKey === "string"
          ? { idempotencyKey: update.patch.idempotencyKey.trim() }
          : {}),
      });
      issues.push(...dependencyIssues);
    }

    const actionableCreateCount = batch.creates.filter(
      (create) => create.task.intent !== "review_milestone",
    ).length;
    const actionableUpdateCount = batch.updates.filter((update) => {
      const existing = existingUpdatedTasks.get(update.taskId);
      return existing ? existing.intent !== "review_milestone" : false;
    }).length;

    if (actionableCreateCount + actionableUpdateCount === 0) {
      issues.push({
        code: "TASK_BATCH_ACTIONABLE_TASKS_EMPTY",
        message:
          "Milestone planning batch does not contain any actionable non-review tasks.",
        stage: "validation",
        details: {
          plannerTaskId: batch.plannerTaskId,
          milestoneId: batch.milestoneId,
          createCount: batch.creates.length,
          updateCount: batch.updates.length,
        },
      });
    }

    if (!reviewTaskPresent) {
      issues.push({
        code: "TASK_BATCH_REVIEW_TASK_MISSING",
        message:
          "Milestone planning batch must include a review_milestone task.",
        stage: "validation",
        details: {
          plannerTaskId: batch.plannerTaskId,
          milestoneId: batch.milestoneId,
        },
      });
    }

    return {
      ok: issues.length === 0,
      issues,
    };
  }

  async commitMilestonePlanningBatch(
    batch: CommitMilestonePlanningBatchInput,
  ): Promise<CommitMilestonePlanningBatchResult> {
    const validation = await this.validateMilestonePlanningBatch(batch);

    if (!validation.ok) {
      throw new ValidationError({
        message: `Milestone planning batch failed validation with ${validation.issues.length} issue${validation.issues.length === 1 ? "" : "s"}.`,
        code: "TASK_BATCH_VALIDATION_FAILED",
        statusCode: 422,
        details: {
          plannerTaskId: batch.plannerTaskId,
          milestoneId: batch.milestoneId,
          issues: validation.issues,
        },
      });
    }

    const referenceToTaskId = new Map<string, string>();
    const createdTaskIds: string[] = [];
    const updatedTaskIds: string[] = [];
    const createdTasks: CommittedMilestonePlanningBatchTask[] = [];
    const updatedTasks: CommittedMilestonePlanningBatchTask[] = [];
    let reviewTaskId: string | null = null;
    let reviewTaskCreated = false;
    let reviewTaskUpdated = false;

    try {
      for (const [updateIndex, update] of batch.updates.entries()) {
        const existing = await TaskModel.findById(
          toObjectId(update.taskId, "taskId"),
        ).exec();

        if (!existing) {
          throw new NotFoundError({
            message: `Task not found: ${update.taskId}`,
            code: "TASK_NOT_FOUND",
            details: { taskId: update.taskId },
          });
        }

        for (const referenceKey of dedupeStringIds(update.referenceKeys)) {
          referenceToTaskId.set(referenceKey, existing.id);
        }

        if (existing.intent === "review_milestone") {
          reviewTaskId = existing.id;
        }
      }
    } catch (error: unknown) {
      throw this.wrapMilestonePlanningBatchCommitError(error, {
        batch,
        stage: "seed_updates",
        operationKind: "update",
        operationIndex: -1,
        ownerLabel: "seed_updates",
        createdTaskIds,
        updatedTaskIds,
        createdTasks,
        updatedTasks,
        reviewTaskId,
      });
    }

    for (const [createIndex, create] of batch.creates.entries()) {
      const ownerLabel = getBatchOwnerLabel({
        referenceKeys: create.referenceKeys,
        fallback: create.task.idempotencyKey,
      });

      try {
        assertValidCreateTaskInput(create.task);
        const normalized = normalizeCreateTaskInput(create.task);
        const dependencyIds = await this.resolveBatchDependencyIds(
          {
            explicitTaskIds: create.dependencyTaskIds,
            dependencyRefs: create.dependencyRefs,
            projectId: batch.projectId,
            milestoneId: batch.milestoneId,
          },
          referenceToTaskId,
        );

        await this.assertMilestoneBelongsToProject({
          projectId: create.task.projectId,
          milestoneId: create.task.milestoneId,
        });

        const created = await TaskModel.create({
          jobId: toObjectId(create.task.jobId, "jobId"),
          projectId: toObjectId(create.task.projectId, "projectId"),
          milestoneId: toObjectId(create.task.milestoneId, "milestoneId"),
          ...(typeof create.task.parentTaskId === "string"
            ? {
                parentTaskId: toObjectId(
                  create.task.parentTaskId,
                  "parentTaskId",
                ),
              }
            : {}),
          dependencies: dependencyIds.map((dependencyId) =>
            toObjectId(dependencyId, "dependencyId"),
          ),
          issuer: {
            kind: create.task.issuer.kind,
            id: create.task.issuer.id.trim(),
            ...(typeof create.task.issuer.sessionId === "string"
              ? { sessionId: create.task.issuer.sessionId.trim() }
              : {}),
            ...(typeof create.task.issuer.role === "string"
              ? { role: create.task.issuer.role.trim() }
              : {}),
          },
          target: {
            agentId: create.task.target.agentId.trim(),
          },
          intent: create.task.intent,
          inputs: create.task.inputs,
          constraints: {
            toolProfile: create.task.constraints.toolProfile.trim(),
            sandbox: create.task.constraints.sandbox,
            ...(typeof create.task.constraints.maxTokens === "number"
              ? { maxTokens: create.task.constraints.maxTokens }
              : {}),
            ...(typeof create.task.constraints.maxCost === "number"
              ? { maxCost: create.task.constraints.maxCost }
              : {}),
          },
          requiredArtifacts: normalized.requiredArtifacts,
          acceptanceCriteria: normalized.acceptanceCriteria,
          idempotencyKey: create.task.idempotencyKey.trim(),
          status: normalized.status,
          attemptCount: normalized.attemptCount,
          maxAttempts: normalized.maxAttempts,
          ...(normalized.sessionName
            ? { sessionName: normalized.sessionName }
            : {}),
          sessionCount: normalized.sessionCount,
          maxSessions: normalized.maxSessions,
          ...(create.task.nextRetryAt instanceof Date
            ? { nextRetryAt: create.task.nextRetryAt }
            : {}),
          ...(typeof create.task.lastError === "string" &&
          create.task.lastError.trim().length > 0
            ? { lastError: create.task.lastError.trim() }
            : {}),
          retryable: normalized.retryable,
          sequence: normalized.sequence,
          ...(create.task.outputs !== undefined
            ? { outputs: create.task.outputs }
            : {}),
          artifacts: normalized.artifacts,
          errors: normalized.errors,
        });

        createdTaskIds.push(created.id);
        createdTasks.push({
          stage: "create",
          operationKind: "create",
          operationIndex: createIndex,
          taskId: created.id,
          ownerLabel,
          referenceKeys: dedupeStringIds(create.referenceKeys),
          dependencyTaskIds: dedupeStringIds(create.dependencyTaskIds),
          dependencyRefs: dedupeStringIds(create.dependencyRefs),
          taskIntent: created.intent,
          idempotencyKey: created.idempotencyKey,
        });

        for (const referenceKey of dedupeStringIds(create.referenceKeys)) {
          referenceToTaskId.set(referenceKey, created.id);
        }

        if (created.intent === "review_milestone") {
          reviewTaskId = created.id;
          reviewTaskCreated = true;
        }
      } catch (error: unknown) {
        throw this.wrapMilestonePlanningBatchCommitError(error, {
          batch,
          stage: "create",
          operationKind: "create",
          operationIndex: createIndex,
          ownerLabel,
          taskIntent: create.task.intent,
          idempotencyKey: create.task.idempotencyKey,
          referenceKeys: create.referenceKeys,
          dependencyTaskIds: create.dependencyTaskIds,
          dependencyRefs: create.dependencyRefs,
          createdTaskIds,
          updatedTaskIds,
          createdTasks,
          updatedTasks,
          reviewTaskId,
        });
      }
    }

    for (const [updateIndex, update] of batch.updates.entries()) {
      const ownerLabel = getBatchOwnerLabel({
        referenceKeys: update.referenceKeys,
        fallback: update.taskId,
      });
      const existingTask = await this.getTaskById(update.taskId);

      try {
        const dependencyIds = await this.resolveBatchDependencyIds(
          {
            explicitTaskIds: update.dependencyTaskIds,
            dependencyRefs: update.dependencyRefs,
            projectId: batch.projectId,
            milestoneId: batch.milestoneId,
            currentTaskId: update.taskId,
          },
          referenceToTaskId,
        );

        const updatePayload = this.buildAtomicBatchUpdatePayload({
          patch: update.patch,
          dependencyIds,
        });

        const updated = await TaskModel.findByIdAndUpdate(
          toObjectId(update.taskId, "taskId"),
          updatePayload,
          {
            new: true,
            runValidators: true,
          },
        ).exec();

        if (!updated) {
          throw new NotFoundError({
            message: `Task not found: ${update.taskId}`,
            code: "TASK_NOT_FOUND",
            details: { taskId: update.taskId },
          });
        }

        updatedTaskIds.push(updated.id);
        updatedTasks.push({
          stage: "update",
          operationKind: "update",
          operationIndex: updateIndex,
          taskId: updated.id,
          ownerLabel,
          referenceKeys: dedupeStringIds(update.referenceKeys),
          dependencyTaskIds: dedupeStringIds(update.dependencyTaskIds),
          dependencyRefs: dedupeStringIds(update.dependencyRefs),
          taskIntent: updated.intent,
          idempotencyKey: updated.idempotencyKey,
        });

        for (const referenceKey of dedupeStringIds(update.referenceKeys)) {
          referenceToTaskId.set(referenceKey, updated.id);
        }

        if (updated.intent === "review_milestone") {
          reviewTaskId = updated.id;
          reviewTaskUpdated = true;
        }
      } catch (error: unknown) {
        throw this.wrapMilestonePlanningBatchCommitError(error, {
          batch,
          stage: "update",
          operationKind: "update",
          operationIndex: updateIndex,
          ownerLabel,
          taskId: update.taskId,
          ...(existingTask?.intent ? { taskIntent: existingTask.intent } : {}),
          ...(typeof update.patch.idempotencyKey === "string" &&
          update.patch.idempotencyKey.trim().length > 0
            ? { idempotencyKey: update.patch.idempotencyKey.trim() }
            : existingTask?.idempotencyKey
              ? { idempotencyKey: existingTask.idempotencyKey }
              : {}),
          referenceKeys: update.referenceKeys,
          dependencyTaskIds: update.dependencyTaskIds,
          dependencyRefs: update.dependencyRefs,
          createdTaskIds,
          updatedTaskIds,
          createdTasks,
          updatedTasks,
          reviewTaskId,
        });
      }
    }

    if (!reviewTaskId) {
      throw this.wrapMilestonePlanningBatchCommitError(
        new ValidationError({
          message:
            "Milestone planning batch must include a review_milestone task.",
          code: "TASK_BATCH_REVIEW_TASK_MISSING",
          statusCode: 400,
          details: {
            plannerTaskId: batch.plannerTaskId,
            milestoneId: batch.milestoneId,
          },
        }),
        {
          batch,
          stage: "finalize",
          operationKind: "update",
          operationIndex: -1,
          ownerLabel: "review_milestone",
          createdTaskIds,
          updatedTaskIds,
          createdTasks,
          updatedTasks,
          reviewTaskId,
        },
      );
    }

    return {
      createdTaskIds,
      updatedTaskIds,
      reviewTaskId,
      reviewTaskCreated,
      reviewTaskUpdated,
      createdTasks,
      updatedTasks,
    };
  }

  private buildFilter(
    input: CountTasksInput | ListTasksInput,
  ): QueryFilter<TaskModelType> {
    const filter: QueryFilter<TaskModelType> = {};

    if (typeof input.jobId === "string") {
      filter.jobId = toObjectId(input.jobId, "jobId");
    }

    if (typeof input.projectId === "string") {
      filter.projectId = toObjectId(input.projectId, "projectId");
    }

    if (typeof input.milestoneId === "string") {
      filter.milestoneId = toObjectId(input.milestoneId, "milestoneId");
    }

    if (typeof input.parentTaskId === "string") {
      filter.parentTaskId = toObjectId(input.parentTaskId, "parentTaskId");
    }

    if (input.status) {
      filter.status = input.status;
    }

    if (input.intent) {
      filter.intent = input.intent;
    }

    if (typeof input.agentId === "string") {
      filter["target.agentId"] = input.agentId.trim();
    }

    if (typeof input.sessionName === "string") {
      filter.sessionName = input.sessionName.trim();
    }

    if (typeof input.sessionCount === "number") {
      filter.sessionCount = input.sessionCount;
    }

    if (typeof input.maxSessions === "number") {
      filter.maxSessions = input.maxSessions;
    }

    return filter;
  }

  private async assertMilestoneBelongsToProject(
    input: {
      projectId: string;
      milestoneId: string;
    },
    session?: ClientSession,
  ): Promise<void> {
    const milestone = await MilestoneModel.findById(
      toObjectId(input.milestoneId, "milestoneId"),
    )
      .session(session ?? null)
      .exec();

    if (!milestone) {
      throw new NotFoundError({
        message: `Milestone not found: ${input.milestoneId}`,
        code: "MILESTONE_NOT_FOUND",
        details: {
          milestoneId: input.milestoneId,
        },
      });
    }

    if (milestone.projectId.toString() !== input.projectId) {
      throw new ValidationError({
        message: "Task milestone must belong to the same project.",
        code: "TASK_MILESTONE_PROJECT_MISMATCH",
        statusCode: 400,
        details: {
          projectId: input.projectId,
          milestoneId: input.milestoneId,
        },
      });
    }
  }

  private async assertDependenciesAreValid(
    input: {
      projectId: string;
      milestoneId: string;
      dependencyIds: string[];
      currentTaskId?: string;
    },
    session?: ClientSession,
  ): Promise<void> {
    const dependencyIds = dedupeStringIds(input.dependencyIds);

    if (dependencyIds.length === 0) {
      return;
    }

    if (
      typeof input.currentTaskId === "string" &&
      dependencyIds.includes(input.currentTaskId)
    ) {
      throw new ValidationError({
        message: "A task cannot depend on itself.",
        code: "TASK_SELF_DEPENDENCY",
        statusCode: 400,
        details: {
          taskId: input.currentTaskId,
        },
      });
    }

    const dependencyObjectIds = dependencyIds.map((dependencyId) =>
      toObjectId(dependencyId, "dependencyId"),
    );

    const dependencyTasks = await TaskModel.find({
      _id: { $in: dependencyObjectIds },
    })
      .session(session ?? null)
      .exec();

    if (dependencyTasks.length !== dependencyObjectIds.length) {
      const foundIds = new Set(dependencyTasks.map((task) => task.id));
      const missingDependencyId = dependencyIds.find(
        (dependencyId) => !foundIds.has(dependencyId),
      );

      throw new NotFoundError({
        message: `Dependency task not found: ${missingDependencyId ?? "unknown"}`,
        code: "TASK_DEPENDENCY_NOT_FOUND",
        details: {
          dependencyIds,
        },
      });
    }

    for (const dependencyTask of dependencyTasks) {
      if (dependencyTask.projectId.toString() !== input.projectId) {
        throw new ValidationError({
          message: "Task dependencies must belong to the same project.",
          code: "TASK_DEPENDENCY_PROJECT_MISMATCH",
          statusCode: 400,
          details: {
            projectId: input.projectId,
            dependencyTaskId: dependencyTask.id,
          },
        });
      }

      if (dependencyTask.milestoneId.toString() !== input.milestoneId) {
        throw new ValidationError({
          message: "Task dependencies must belong to the same milestone.",
          code: "TASK_DEPENDENCY_MILESTONE_MISMATCH",
          statusCode: 400,
          details: {
            milestoneId: input.milestoneId,
            dependencyTaskId: dependencyTask.id,
          },
        });
      }
    }
  }

  private buildAtomicBatchUpdatePayload(input: {
    patch: Record<string, unknown>;
    dependencyIds: string[];
  }): Record<string, unknown> {
    const patch = input.patch;
    const updatePayload: Record<string, unknown> = {
      dependencies: input.dependencyIds.map((dependencyId) =>
        toObjectId(dependencyId, "dependencyId"),
      ),
    };

    if (typeof patch.parentTaskId === "string") {
      updatePayload.parentTaskId = toObjectId(
        patch.parentTaskId,
        "parentTaskId",
      );
    }

    if (typeof patch.milestoneId === "string") {
      updatePayload.milestoneId = toObjectId(patch.milestoneId, "milestoneId");
    }

    if (typeof patch.status === "string") {
      updatePayload.status = patch.status;
    }

    if (typeof patch.attemptCount === "number") {
      updatePayload.attemptCount = patch.attemptCount;
    }

    if (typeof patch.maxAttempts === "number") {
      updatePayload.maxAttempts = patch.maxAttempts;
    }

    if (typeof patch.sessionName === "string") {
      const trimmed = patch.sessionName.trim();
      if (trimmed.length > 0) {
        updatePayload.sessionName = trimmed;
      }
    }

    if (typeof patch.sessionCount === "number") {
      updatePayload.sessionCount = patch.sessionCount;
    }

    if (typeof patch.maxSessions === "number") {
      updatePayload.maxSessions = patch.maxSessions;
    }

    if (patch.nextRetryAt instanceof Date) {
      updatePayload.nextRetryAt = patch.nextRetryAt;
    }

    if (typeof patch.lastError === "string") {
      updatePayload.lastError = patch.lastError.trim();
    }

    if (typeof patch.retryable === "boolean") {
      updatePayload.retryable = patch.retryable;
    }

    if (typeof patch.sequence === "number") {
      updatePayload.sequence = patch.sequence;
    }

    if (patch.outputs !== undefined) {
      updatePayload.outputs = patch.outputs;
    }

    if (patch.artifacts !== undefined) {
      updatePayload.artifacts = patch.artifacts;
    }

    if (patch.errors !== undefined) {
      updatePayload.errors = patch.errors;
    }

    if (
      patch.target &&
      typeof patch.target === "object" &&
      patch.target !== null
    ) {
      const candidate = patch.target as { agentId?: unknown };
      if (typeof candidate.agentId === "string") {
        updatePayload.target = { agentId: candidate.agentId.trim() };
      }
    }

    if (patch.inputs !== undefined) {
      updatePayload.inputs = patch.inputs;
    }

    if (
      patch.constraints &&
      typeof patch.constraints === "object" &&
      patch.constraints !== null
    ) {
      const candidate = patch.constraints as {
        toolProfile?: unknown;
        sandbox?: unknown;
        maxTokens?: unknown;
        maxCost?: unknown;
      };

      if (
        typeof candidate.toolProfile === "string" &&
        typeof candidate.sandbox === "string"
      ) {
        updatePayload.constraints = {
          toolProfile: candidate.toolProfile.trim(),
          sandbox: candidate.sandbox,
          ...(typeof candidate.maxTokens === "number"
            ? { maxTokens: candidate.maxTokens }
            : {}),
          ...(typeof candidate.maxCost === "number"
            ? { maxCost: candidate.maxCost }
            : {}),
        };
      }
    }

    if (Array.isArray(patch.requiredArtifacts)) {
      updatePayload.requiredArtifacts = [...patch.requiredArtifacts];
    }

    if (Array.isArray(patch.acceptanceCriteria)) {
      updatePayload.acceptanceCriteria = [...patch.acceptanceCriteria];
    }

    if (typeof patch.idempotencyKey === "string") {
      updatePayload.idempotencyKey = patch.idempotencyKey.trim();
    }

    return updatePayload;
  }

  private async resolveBatchDependencyIds(
    input: {
      explicitTaskIds: string[];
      dependencyRefs: string[];
      projectId: string;
      milestoneId: string;
      currentTaskId?: string;
    },
    referenceToTaskId: Map<string, string>,
    session?: ClientSession,
  ): Promise<string[]> {
    const resolvedIds = new Set<string>(dedupeStringIds(input.explicitTaskIds));

    for (const dependencyRef of dedupeStringIds(input.dependencyRefs)) {
      let resolvedTaskId = referenceToTaskId.get(dependencyRef);

      if (!resolvedTaskId) {
        const resolvedByRef = await this.resolveTaskIdByBatchReference(
          dependencyRef,
          input.projectId,
          input.milestoneId,
          session,
        );

        if (resolvedByRef) {
          resolvedTaskId = resolvedByRef;
          referenceToTaskId.set(dependencyRef, resolvedByRef);
        }
      }

      if (!resolvedTaskId) {
        throw new ValidationError({
          message: `Unable to resolve task dependency reference: ${dependencyRef}`,
          code: "TASK_BATCH_DEPENDENCY_REFERENCE_UNRESOLVED",
          statusCode: 400,
          details: {
            dependencyRef,
            plannerProjectId: input.projectId,
            plannerMilestoneId: input.milestoneId,
          },
        });
      }

      resolvedIds.add(resolvedTaskId);
    }

    const dependencyIds = Array.from(resolvedIds);

    if (dependencyIds.length > 0) {
      await this.assertDependenciesAreValid(
        {
          projectId: input.projectId,
          milestoneId: input.milestoneId,
          dependencyIds,
          ...(typeof input.currentTaskId === "string"
            ? { currentTaskId: input.currentTaskId }
            : {}),
        },
        session,
      );
    }

    return dependencyIds;
  }

  private async resolveTaskIdByBatchReference(
    reference: string,
    projectId: string,
    milestoneId: string,
    session?: ClientSession,
  ): Promise<string | null> {
    const candidateIdempotencyKey =
      this.extractIdempotencyKeyFromBatchReference(reference);

    if (!candidateIdempotencyKey) {
      return null;
    }

    const task = await TaskModel.findOne({
      idempotencyKey: candidateIdempotencyKey,
      projectId: toObjectId(projectId, "projectId"),
      milestoneId: toObjectId(milestoneId, "milestoneId"),
    })
      .session(session ?? null)
      .exec();

    return task ? task.id : null;
  }

  private extractIdempotencyKeyFromBatchReference(
    reference: string,
  ): string | null {
    const trimmed = reference.trim();

    if (trimmed.length === 0) {
      return null;
    }

    if (trimmed.startsWith("execution:")) {
      return trimmed.slice("execution:".length) || null;
    }

    if (trimmed.startsWith("enrichment:")) {
      const executionIdempotencyKey = trimmed.slice("enrichment:".length);
      return executionIdempotencyKey
        ? `${executionIdempotencyKey}:enrich`
        : null;
    }

    if (trimmed.startsWith("review:")) {
      return trimmed.slice("review:".length) || null;
    }

    return null;
  }

  private async validateBatchDependencyTargets(input: {
    explicitTaskIds: string[];
    dependencyRefs: string[];
    projectId: string;
    milestoneId: string;
    ownerLabel: string;
    referenceToTaskId: Map<string, string>;
    currentTaskId?: string;
    operationIndex?: number;
    operationKind?: MilestonePlanningBatchOperationKind;
    taskIntent?: TaskIntent;
    idempotencyKey?: string;
  }): Promise<MilestonePlanningBatchValidationIssue[]> {
    const issues: MilestonePlanningBatchValidationIssue[] = [];

    for (const dependencyRef of dedupeStringIds(input.dependencyRefs)) {
      const resolvedTaskId = input.referenceToTaskId.get(dependencyRef);
      if (resolvedTaskId) {
        continue;
      }

      const resolvedByRef = await this.resolveTaskIdByBatchReference(
        dependencyRef,
        input.projectId,
        input.milestoneId,
      );

      if (!resolvedByRef) {
        issues.push({
          code: "TASK_BATCH_DEPENDENCY_REFERENCE_UNRESOLVED",
          message: `Unable to resolve task dependency reference: ${dependencyRef}`,
          referenceKey: input.ownerLabel,
          field: "dependencyRefs",
          stage: "validation",
          ...(input.operationKind
            ? { operationKind: input.operationKind }
            : {}),
          ...(typeof input.operationIndex === "number"
            ? { operationIndex: input.operationIndex }
            : {}),
          ownerLabel: input.ownerLabel,
          ...(input.taskIntent ? { taskIntent: input.taskIntent } : {}),
          ...(input.idempotencyKey
            ? { idempotencyKey: input.idempotencyKey }
            : {}),
          details: {
            dependencyRef,
            projectId: input.projectId,
            milestoneId: input.milestoneId,
          },
        });
        continue;
      }

      input.referenceToTaskId.set(dependencyRef, resolvedByRef);
    }

    const explicitTaskIds = dedupeStringIds(input.explicitTaskIds);
    if (explicitTaskIds.length > 0) {
      try {
        await this.assertDependenciesAreValid({
          projectId: input.projectId,
          milestoneId: input.milestoneId,
          dependencyIds: explicitTaskIds,
          ...(typeof input.currentTaskId === "string"
            ? { currentTaskId: input.currentTaskId }
            : {}),
        });
      } catch (error: unknown) {
        issues.push(
          this.toMilestoneBatchValidationIssue(error, {
            code: "TASK_BATCH_DEPENDENCY_INVALID",
            message: `Invalid dependency ids for ${input.ownerLabel}.`,
            referenceKey: input.ownerLabel,
            field: "dependencyTaskIds",
            stage: "validation",
            ...(input.operationKind
              ? { operationKind: input.operationKind }
              : {}),
            ...(typeof input.operationIndex === "number"
              ? { operationIndex: input.operationIndex }
              : {}),
            ownerLabel: input.ownerLabel,
            ...(input.taskIntent ? { taskIntent: input.taskIntent } : {}),
            ...(input.idempotencyKey
              ? { idempotencyKey: input.idempotencyKey }
              : {}),
            details: {
              dependencyIds: explicitTaskIds,
            },
          }),
        );
      }
    }

    return issues;
  }

  private toMilestoneBatchValidationIssue(
    error: unknown,
    fallback: MilestonePlanningBatchValidationIssue,
  ): MilestonePlanningBatchValidationIssue {
    if (
      typeof error === "object" &&
      error !== null &&
      "message" in error &&
      typeof (error as { message?: unknown }).message === "string"
    ) {
      const candidate = error as {
        message: string;
        code?: unknown;
        details?: unknown;
      };

      const resolvedDetails =
        candidate.details && typeof candidate.details === "object"
          ? (candidate.details as Record<string, unknown>)
          : fallback.details;

      return {
        code:
          typeof candidate.code === "string" ? candidate.code : fallback.code,
        message: candidate.message,
        ...(fallback.taskId ? { taskId: fallback.taskId } : {}),
        ...(fallback.referenceKey
          ? { referenceKey: fallback.referenceKey }
          : {}),
        ...(fallback.field ? { field: fallback.field } : {}),
        ...(fallback.stage ? { stage: fallback.stage } : {}),
        ...(fallback.operationKind
          ? { operationKind: fallback.operationKind }
          : {}),
        ...(typeof fallback.operationIndex === "number"
          ? { operationIndex: fallback.operationIndex }
          : {}),
        ...(fallback.ownerLabel ? { ownerLabel: fallback.ownerLabel } : {}),
        ...(fallback.taskIntent ? { taskIntent: fallback.taskIntent } : {}),
        ...(fallback.idempotencyKey
          ? { idempotencyKey: fallback.idempotencyKey }
          : {}),
        ...(resolvedDetails ? { details: resolvedDetails } : {}),
      };
    }

    return fallback;
  }

  private toTaskServiceErrorLike(error: unknown): TaskServiceErrorLike | null {
    if (
      typeof error === "object" &&
      error !== null &&
      "message" in error &&
      typeof (error as { message?: unknown }).message === "string"
    ) {
      const candidate = error as {
        message: string;
        code?: unknown;
        statusCode?: unknown;
        details?: unknown;
        cause?: unknown;
      };

      const normalized = new Error(candidate.message) as TaskServiceErrorLike;
      if (typeof candidate.code === "string") {
        normalized.code = candidate.code;
      }
      if (typeof candidate.statusCode === "number") {
        normalized.statusCode = candidate.statusCode;
      }
      if (candidate.details && typeof candidate.details === "object") {
        normalized.details = candidate.details as TaskServiceErrorDetails;
      }
      if ("cause" in candidate) {
        normalized.cause = candidate.cause;
      }

      return normalized;
    }

    return null;
  }

  private wrapMilestonePlanningBatchCommitError(
    error: unknown,
    input: {
      batch: CommitMilestonePlanningBatchInput;
      stage: MilestonePlanningBatchOperationStage;
      operationKind: MilestonePlanningBatchOperationKind;
      operationIndex: number;
      ownerLabel: string;
      taskId?: string;
      taskIntent?: TaskIntent;
      idempotencyKey?: string;
      referenceKeys?: string[];
      dependencyTaskIds?: string[];
      dependencyRefs?: string[];
      createdTaskIds: string[];
      updatedTaskIds: string[];
      createdTasks: CommittedMilestonePlanningBatchTask[];
      updatedTasks: CommittedMilestonePlanningBatchTask[];
      reviewTaskId: string | null;
    },
  ): Error {
    const normalizedReferenceKeys = dedupeStringIds(input.referenceKeys ?? []);
    const normalizedDependencyTaskIds = dedupeStringIds(
      input.dependencyTaskIds ?? [],
    );
    const normalizedDependencyRefs = dedupeStringIds(
      input.dependencyRefs ?? [],
    );
    const knownError = this.toTaskServiceErrorLike(error);
    const detailPayload: Record<string, unknown> = {
      plannerTaskId: input.batch.plannerTaskId,
      jobId: input.batch.jobId,
      projectId: input.batch.projectId,
      milestoneId: input.batch.milestoneId,
      stage: input.stage,
      operationKind: input.operationKind,
      operationIndex: input.operationIndex,
      ownerLabel: input.ownerLabel,
      ...(input.taskId ? { taskId: input.taskId } : {}),
      ...(input.taskIntent ? { taskIntent: input.taskIntent } : {}),
      ...(input.idempotencyKey ? { idempotencyKey: input.idempotencyKey } : {}),
      ...(normalizedReferenceKeys.length > 0
        ? { referenceKeys: normalizedReferenceKeys }
        : {}),
      ...(normalizedDependencyTaskIds.length > 0
        ? { dependencyTaskIds: normalizedDependencyTaskIds }
        : {}),
      ...(normalizedDependencyRefs.length > 0
        ? { dependencyRefs: normalizedDependencyRefs }
        : {}),
      createdTaskIdsSoFar: [...input.createdTaskIds],
      updatedTaskIdsSoFar: [...input.updatedTaskIds],
      createdTasksSoFar: input.createdTasks.map((task) => ({ ...task })),
      updatedTasksSoFar: input.updatedTasks.map((task) => ({ ...task })),
      ...(input.reviewTaskId ? { reviewTaskIdSoFar: input.reviewTaskId } : {}),
      ...(knownError?.code ? { causeCode: knownError.code } : {}),
      ...(knownError?.details ? { causeDetails: knownError.details } : {}),
    };

    const baseMessage =
      input.operationIndex >= 0
        ? `Milestone planning batch ${input.stage} ${input.operationKind}[${input.operationIndex}] failed for ${input.ownerLabel}.`
        : `Milestone planning batch ${input.stage} failed for ${input.ownerLabel}.`;
    const message = knownError
      ? `${baseMessage} ${knownError.message}`
      : baseMessage;

    if (this.isDuplicateKeyError(error)) {
      return new ConflictError({
        message,
        code: "TASK_BATCH_DUPLICATE_KEY",
        details: detailPayload,
        cause: error,
      });
    }

    if (knownError?.statusCode === 404) {
      return new NotFoundError({
        message,
        code: knownError.code ?? "TASK_BATCH_COMMIT_NOT_FOUND",
        details: detailPayload,
      });
    }

    return new ValidationError({
      message,
      code: knownError?.code ?? "TASK_BATCH_COMMIT_FAILED",
      statusCode: knownError?.statusCode === 400 ? 400 : 422,
      details: detailPayload,
    });
  }

  private resolveConcreteTaskExecutionUpdates(
    updates: UpdateConcreteTaskExecutionInput,
  ): {
    prompt?: string;
    testingCriteria?: string[];
    acceptanceCriteria?: string[];
    requiredArtifacts?: string[];
  } {
    const directRecord = asRecordValue(updates);
    const outputsRecord = asRecordValue(updates.outputs);
    const enrichmentRecord = asRecordValue(updates.enrichment);

    const candidateRecords = [
      directRecord,
      enrichmentRecord,
      asRecordValue(outputsRecord.enrichment),
      outputsRecord,
      asRecordValue(outputsRecord.outputs),
      asRecordValue(enrichmentRecord.outputs),
    ];

    let prompt =
      typeof updates.prompt === "string" ? updates.prompt : undefined;
    let testingCriteria =
      updates.testingCriteria !== undefined
        ? [...updates.testingCriteria]
        : undefined;
    let acceptanceCriteria =
      updates.acceptanceCriteria !== undefined
        ? [...updates.acceptanceCriteria]
        : undefined;
    let requiredArtifacts =
      updates.requiredArtifacts !== undefined
        ? [...updates.requiredArtifacts]
        : undefined;

    for (const candidate of candidateRecords) {
      if (prompt === undefined) {
        prompt = readOptionalStringValue(candidate, ["prompt"]);
      }

      if (testingCriteria === undefined) {
        const nextTestingCriteria = normalizeStringArrayValue(
          candidate.testingCriteria,
        );
        if (nextTestingCriteria.length > 0) {
          testingCriteria = nextTestingCriteria;
        }
      }

      if (acceptanceCriteria === undefined) {
        const nextAcceptanceCriteria = normalizeStringArrayValue(
          candidate.acceptanceCriteria,
        );
        if (nextAcceptanceCriteria.length > 0) {
          acceptanceCriteria = nextAcceptanceCriteria;
        }
      }

      if (requiredArtifacts === undefined) {
        const nextRequiredArtifacts = normalizeStringArrayValue(
          candidate.requiredArtifacts ?? candidate.artifacts,
        );
        if (nextRequiredArtifacts.length > 0) {
          requiredArtifacts = nextRequiredArtifacts;
        }
      }
    }

    return {
      ...(prompt !== undefined ? { prompt } : {}),
      ...(testingCriteria !== undefined ? { testingCriteria } : {}),
      ...(acceptanceCriteria !== undefined ? { acceptanceCriteria } : {}),
      ...(requiredArtifacts !== undefined ? { requiredArtifacts } : {}),
    };
  }

  private async areDependenciesSatisfied(task: TaskRecord): Promise<boolean> {
    if (task.dependencies.length === 0) {
      return true;
    }

    const incompleteDependencyCount = await TaskModel.countDocuments({
      _id: {
        $in: task.dependencies.map((dependencyId) =>
          toObjectId(dependencyId, "dependencyId"),
        ),
      },
      status: { $ne: "succeeded" },
    }).exec();

    return incompleteDependencyCount === 0;
  }

  private isDuplicateKeyError(error: unknown): boolean {
    if (
      typeof error === "object" &&
      error !== null &&
      "name" in error &&
      "code" in error
    ) {
      const candidate = error as {
        name?: unknown;
        code?: unknown;
      };

      return candidate.name === "MongoServerError" && candidate.code === 11000;
    }

    return false;
  }
}

export default TaskService;
