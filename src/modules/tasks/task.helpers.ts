import { type QueryFilter, Types } from "mongoose";
import { ConflictError } from "../../shared/errors/conflict-error";
import { NotFoundError } from "../../shared/errors/not-found-error";
import { ValidationError } from "../../shared/errors/validation-error";
import type { TaskModelType } from "./task.model";
import type {
  CommitMilestonePlanningBatchInput,
  CommittedMilestonePlanningBatchTask,
  CountTasksInput,
  CreateTaskInput,
  ListTasksInput,
  MilestonePlanningBatchOperationKind,
  MilestonePlanningBatchOperationStage,
  MilestonePlanningBatchValidationIssue,
  TaskDocumentLike,
  TaskInputValidationIssue,
  TaskIntent,
  TaskRecord,
  TaskServiceErrorDetails,
  TaskServiceErrorLike,
  TaskStatus,
  UpdateConcreteTaskExecutionInput,
} from "./task.types";

export function isPlainRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

export function validateRequiredTrimmedString(
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

export function validateRequiredEnumValue(
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

export function validateOptionalEnumValue(
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

export function validateOptionalStringArray(
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

export function getCreateTaskValidationIssues(
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

export function assertValidCreateTaskInput(input: CreateTaskInput): void {
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

export function getBatchShapeValidationIssues(
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

export function toObjectId(value: string, fieldName: string): Types.ObjectId {
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

export function mapTask(document: TaskDocumentLike): TaskRecord {
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

export function normalizeCreateTaskInput(input: CreateTaskInput): {
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

export function dedupeStringIds(values: string[]): string[] {
  return [...new Set(values.map((value) => value.trim()).filter(Boolean))];
}

export function asRecordValue(value: unknown): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return {};
  }

  return value as Record<string, unknown>;
}

export function readOptionalStringValue(
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

export function normalizeStringArrayValue(value: unknown): string[] {
  if (!Array.isArray(value)) {
    return [];
  }

  return value
    .filter((item): item is string => typeof item === "string")
    .map((item) => item.trim())
    .filter((item) => item.length > 0);
}


export function getBatchOwnerLabel(input: {
  referenceKeys: string[];
  fallback: string;
}): string {
  return dedupeStringIds(input.referenceKeys)[0] ?? input.fallback;
}

export function buildFilter(
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


export function buildAtomicBatchUpdatePayload(input: {
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


export function toMilestoneBatchValidationIssue(
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


export function toTaskServiceErrorLike(error: unknown): TaskServiceErrorLike | null {
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


export function wrapMilestonePlanningBatchCommitError(
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
    const knownError = toTaskServiceErrorLike(error);
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

    if (isDuplicateKeyError(error)) {
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


export function resolveConcreteTaskExecutionUpdates(
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


export function isDuplicateKeyError(error: unknown): boolean {
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

export function extractIdempotencyKeyFromBatchReference(
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
