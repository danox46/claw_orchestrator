import type { MilestoneRecord } from "../milestones/milestone.service";
import type {
  CreateTaskInput,
  TaskRecord,
  TasksServicePort,
} from "../tasks/task.service";
import type { OpenClawTaskStatusResponse } from "./openclaw.client";
import type {
  MilestoneReviewOutcome,
  PatchMilestonePlan,
  PlannedPhase,
  PlannedTaskDefinition,
  PlanningValidationIssue,
  ServiceError,
} from "./agent.types";
export function createServiceError(input: {
  message: string;
  code: string;
  statusCode: number;
  details?: unknown;
  retryable?: boolean;
}): ServiceError {
  const error = new Error(input.message) as ServiceError;
  error.code = input.code;
  error.statusCode = input.statusCode;
  if ("details" in input) {
    error.details = input.details;
  }
  if (typeof input.retryable === "boolean") {
    error.retryable = input.retryable;
  }
  return error;
}
export function shouldRetryTask(
  task: TaskRecord,
  attemptNumber: number,
): boolean {
  return task.retryable && attemptNumber < task.maxAttempts;
}
export function getErrorCode(error: unknown): string | undefined {
  if (!error || typeof error !== "object") {
    return undefined;
  }
  const code = (
    error as {
      code?: unknown;
    }
  ).code;
  return typeof code === "string" && code.trim().length > 0
    ? code.trim()
    : undefined;
}
export function getErrorStatusCode(error: unknown): number | undefined {
  if (!error || typeof error !== "object") {
    return undefined;
  }
  const statusCode = (
    error as {
      statusCode?: unknown;
    }
  ).statusCode;
  return typeof statusCode === "number" ? statusCode : undefined;
}
export function shouldRotateTaskSession(task: TaskRecord): boolean {
  const sessionState = getTaskSessionState(task);
  return task.retryable && sessionState.sessionCount < sessionState.maxSessions;
}
export function computeNextRetryAt(attemptNumber: number): Date | undefined {
  const now = Date.now();
  if (attemptNumber <= 1) {
    return new Date(now + 300000);
  }
  if (attemptNumber === 2) {
    return new Date(now + 900000);
  }
  if (attemptNumber === 3) {
    return new Date(now + 2000000);
  }
  return new Date(now + 300000);
}
export function getTaskSessionState(task: TaskRecord): {
  sessionName: string;
  sessionCount: number;
  maxSessions: number;
} {
  const taskRecord = task as TaskRecord & {
    sessionName?: unknown;
    sessionCount?: unknown;
    maxSessions?: unknown;
  };
  const sessionNameValue = taskRecord.sessionName;
  const sessionCountValue = taskRecord.sessionCount;
  const maxSessionsValue = taskRecord.maxSessions;
  const sessionName =
    typeof sessionNameValue === "string" && sessionNameValue.trim().length > 0
      ? sessionNameValue.trim()
      : buildTaskSessionBaseName(task);
  const sessionCount =
    typeof sessionCountValue === "number" && Number.isFinite(sessionCountValue)
      ? Math.max(Math.trunc(sessionCountValue), 1)
      : 1;
  const maxSessions =
    typeof maxSessionsValue === "number" && Number.isFinite(maxSessionsValue)
      ? Math.max(Math.trunc(maxSessionsValue), 1)
      : 2;
  return {
    sessionName,
    sessionCount,
    maxSessions,
  };
}
export function buildTaskSessionBaseName(task: TaskRecord): string {
  return `orchestrator:agent:${task.target.agentId}:task:${task._id}`;
}
export function buildTaskSessionName(
  task: TaskRecord,
  baseSessionName: string | undefined,
  sessionCount: number,
): string {
  const base =
    typeof baseSessionName === "string" && baseSessionName.trim().length > 0
      ? baseSessionName.trim().replace(/:session-attempt:\d+$/u, "")
      : buildTaskSessionBaseName(task);
  return `${base}:session-attempt:${Math.max(Math.trunc(sessionCount), 1)}`;
}
export function shouldResetConcreteTaskToQueued(task: TaskRecord): boolean {
  return task.status === "failed" || task.status === "canceled";
}
export function canSyncConcreteTaskDefinition(task: TaskRecord): boolean {
  return (
    task.status === "queued" ||
    task.status === "qa" ||
    task.status === "failed" ||
    task.status === "canceled"
  );
}
export function areValuesEquivalent(left: unknown, right: unknown): boolean {
  return JSON.stringify(left ?? null) === JSON.stringify(right ?? null);
}
export function areStringArraysEquivalent(
  left: string[],
  right: string[],
): boolean {
  return areValuesEquivalent(left, right);
}
export function buildConcreteTaskInputs(input: {
  parentTask: TaskRecord;
  plannedTask: PlannedTaskDefinition;
  phaseId: string;
  phaseName: string;
  phaseGoal?: string;
}): Record<string, unknown> {
  const parentInputs = asRecord(input.parentTask.inputs);
  const plannedInputs = asRecord(input.plannedTask.inputs);
  const plannedPhaseGoal = readOptionalString(plannedInputs, ["phaseGoal"]);
  return {
    ...(typeof parentInputs.projectName === "string"
      ? { projectName: parentInputs.projectName }
      : {}),
    ...(typeof parentInputs.projectRequest === "string"
      ? { projectRequest: parentInputs.projectRequest }
      : typeof parentInputs.prompt === "string"
        ? { projectRequest: parentInputs.prompt }
        : {}),
    ...(typeof parentInputs.request === "string"
      ? { request: parentInputs.request }
      : {}),
    ...(typeof parentInputs.appType === "string"
      ? { appType: parentInputs.appType }
      : {}),
    ...(typeof parentInputs.stack === "string"
      ? { stack: parentInputs.stack }
      : {}),
    ...(typeof parentInputs.deployment === "string"
      ? { deployment: parentInputs.deployment }
      : {}),
    ...(isRecord(parentInputs.phase) ? { phase: parentInputs.phase } : {}),
    ...plannedInputs,
    milestoneId: input.parentTask.milestoneId,
    phaseId: readOptionalString(plannedInputs, ["phaseId"]) ?? input.phaseId,
    phaseName:
      readOptionalString(plannedInputs, ["phaseName"]) ?? input.phaseName,
    ...((plannedPhaseGoal ?? input.phaseGoal)
      ? {
          phaseGoal: plannedPhaseGoal ?? input.phaseGoal,
        }
      : {}),
  };
}
export function sanitizeConcreteTaskInputsForDispatch(
  baseInputs: Record<string, unknown>,
  isQaDispatch: boolean,
): Record<string, unknown> {
  const sanitizedInputs = {
    ...baseInputs,
  };
  delete sanitizedInputs.taskPlan;
  delete sanitizedInputs.milestoneTaskGraph;
  delete sanitizedInputs.dependencyTaskContext;
  delete sanitizedInputs.enrichment;
  delete sanitizedInputs.enrichmentTask;
  if (isQaDispatch) {
    delete sanitizedInputs.systemTaskType;
  }
  return sanitizedInputs;
}
export function extractConcreteTaskEnrichmentContext(
  enrichmentTask?: TaskRecord,
): Record<string, unknown> | undefined {
  if (!enrichmentTask || !enrichmentTask.outputs) {
    return undefined;
  }
  const outputRecord = asRecord(enrichmentTask.outputs);
  const nestedEnrichmentRecord = asRecord(outputRecord.enrichment);
  if (Object.keys(nestedEnrichmentRecord).length > 0) {
    return nestedEnrichmentRecord;
  }
  return Object.keys(outputRecord).length > 0 ? outputRecord : undefined;
}
export function isEnrichmentTask(task: TaskRecord): boolean {
  if (String(task.intent) === "enrich_task") {
    return true;
  }
  const taskInputs = asRecord(task.inputs);
  return readOptionalString(taskInputs, ["systemTaskType"]) === "enrichment";
}
export function extractExecutionTaskEnrichmentUpdates(
  outputs: unknown,
): Parameters<TasksServicePort["updateConcreteTaskExecution"]>[1] | null {
  const direct = asRecord(outputs);
  const nestedOutputs = asRecord(direct.outputs);
  const candidates = [
    asRecord(direct.enrichment),
    asRecord(nestedOutputs.enrichment),
    direct,
    nestedOutputs,
  ];
  for (const candidate of candidates) {
    const prompt = readOptionalString(candidate, ["prompt"]);
    const testingCriteria = normalizeStringArray(candidate.testingCriteria);
    const acceptanceCriteria = normalizeStringArray(
      candidate.acceptanceCriteria,
    );
    const requiredArtifacts = normalizeStringArray(candidate.requiredArtifacts);
    const hasUsableFields =
      typeof prompt === "string" ||
      testingCriteria.length > 0 ||
      acceptanceCriteria.length > 0 ||
      requiredArtifacts.length > 0;
    if (!hasUsableFields) {
      continue;
    }
    return {
      ...(typeof prompt === "string" ? { prompt } : {}),
      ...(testingCriteria.length > 0 ? { testingCriteria } : {}),
      ...(acceptanceCriteria.length > 0 ? { acceptanceCriteria } : {}),
      ...(requiredArtifacts.length > 0 ? { requiredArtifacts } : {}),
    };
  }
  return null;
}
export function extractMilestoneReviewOutcome(
  outputs: unknown,
): MilestoneReviewOutcome | null {
  const record = asRecord(outputs);
  const nestedOutputs = asRecord(record.outputs);
  const source = Object.keys(record).length > 0 ? record : nestedOutputs;
  const decisionValue =
    readOptionalString(source, ["decision", "reviewDecision", "outcome"]) ??
    readOptionalString(nestedOutputs, [
      "decision",
      "reviewDecision",
      "outcome",
    ]);
  const decision =
    decisionValue === "pass" || decisionValue === "patch"
      ? decisionValue
      : decisionValue?.toLowerCase() === "pass"
        ? "pass"
        : decisionValue?.toLowerCase() === "patch"
          ? "patch"
          : undefined;
  if (!decision) {
    return null;
  }
  const metAcceptanceCriteria = normalizeStringArray(
    source.metAcceptanceCriteria ??
      source.criteriaMet ??
      nestedOutputs.metAcceptanceCriteria ??
      nestedOutputs.criteriaMet,
  );
  const missingOrBrokenItems = normalizeStringArray(
    source.missingOrBrokenItems ??
      source.issues ??
      nestedOutputs.missingOrBrokenItems ??
      nestedOutputs.issues,
  );
  const summary =
    readOptionalString(source, ["summary"]) ??
    readOptionalString(nestedOutputs, ["summary"]);
  const patchMilestone = normalizePatchMilestonePlan(
    source.patchMilestone ??
      source.patch ??
      nestedOutputs.patchMilestone ??
      nestedOutputs.patch,
    missingOrBrokenItems,
  );
  return {
    decision,
    ...(summary ? { summary } : {}),
    metAcceptanceCriteria,
    missingOrBrokenItems,
    ...(patchMilestone ? { patchMilestone } : {}),
  };
}
export function normalizePatchMilestonePlan(
  value: unknown,
  fallbackItems: string[],
): PatchMilestonePlan | null {
  const record = asRecord(value);
  if (Object.keys(record).length === 0 && fallbackItems.length === 0) {
    return null;
  }
  const title = readOptionalString(record, ["title", "name"]);
  const goal = readOptionalString(record, ["goal"]);
  const rawDescription = readOptionalString(record, ["description"]);
  const scope = normalizeStringArray(
    record.scope ?? record.deliverables ?? fallbackItems,
  );
  const acceptanceCriteria = normalizeStringArray(
    record.acceptanceCriteria ?? record.exitCriteria ?? fallbackItems,
  );
  if (!title && acceptanceCriteria.length === 0 && scope.length === 0) {
    return null;
  }
  return {
    title: title ?? "Patch milestone",
    ...(goal ? { goal } : {}),
    ...(rawDescription ? { description: rawDescription } : {}),
    scope,
    acceptanceCriteria,
  };
}
export function buildPatchMilestonePlan(input: {
  sourceMilestone: MilestoneRecord;
  reviewTaskId: string;
  outcome: MilestoneReviewOutcome;
}): PatchMilestonePlan {
  const reviewMarker = buildPatchReviewMarker(input.reviewTaskId);
  const fallbackTitle = `${input.sourceMilestone.title} Patch`;
  const requestedPatch = input.outcome.patchMilestone;
  const title = requestedPatch?.title?.trim().length
    ? requestedPatch.title.trim()
    : fallbackTitle;
  const goal = requestedPatch?.goal?.trim().length
    ? requestedPatch.goal.trim()
    : input.outcome.summary;
  const descriptionLines = [reviewMarker];
  if (
    typeof requestedPatch?.description === "string" &&
    requestedPatch.description.trim().length > 0
  ) {
    descriptionLines.push(requestedPatch.description.trim());
  }
  const scopeSource = requestedPatch?.scope.length
    ? requestedPatch.scope
    : input.outcome.missingOrBrokenItems;
  const acceptanceCriteriaSource = requestedPatch?.acceptanceCriteria.length
    ? requestedPatch.acceptanceCriteria
    : input.outcome.missingOrBrokenItems;
  return {
    title,
    ...(goal ? { goal } : {}),
    description: descriptionLines.join("\n\n"),
    scope: [...scopeSource],
    acceptanceCriteria: [...acceptanceCriteriaSource],
  };
}
export function buildPatchReviewMarker(reviewTaskId: string): string {
  return `Patch source review task: ${reviewTaskId}`;
}
export function isPatchMilestoneForReview(
  milestone: MilestoneRecord,
  reviewTaskId: string,
): boolean {
  return (
    typeof milestone.description === "string" &&
    milestone.description.includes(buildPatchReviewMarker(reviewTaskId))
  );
}
export function describePlannedTask(
  plannedTask: PlannedTaskDefinition,
  index: number,
): string {
  const label = plannedTask.localId ?? `task-${index + 1}`;
  return `planned task ${label} (intent=${plannedTask.intent})`;
}
export function createPlannedTaskValidationIssue(input: {
  code: string;
  message: string;
  stage: PlanningValidationIssue["stage"];
  plannerTask: TaskRecord;
  plannedTask: PlannedTaskDefinition;
  plannedTaskIndex: number;
  taskVariant?: PlanningValidationIssue["taskVariant"];
  field?: string;
  details?: Record<string, unknown>;
}): PlanningValidationIssue {
  return {
    code: input.code,
    message: input.message,
    stage: input.stage,
    plannedTaskIndex: input.plannedTaskIndex,
    ...(input.plannedTask.localId
      ? { taskLocalId: input.plannedTask.localId }
      : {}),
    taskIntent: input.plannedTask.intent,
    ...(input.taskVariant ? { taskVariant: input.taskVariant } : {}),
    ...(input.field ? { field: input.field } : {}),
    details: {
      jobId: input.plannerTask.jobId,
      milestoneId: input.plannerTask.milestoneId,
      plannerTaskId: input.plannerTask._id,
      plannedTaskIndex: input.plannedTaskIndex,
      plannedTaskLocalId: input.plannedTask.localId,
      plannedTaskIntent: input.plannedTask.intent,
      ...(input.details ?? {}),
    },
  };
}
export function createAggregatePlannedTaskValidationError(input: {
  plannerTask: TaskRecord;
  issues: PlanningValidationIssue[];
}): ServiceError {
  const firstIssue = input.issues[0];
  const extraIssueCount = input.issues.length > 1 ? input.issues.length - 1 : 0;
  const issueSummary =
    typeof firstIssue?.message === "string" &&
    firstIssue.message.trim().length > 0
      ? firstIssue.message.trim()
      : undefined;
  return createServiceError({
    code: "PLANNED_TASK_BATCH_INVALID",
    message: issueSummary
      ? `Phase task plan failed validation: ${issueSummary}${extraIssueCount > 0 ? ` (+${extraIssueCount} more issue${extraIssueCount === 1 ? "" : "s"})` : ""}`
      : `Phase task plan failed validation with ${input.issues.length} issue${input.issues.length === 1 ? "" : "s"}.`,
    statusCode: 422,
    retryable: true,
    details: {
      jobId: input.plannerTask.jobId,
      milestoneId: input.plannerTask.milestoneId,
      plannerTaskId: input.plannerTask._id,
      issues: input.issues,
    },
  });
}
export function buildTaskConstraints(
  fallback: TaskRecord["constraints"],
  override?: Record<string, unknown>,
): CreateTaskInput["constraints"] {
  const toolProfile =
    typeof override?.toolProfile === "string" &&
    override.toolProfile.trim().length > 0
      ? override.toolProfile.trim()
      : fallback.toolProfile;
  const sandbox =
    override?.sandbox === "off" ||
    override?.sandbox === "non-main" ||
    override?.sandbox === "all"
      ? override.sandbox
      : fallback.sandbox;
  const maxTokens =
    typeof override?.maxTokens === "number"
      ? override.maxTokens
      : typeof fallback.maxTokens === "number"
        ? fallback.maxTokens
        : undefined;
  const maxCost =
    typeof override?.maxCost === "number"
      ? override.maxCost
      : typeof fallback.maxCost === "number"
        ? fallback.maxCost
        : undefined;
  return {
    toolProfile,
    sandbox,
    ...(typeof maxTokens === "number" ? { maxTokens } : {}),
    ...(typeof maxCost === "number" ? { maxCost } : {}),
  };
}
export function getEnrichmentTaskSequence(
  phasePlannerSequence: number,
  taskIndex: number,
): number {
  return phasePlannerSequence + (taskIndex + 1) * 20;
}
export function getExecutionTaskSequence(
  phasePlannerSequence: number,
  taskIndex: number,
): number {
  return getEnrichmentTaskSequence(phasePlannerSequence, taskIndex) + 10;
}
export function buildPlannedTaskReferenceKeys(
  plannedTask: PlannedTaskDefinition,
  idempotencyKey: string,
): string[] {
  const referenceKeys = new Set<string>();
  if (plannedTask.localId) {
    referenceKeys.add(plannedTask.localId);
  }
  if (plannedTask.idempotencyKey) {
    referenceKeys.add(plannedTask.idempotencyKey);
  }
  referenceKeys.add(idempotencyKey);
  return Array.from(referenceKeys);
}
export function buildMilestoneTaskSnapshots(
  tasks: TaskRecord[],
): Array<Record<string, unknown>> {
  return tasks.map((task) => buildMilestoneTaskSnapshot(task));
}
export function buildMilestoneTaskSnapshot(
  task: TaskRecord,
  options?: {
    includeInputs?: boolean;
    includeOutputs?: boolean;
  },
): Record<string, unknown> {
  const baseInputs = asRecord(task.inputs);
  const outputRecord = asRecord(task.outputs);
  const outputSummary = readOptionalString(outputRecord, [
    "summary",
    "resultSummary",
    "findingSummary",
  ]);
  return {
    taskId: task._id,
    intent: String(task.intent),
    targetAgentId: task.target.agentId,
    status: task.status,
    sequence: task.sequence,
    dependencies: [...task.dependencies],
    acceptanceCriteria: [...task.acceptanceCriteria],
    ...(outputSummary ? { summary: outputSummary } : {}),
    ...(readOptionalString(baseInputs, ["systemTaskType"])
      ? {
          systemTaskType: readOptionalString(baseInputs, ["systemTaskType"]),
        }
      : {}),
    ...(readOptionalString(baseInputs, ["plannedTaskLocalId"])
      ? {
          plannedTaskLocalId: readOptionalString(baseInputs, [
            "plannedTaskLocalId",
          ]),
        }
      : {}),
    ...(options?.includeInputs ? { inputs: task.inputs } : {}),
    ...(options?.includeOutputs && task.outputs
      ? { outputs: task.outputs }
      : {}),
    ...(task.artifacts.length > 0 ? { artifacts: [...task.artifacts] } : {}),
    ...(task.errors.length > 0 ? { errors: [...task.errors] } : {}),
    ...(typeof task.lastError === "string" && task.lastError.trim().length > 0
      ? { lastError: task.lastError }
      : {}),
  };
}
export function extractPlannedPhases(outputs: unknown): PlannedPhase[] {
  const record = asRecord(outputs);
  const direct = Array.isArray(record.phases) ? record.phases : [];
  const nestedOutputs = asRecord(record.outputs);
  const nested = Array.isArray(nestedOutputs.phases)
    ? nestedOutputs.phases
    : [];
  const source = direct.length > 0 ? direct : nested;
  return source
    .map((item) => normalizePlannedPhase(item))
    .filter((item): item is PlannedPhase => item !== null);
}
export function normalizePlannedPhase(value: unknown): PlannedPhase | null {
  const record = asRecord(value);
  const phaseId = readOptionalString(record, ["phaseId", "id"]);
  const name = readOptionalString(record, ["name", "title"]);
  const goal = readOptionalString(record, ["goal"]);
  const description = readOptionalString(record, ["description"]);
  const dependsOn = normalizeStringArray(
    record.dependsOn ?? record.dependencies,
  );
  const inputs = isRecord(record.inputs) ? record.inputs : {};
  const deliverables = normalizeStringArray(record.deliverables);
  const exitCriteria = normalizeStringArray(
    record.exitCriteria ?? record.acceptanceCriteria,
  );
  if (!phaseId || !name) {
    return null;
  }
  return {
    phaseId,
    name,
    ...(goal ? { goal } : {}),
    ...(description ? { description } : {}),
    dependsOn,
    inputs,
    deliverables,
    exitCriteria,
    raw: record,
  };
}
export function extractPlannedTasks(outputs: unknown): PlannedTaskDefinition[] {
  const record = asRecord(outputs);
  const direct = Array.isArray(record.tasks) ? record.tasks : [];
  const nestedOutputs = asRecord(record.outputs);
  const nested = Array.isArray(nestedOutputs.tasks) ? nestedOutputs.tasks : [];
  const source = direct.length > 0 ? direct : nested;
  return source
    .map((item) => normalizePlannedTask(item))
    .filter((item): item is PlannedTaskDefinition => item !== null);
}
export function normalizePlannedTask(
  value: unknown,
): PlannedTaskDefinition | null {
  const record = asRecord(value);
  const target = asRecord(record.target);
  const intent = readOptionalString(record, ["intent"]);
  const localId = readOptionalString(record, [
    "taskId",
    "id",
    "key",
    "localId",
    "slug",
  ]);
  const targetAgentId =
    readOptionalString(target, ["agentId"]) ??
    readOptionalString(record, ["agentId", "targetAgentId"]);
  const inputs = isRecord(record.inputs) ? record.inputs : {};
  const constraints = isRecord(record.constraints)
    ? record.constraints
    : undefined;
  const requiredArtifacts = normalizeStringArray(record.requiredArtifacts);
  const acceptanceCriteria = normalizeStringArray(record.acceptanceCriteria);
  const idempotencyKey = readOptionalString(record, ["idempotencyKey"]);
  const dependsOn = normalizeStringArray(
    record.dependsOn ?? record.dependencies,
  );
  if (!intent) {
    return null;
  }
  return {
    ...(localId ? { localId } : {}),
    intent,
    ...(targetAgentId ? { targetAgentId } : {}),
    inputs,
    ...(constraints ? { constraints } : {}),
    requiredArtifacts,
    acceptanceCriteria,
    ...(idempotencyKey ? { idempotencyKey } : {}),
    dependsOn,
  };
}
export function serializeUnknownError(error: unknown): Record<string, unknown> {
  if (error instanceof Error) {
    const details = extractErrorDetailsRecord(error);
    return {
      message: error.message,
      ...(getErrorCode(error) ? { code: getErrorCode(error) } : {}),
      ...(typeof getErrorStatusCode(error) === "number"
        ? { statusCode: getErrorStatusCode(error) }
        : {}),
      ...(details ? { details } : {}),
    };
  }
  if (isRecord(error)) {
    return error;
  }
  return {
    value: error,
  };
}
export function normalizeBatchValidationIssues(
  result: unknown,
): PlanningValidationIssue[] {
  const record = asRecord(result);
  const rawIssues = Array.isArray(record.issues) ? record.issues : [];
  return rawIssues
    .map((issue) => normalizeBatchValidationIssue(issue))
    .filter((issue): issue is PlanningValidationIssue => issue !== null);
}
export function normalizeBatchValidationIssue(
  issue: unknown,
): PlanningValidationIssue | null {
  const record = asRecord(issue);
  const code = readOptionalString(record, ["code"]);
  const message = readOptionalString(record, ["message"]);
  const stageCandidate = readOptionalString(record, ["stage"]);
  const taskLocalId = readOptionalString(record, ["taskLocalId"]);
  const taskIntent = readOptionalString(record, ["taskIntent"]);
  const field = readOptionalString(record, ["field"]);
  const taskVariant = readOptionalString(record, ["taskVariant"]);
  const ownerLabel = readOptionalString(record, ["ownerLabel"]);
  const idempotencyKey = readOptionalString(record, ["idempotencyKey"]);
  const operationKindCandidate = readOptionalString(record, ["operationKind"]);
  const plannedTaskIndex =
    typeof record.plannedTaskIndex === "number"
      ? record.plannedTaskIndex
      : undefined;
  const operationIndex =
    typeof record.operationIndex === "number"
      ? record.operationIndex
      : undefined;
  if (!code || !message) {
    return null;
  }
  const normalizedStage: PlanningValidationIssue["stage"] =
    stageCandidate === "planned-task-validation" ||
    stageCandidate === "planned-task-graph" ||
    stageCandidate === "expanded-task-validation" ||
    stageCandidate === "batch-preflight"
      ? stageCandidate
      : "batch-preflight";
  return {
    code,
    message,
    stage: normalizedStage,
    ...(typeof plannedTaskIndex === "number" ? { plannedTaskIndex } : {}),
    ...(typeof operationIndex === "number" ? { operationIndex } : {}),
    ...(taskLocalId ? { taskLocalId } : {}),
    ...(taskIntent ? { taskIntent } : {}),
    ...(field ? { field } : {}),
    ...(ownerLabel ? { ownerLabel } : {}),
    ...(idempotencyKey ? { idempotencyKey } : {}),
    ...(operationKindCandidate === "create" ||
    operationKindCandidate === "update"
      ? { operationKind: operationKindCandidate }
      : {}),
    ...(taskVariant === "enrichment" ||
    taskVariant === "execution" ||
    taskVariant === "review"
      ? { taskVariant }
      : {}),
    ...(isRecord(record.details) ? { details: record.details } : {}),
  };
}
export function extractErrorDetailsRecord(
  error: unknown,
): Record<string, unknown> | undefined {
  if (!error || typeof error !== "object") {
    return undefined;
  }
  const details = (
    error as {
      details?: unknown;
    }
  ).details;
  return isRecord(details) ? details : undefined;
}
export function describeMilestonePlanningOperation(
  details: Record<string, unknown> | undefined,
): string | undefined {
  if (!details) {
    return undefined;
  }
  const stage = readOptionalString(details, ["stage"]);
  const operationKind = readOptionalString(details, ["operationKind"]);
  const ownerLabel = readOptionalString(details, ["ownerLabel"]);
  const operationIndex =
    typeof details.operationIndex === "number"
      ? details.operationIndex
      : undefined;
  const parts: string[] = [];
  if (stage) {
    parts.push(stage);
  }
  if (operationKind) {
    parts.push(
      typeof operationIndex === "number" && operationIndex >= 0
        ? `${operationKind}[${operationIndex}]`
        : operationKind,
    );
  } else if (typeof operationIndex === "number" && operationIndex >= 0) {
    parts.push(`operation[${operationIndex}]`);
  }
  if (ownerLabel) {
    parts.push(`for ${ownerLabel}`);
  }
  return parts.length > 0 ? parts.join(" ") : undefined;
}
export function describeMilestonePlanningProgress(
  details: Record<string, unknown> | undefined,
  fallback?: {
    createdCount?: number;
  },
): string | undefined {
  const createdTasksSoFar = Array.isArray(details?.createdTasksSoFar)
    ? details.createdTasksSoFar.length
    : undefined;
  const updatedTasksSoFar = Array.isArray(details?.updatedTasksSoFar)
    ? details.updatedTasksSoFar.length
    : undefined;
  const createdCount =
    typeof createdTasksSoFar === "number"
      ? createdTasksSoFar
      : typeof fallback?.createdCount === "number"
        ? fallback.createdCount
        : undefined;
  const updatedCount =
    typeof updatedTasksSoFar === "number" ? updatedTasksSoFar : 0;
  if (
    typeof createdCount !== "number" &&
    typeof updatedTasksSoFar !== "number"
  ) {
    return undefined;
  }
  const progressParts: string[] = [];
  if (typeof createdCount === "number") {
    progressParts.push(
      `${createdCount} task${createdCount === 1 ? "" : "s"} created`,
    );
  }
  if (typeof updatedCount === "number") {
    progressParts.push(
      `${updatedCount} task${updatedCount === 1 ? "" : "s"} updated`,
    );
  }
  return progressParts.length > 0
    ? `progress before failure: ${progressParts.join(", ")}`
    : undefined;
}
export function buildDispatchFailureMessage(
  error: unknown,
  fallback: string,
): string {
  const baseMessage =
    typeof fallback === "string" && fallback.trim().length > 0
      ? fallback.trim()
      : "Unknown dispatch error";
  const details = extractErrorDetailsRecord(error);
  if (!details) {
    return baseMessage;
  }
  const issueMessages = Array.isArray(details.issues)
    ? details.issues
        .map((issue) => {
          const normalized = normalizeBatchValidationIssue(issue);
          return normalized?.message;
        })
        .filter(
          (message): message is string =>
            typeof message === "string" && message.trim().length > 0,
        )
    : [];
  if (issueMessages.length > 0) {
    return `${baseMessage} First issue: ${issueMessages[0]}${
      issueMessages.length > 1
        ? ` (+${issueMessages.length - 1} more issue${issueMessages.length - 1 === 1 ? "" : "s"})`
        : ""
    }`;
  }
  const operationSummary = describeMilestonePlanningOperation(details);
  const progressSummary = describeMilestonePlanningProgress(details);
  const causeMessage = readOptionalString(details, ["causeMessage", "message"]);
  const enriched = [
    baseMessage,
    operationSummary ? `Operation: ${operationSummary}.` : undefined,
    progressSummary ? `${progressSummary}.` : undefined,
    causeMessage && causeMessage !== baseMessage
      ? `Cause: ${causeMessage}.`
      : undefined,
  ]
    .filter(
      (part): part is string =>
        typeof part === "string" && part.trim().length > 0,
    )
    .join(" ");
  return enriched.length > 0 ? enriched : baseMessage;
}
export function asCreateTaskIntent(intent: string): CreateTaskInput["intent"] {
  return intent as CreateTaskInput["intent"];
}
export function normalizeTaskArtifactRefs(value: unknown): string[] {
  if (!Array.isArray(value)) {
    return [];
  }
  const refs = new Set<string>();
  for (const item of value) {
    if (typeof item === "string") {
      const normalized = item.trim();
      if (normalized.length > 0) {
        refs.add(extractArtifactName(normalized));
      }
      continue;
    }
    if (typeof item !== "object" || item === null) {
      continue;
    }
    const candidate = item as Record<string, unknown>;
    const explicitName =
      typeof candidate.name === "string"
        ? candidate.name.trim()
        : typeof candidate.fileName === "string"
          ? candidate.fileName.trim()
          : "";
    const relativePath =
      typeof candidate.relativePath === "string"
        ? candidate.relativePath.trim()
        : typeof candidate.path === "string"
          ? candidate.path.trim()
          : "";
    const ref =
      explicitName.length > 0
        ? explicitName
        : relativePath.length > 0
          ? extractArtifactName(relativePath)
          : "";
    if (ref.length > 0) {
      refs.add(ref);
    }
  }
  return Array.from(refs);
}
export function extractArtifactName(relativePath: string): string {
  const parts = relativePath.split("/").filter(Boolean);
  return parts[parts.length - 1] ?? relativePath;
}
export function buildFailureMessage(
  result: OpenClawTaskStatusResponse,
): string {
  if (Array.isArray(result.errors) && result.errors.length > 0) {
    return result.errors.join("; ");
  }
  if (typeof result.summary === "string" && result.summary.trim().length > 0) {
    return result.summary.trim();
  }
  return `OpenClaw task ended with status "${result.status}".`;
}
export function readOptionalString(
  source: Record<string, unknown> | undefined,
  keys: string[],
): string | undefined {
  if (!source) {
    return undefined;
  }
  for (const key of keys) {
    const value = source[key];
    if (typeof value === "string" && value.trim().length > 0) {
      return value.trim();
    }
  }
  return undefined;
}
export function normalizeStringArray(value: unknown): string[] {
  if (!Array.isArray(value)) {
    return [];
  }
  return value
    .filter((item): item is string => typeof item === "string")
    .map((item) => item.trim())
    .filter((item) => item.length > 0);
}
export function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
export function asRecord(value: unknown): Record<string, unknown> {
  return isRecord(value) ? value : {};
}
