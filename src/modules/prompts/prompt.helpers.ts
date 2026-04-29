import { promptConfig } from "./prompt.config";
import type {
  OpenClawPromptBuildInput,
  OpenClawTaskPayloadLike,
  OutputReminderKey,
  PromptAgentKey,
  PromptBuildInput,
  PromptGlobalLayerKey,
  PromptIntentKey,
  PromptRetryInput,
  PromptRetryKey,
} from "./prompt.types";

export function normalizePayloadInputs(
    inputs: Record<string, unknown>,
  ): Record<string, unknown> {
    const embeddedInputs = readEmbeddedPromptRecord(inputs);

    if (!embeddedInputs) {
      return inputs;
    }

    return {
      ...embeddedInputs,
      ...inputs,
      ...(embeddedInputs.milestone && !inputs.milestone
        ? { milestone: embeddedInputs.milestone }
        : {}),
      ...(embeddedInputs.phase && !inputs.phase
        ? { phase: embeddedInputs.phase }
        : {}),
    };
  }

export function shouldUseProjectUpdatePlanningPrompt(
    inputs: Record<string, unknown>,
  ): boolean {
    const updateSignals = [
      readString(inputs.requestType),
      readString(inputs.updateRequestType),
      readString(inputs.canonicalProjectRoot),
      readString(inputs.latestAcceptedMilestoneSummary),
      readString(inputs.latestMilestoneSummary),
      readString(inputs.latestReviewOutcome),
      readString(inputs.reviewOutcome),
      readString(inputs.userRequest),
      readString(inputs.request),
    ];

    return Boolean(
      readString(inputs.projectName) &&
      (inputs.isProjectUpdate === true ||
        updateSignals.some(
          (value) => typeof value === "string" && value.length > 0,
        )),
    );
  }

export function shouldUseProjectPhasePlanningPrompt(
    inputs: Record<string, unknown>,
  ): boolean {
    return Boolean(
      readString(inputs.projectName) &&
      (readString(inputs.projectRequest) ||
        readString(inputs.userPrompt)) &&
      !shouldUseProjectUpdatePlanningPrompt(inputs) &&
      !shouldUseMilestoneTaskPlanningPrompt(inputs),
    );
  }

export function shouldUseMilestoneTaskPlanningPrompt(
    inputs: Record<string, unknown>,
  ): boolean {
    const phase = readRecord(inputs.phase);

    return Boolean(
      readString(inputs.milestoneId) &&
      (readString(inputs.phaseId) ||
        readString(inputs.phaseName) ||
        readString(inputs.phaseGoal) ||
        phase),
    );
  }

export function shouldUseMilestoneReviewPrompt(
    inputs: Record<string, unknown>,
  ): boolean {
    const milestone = readRecord(inputs.milestone);
    const reviewContract = readRecord(inputs.reviewContract);
    const milestoneExecution = readRecord(inputs.milestoneExecution);

    return (
      Boolean(
        readString(inputs.milestoneId) ||
        readString(inputs.milestoneTitle) ||
        milestone,
      ) && Boolean(reviewContract || milestoneExecution)
    );
  }

export function isPhaseTaskPlanningIntent(intent?: string): boolean {
    return intent === "plan_milestone_tasks" || intent === "plan_phase_tasks";
  }

export function resolvePhaseTaskPlanningIntentKey(): PromptIntentKey {
    return (
      "plan_phase_tasks" in promptConfig.intents
        ? "plan_phase_tasks"
        : "plan_milestone_tasks"
    ) as PromptIntentKey;
  }

export function resolveProjectUpdatePlanningIntentKey(): PromptIntentKey {
    return (
      "plan_project_update" in promptConfig.intents
        ? "plan_project_update"
        : "plan_project_phases"
    ) as PromptIntentKey;
  }

export function readPromptValue(value: unknown): string | undefined {
    if (typeof value === "undefined") {
      return undefined;
    }

    return formatPromptValue(value);
  }

export function formatPromptValue(value: unknown): string | undefined {
    if (typeof value === "string") {
      const trimmed = value.trim();

      return trimmed.length > 0 ? trimmed : undefined;
    }

    if (
      typeof value === "number" ||
      typeof value === "boolean" ||
      value === null
    ) {
      return String(value);
    }

    if (Array.isArray(value)) {
      if (value.length === 0) {
        return undefined;
      }

      return stringifyValue(value);
    }

    if (readRecord(value)) {
      return stringifyValue(value);
    }

    return undefined;
  }

export function buildRetryInput(
    payload: OpenClawTaskPayloadLike,
  ): PromptRetryInput | undefined {
    const hasRetryContext =
      (typeof payload.attemptNumber === "number" &&
        payload.attemptNumber > 1) ||
      typeof payload.lastError === "string" ||
      (Array.isArray(payload.errors) && payload.errors.length > 0);

    if (!hasRetryContext) {
      return undefined;
    }

    return {
      attemptNumber: payload.attemptNumber ?? 1,
      ...(typeof payload.lastError === "string"
        ? { failureMessage: payload.lastError }
        : {}),
      ...(payload.outputs ? { previousOutputs: payload.outputs } : {}),
      ...(payload.artifacts ? { previousArtifacts: payload.artifacts } : {}),
      ...(payload.errors ? { previousErrors: payload.errors } : {}),
    };
  }

export function buildRetryBlock(input?: PromptRetryInput): string | undefined {
    if (!input) {
      return undefined;
    }

    const failureMessage =
      input.failureMessage ??
      compactRetryErrors(input.previousErrors).at(-1) ??
      promptConfig.content.defaults.missingRetryFailureMessage;

    return `${promptConfig.sections.retry}:\n- ${promptConfig.content.labels.failureMessage}: ${failureMessage}`;
  }

export function buildMinimalPayloadRetryPrompt(
    input: OpenClawPromptBuildInput,
    retry: PromptRetryInput,
  ): string {
    return (
      joinSections([
        renderRawSection(
          promptConfig.sections.task,
          resolvePayloadRetryTaskContext(input),
        ),
        buildRetryBlock(retry),
      ]) ?? ""
    );
  }

export function resolvePayloadRetryTaskContext(
    input: OpenClawPromptBuildInput,
  ): string[] {
    const payloadInputs = input.payload.inputs;
    const intent = readString(input.payload.intent);
    const projectName =
      readString(payloadInputs.projectName) ??
      promptConfig.content.defaults.unnamedProject;
    const projectId =
      readString(payloadInputs.projectId) ??
      readString(input.payload.projectId);
    const requestType = readString(payloadInputs.requestType);
    const phaseName =
      readString(payloadInputs.phaseName) ??
      promptConfig.content.defaults.unnamedPhase;
    const milestoneTitle =
      readString(payloadInputs.milestoneTitle) ??
      readString(payloadInputs.title) ??
      promptConfig.content.defaults.unnamedMilestone;

    if (
      intent === "plan_project_update" ||
      ((intent === "plan_project_phases" || !intent) &&
        shouldUseProjectUpdatePlanningPrompt(payloadInputs))
    ) {
      const context = promptConfig.content.retryTaskContexts.projectUpdate;
      return [
        context[0],
        ...(projectId
          ? [`${promptConfig.content.labels.projectId}: ${projectId}`]
          : []),
        `${promptConfig.content.labels.projectName}: ${projectName}`,
        ...(requestType
          ? [`${promptConfig.content.labels.requestType}: ${requestType}`]
          : []),
        ...context.slice(1),
      ];
    }

    if (intent === "plan_project_phases") {
      const context = promptConfig.content.retryTaskContexts.projectPhases;
      return [
        context[0],
        `${promptConfig.content.labels.projectName}: ${projectName}`,
        ...context.slice(1),
      ];
    }

    if (isPhaseTaskPlanningIntent(intent)) {
      const context = promptConfig.content.retryTaskContexts.phaseTasks;
      return [
        context[0],
        `${promptConfig.content.labels.projectName}: ${projectName}`,
        `${promptConfig.content.labels.milestoneTitle}: ${milestoneTitle}`,
        `${promptConfig.content.labels.phaseName}: ${phaseName}`,
        ...context.slice(1),
      ];
    }

    if (intent === "enrich_task") {
      const context = promptConfig.content.retryTaskContexts.enrichment;
      return [
        context[0],
        `${promptConfig.content.labels.projectName}: ${projectName}`,
        `${promptConfig.content.labels.phaseName}: ${phaseName}`,
        ...context.slice(1),
      ];
    }

    if (intent === "review_milestone") {
      const context = promptConfig.content.retryTaskContexts.milestoneReview;
      return [
        context[0],
        `${promptConfig.content.labels.projectName}: ${projectName}`,
        `${promptConfig.content.labels.milestoneTitle}: ${milestoneTitle}`,
        ...context.slice(1),
      ];
    }

    const taskPrompt = readTaskPrompt(payloadInputs);
    const reminder = compactTaskReminder(taskPrompt);

    return [
      ...promptConfig.content.retryTaskContexts.generic,
      ...(reminder
        ? ["", `${promptConfig.content.labels.taskReminder}: ${reminder}`]
        : []),
    ];
  }

export function isRetry(input?: PromptRetryInput): input is PromptRetryInput {
    return Boolean(input && input.attemptNumber > 1);
  }

export function renderRetryTaskContext(input: PromptBuildInput): string {
    const reminder = compactTaskReminder(input.taskPrompt);

    const lines = [
      "Continue the same assigned task in this session.",
      "Use the existing session context for the full task details and any prior work.",
      "Focus on correcting the specific failure below instead of restarting from scratch.",
      ...(reminder ? ["", `Task reminder: ${reminder}`] : []),
      ...(input.projectPath ? ["", `Project path: ${input.projectPath}`] : []),
    ];

    return renderRawSection(promptConfig.sections.task, lines) ?? "";
  }

export function compactTaskReminder(taskPrompt: string): string | undefined {
    const trimmed = taskPrompt.trim();
    if (!trimmed) {
      return undefined;
    }

    const normalized = trimmed.replace(/\s+/g, " ");
    if (normalized.length <= 220) {
      return normalized;
    }

    return `${normalized.slice(0, 217).replace(/\s+$/u, "")}...`;
  }

export function compactRetryErrors(errors?: string[]): string[] {
    if (!errors || errors.length === 0) {
      return [];
    }

    const uniqueErrors: string[] = [];

    for (const error of errors) {
      const trimmed = error.trim();
      if (!trimmed || uniqueErrors.indexOf(trimmed) !== -1) {
        continue;
      }

      uniqueErrors.push(trimmed);
    }

    return uniqueErrors.slice(-3);
  }

export function renderTaskContext(input: PromptBuildInput): string {
    const lines = [
      "Task prompt:",
      input.taskPrompt,
      ...(input.systemTaskType
        ? ["", `System task type: ${input.systemTaskType}`]
        : []),
      ...(input.plannedTaskIntent
        ? [`Planned task intent: ${input.plannedTaskIntent}`]
        : []),
      ...(input.phaseName ? [`Phase name: ${input.phaseName}`] : []),
      ...(input.phaseGoal ? [`Phase goal: ${input.phaseGoal}`] : []),
      ...(input.projectPath ? ["", `Project path: ${input.projectPath}`] : []),
    ];

    const taskPlanSection = shouldRenderGenericTaskPlan(input)
      ? renderTaskPlan(input.taskPlan)
      : undefined;

    return (
      joinSections([
        renderRawSection(promptConfig.sections.task, lines),
        renderSourceTask(input.sourceTask),
        taskPlanSection,
        renderDependencyTaskContext(input.dependencyTaskContext),
        renderMilestoneTaskGraphSection(input.milestoneTaskGraph),
        renderEnrichmentContext(input.enrichment),
        renderRelatedEnrichmentTask(input.enrichmentTask),
      ]) ?? ""
    );
  }

export function shouldRenderGenericTaskPlan(input: PromptBuildInput): boolean {
    if (!input.taskPlan || input.taskPlan.length === 0) {
      return false;
    }

    const agentKey = resolveAgentKey(input.agentId);

    if (agentKey === "project_manager" || agentKey === "product_owner") {
      return true;
    }

    if (
      input.intent === "plan_project_phases" ||
      input.intent === "plan_project_update" ||
      input.intent === "plan_phase_tasks" ||
      input.intent === "plan_next_tasks" ||
      input.intent === "review_milestone" ||
      input.intent === "enrich_task"
    ) {
      return true;
    }

    return false;
  }

export function renderRequirements(input: PromptBuildInput): string | undefined {
    return joinSections([
      renderList("Acceptance Criteria", input.acceptanceCriteria),
      renderList("Testing Criteria", input.testingCriteria),
      renderList("Constraints", input.constraints),
    ]);
  }

export function renderOutputGuidance(agentKey: PromptAgentKey): string | undefined {
    const agentRules = getAgentRules(agentKey);
    const reminderKey: OutputReminderKey =
      agentKey === "project_manager"
        ? "planner"
        : agentKey === "product_owner"
          ? "project_owner"
          : agentKey === "qa"
            ? "qa"
            : "default";

    const items = [
      ...agentRules.output,
      ...getOutputReminder(reminderKey),
    ];

    return renderList(promptConfig.sections.output, items);
  }

export function renderGlobalLayers(
    ...layers: PromptGlobalLayerKey[]
  ): string | undefined {
    const items = layers.flatMap((layer) => getGlobalLayer(layer));
    return renderList(promptConfig.sections.global, items);
  }

export function getGlobalLayer(layer: PromptGlobalLayerKey): string[] {
    return [...promptConfig.global[layer]];
  }

export function resolveAgentKey(agentId: string): PromptAgentKey {
    const normalized = agentId.replace(/-/g, "_");
    return (
      normalized in promptConfig.agents ? normalized : "default"
    ) as PromptAgentKey;
  }

export function resolveIntentKey(intent?: string): PromptIntentKey {
    if (!intent) {
      return "default";
    }

    const normalizedIntent =
      intent === "plan_phase_tasks" && !(intent in promptConfig.intents)
        ? "plan_milestone_tasks"
        : intent === "plan_project_update" && !(intent in promptConfig.intents)
          ? "plan_project_phases"
          : intent;

    return (
      normalizedIntent in promptConfig.intents ? normalizedIntent : "default"
    ) as PromptIntentKey;
  }

export function getAgentRules(agentId: PromptAgentKey) {
    return promptConfig.agents[agentId] ?? promptConfig.agents.default;
  }

export function getIntentRules(intent: PromptIntentKey): string[] {
    return [...promptConfig.intents[intent]];
  }

export function getRetryRules(failureType?: string): string[] {
    if (!failureType) {
      return [...promptConfig.retries.default];
    }

    const retryKey = resolveRetryKey(failureType);
    return [...promptConfig.retries[retryKey]];
  }

export function resolveRetryKey(failureType: string): PromptRetryKey {
    return (
      failureType in promptConfig.retries ? failureType : "default"
    ) as PromptRetryKey;
  }

export function getOutputReminder(key: OutputReminderKey): string[] {
    return [...promptConfig.outputReminders[key]];
  }

export function readEmbeddedPromptRecord(
    inputs: Record<string, unknown>,
  ): Record<string, unknown> | undefined {
    const promptValue = readString(inputs.prompt);
    if (!promptValue) {
      return undefined;
    }

    return extractEmbeddedJsonRecord(promptValue);
  }

export function extractEmbeddedJsonRecord(
    value: string,
  ): Record<string, unknown> | undefined {
    const trimmed = value.trim();

    const directRecord = parseJsonRecord(trimmed);
    if (directRecord) {
      return directRecord;
    }

    const marker = "Task prompt:";
    const markerIndex = trimmed.indexOf(marker);
    if (markerIndex !== -1) {
      const afterMarker = trimmed.slice(markerIndex + marker.length).trim();
      const markedRecord = extractFirstJsonRecord(afterMarker);
      if (markedRecord) {
        return markedRecord;
      }
    }

    return extractFirstJsonRecord(trimmed);
  }

export function extractFirstJsonRecord(
    value: string,
  ): Record<string, unknown> | undefined {
    const startIndex = value.indexOf("{");
    if (startIndex === -1) {
      return undefined;
    }

    let depth = 0;
    let inString = false;
    let isEscaped = false;

    for (let index = startIndex; index < value.length; index += 1) {
      const char = value[index];

      if (inString) {
        if (isEscaped) {
          isEscaped = false;
          continue;
        }

        if (char === "\\") {
          isEscaped = true;
          continue;
        }

        if (char === '"') {
          inString = false;
        }

        continue;
      }

      if (char === '"') {
        inString = true;
        continue;
      }

      if (char === "{") {
        depth += 1;
        continue;
      }

      if (char === "}") {
        depth -= 1;

        if (depth === 0) {
          const candidate = value.slice(startIndex, index + 1);
          return parseJsonRecord(candidate);
        }
      }
    }

    return undefined;
  }

export function parseJsonRecord(value: string): Record<string, unknown> | undefined {
    try {
      const parsed = JSON.parse(value);
      return readRecord(parsed);
    } catch {
      return undefined;
    }
  }

export function readRawUpdateRequest(
    inputs: Record<string, unknown>,
  ): string | undefined {
    const directUpdateRequest =
      readString(inputs.updateRequest) ??
      readString(inputs.userRequest) ??
      readString(inputs.request);

    if (directUpdateRequest) {
      return extractUpdateRequestSection(directUpdateRequest);
    }

    const promptValue = readString(inputs.prompt);
    if (promptValue) {
      return extractUpdateRequestSection(promptValue);
    }

    return undefined;
  }

export function readRawProjectRequest(
    inputs: Record<string, unknown>,
  ): string | undefined {
    const directProjectRequest = readString(inputs.projectRequest);
    if (directProjectRequest) {
      return extractProjectRequestSection(directProjectRequest);
    }

    const promptValue = readString(inputs.prompt);
    if (promptValue) {
      return extractProjectRequestSection(promptValue);
    }

    return undefined;
  }

export function extractProjectRequestSection(value: string): string {
    const trimmed = value.trim();
    const marker = "Project request:\n";
    const markerIndex = trimmed.indexOf(marker);

    if (markerIndex === -1) {
      return trimmed;
    }

    const afterMarker = trimmed.slice(markerIndex + marker.length);
    const nextSectionMatch = afterMarker.match(/\n\n[A-Z][A-Za-z ]+:\n/);

    if (!nextSectionMatch || typeof nextSectionMatch.index !== "number") {
      return afterMarker.trim();
    }

    return afterMarker.slice(0, nextSectionMatch.index).trim();
  }

export function extractUpdateRequestSection(value: string): string {
    const trimmed = value.trim();
    const marker = "Update request:\n";
    const markerIndex = trimmed.indexOf(marker);

    if (markerIndex === -1) {
      return extractProjectRequestSection(trimmed);
    }

    const afterMarker = trimmed.slice(markerIndex + marker.length);
    const nextSectionMatch = afterMarker.match(/\n\n[A-Z][A-Za-z ]+:\n/);

    if (!nextSectionMatch || typeof nextSectionMatch.index !== "number") {
      return afterMarker.trim();
    }

    return afterMarker.slice(0, nextSectionMatch.index).trim();
  }

export function readMilestoneReviewProjectBrief(
    inputs: Record<string, unknown>,
  ): string | undefined {
    const rawProjectRequest =
      readRawProjectRequest(inputs) ??
      readString(inputs.request) ??
      readString(inputs.userPrompt);

    if (!rawProjectRequest) {
      return undefined;
    }

    const normalized = rawProjectRequest.replace(/\s+/g, " ").trim();
    const looksLikeRenderedPrompt =
      normalized.includes("Global Guidance:") ||
      normalized.includes("Role Guidance:") ||
      normalized.includes("Intent Guidance:") ||
      normalized.includes("Task Context:") ||
      normalized.includes("Output Guidance:");

    if (looksLikeRenderedPrompt) {
      return undefined;
    }

    return normalized.length > 280
      ? `${normalized.slice(0, 277).replace(/\s+$/u, "")}...`
      : normalized;
  }

export function renderMilestoneExecutionEvidence(
    tasks: Record<string, unknown>[],
  ): string | undefined {
    if (tasks.length === 0) {
      return renderRawSection("Milestone Evidence Summary", [
        "No milestone execution evidence was provided. Base the review on the stated scope and acceptance criteria, and explain any uncertainty clearly.",
      ]);
    }

    const lines: string[] = [];

    for (const [index, task] of tasks.entries()) {
      if (index >= 6) {
        break;
      }

      const taskId =
        readString(task.taskId) ??
        readString(task._id) ??
        `task-${index + 1}`;
      const intent = readString(task.intent);
      const targetAgentId = readString(task.targetAgentId);
      const status = readString(task.status) ?? "unknown";
      const summary =
        readString(task.summary) ??
        readString(task.resultSummary) ??
        readString(task.findingSummary);

      lines.push(
        `${index + 1}. Task ${taskId}: status=${status}${intent ? `, intent=${intent}` : ""}${targetAgentId ? `, target=${targetAgentId}` : ""}`,
      );

      if (summary) {
        lines.push(`   summary: ${summary}`);
      }

      const acceptanceCriteria = readStringArray(
        task.acceptanceCriteria,
      ).slice(0, 2);
      if (acceptanceCriteria.length > 0) {
        lines.push(`   acceptance checks: ${acceptanceCriteria.join("; ")}`);
      }

      const outputs = readRecord(task.outputs);
      if (outputs) {
        const outputKeys = Object.keys(outputs);
        if (outputKeys.length > 0) {
          lines.push(`   output keys: ${outputKeys.slice(0, 6).join(", ")}`);
        }
      }

      const artifacts = readStringArray(task.artifacts);
      if (artifacts.length > 0) {
        lines.push(`   artifacts: ${artifacts.slice(0, 4).join(", ")}`);
      }

      const errors = readStringArray(task.errors);
      const lastError = readString(task.lastError);
      const issues = [...errors.slice(0, 2), ...(lastError ? [lastError] : [])];
      if (issues.length > 0) {
        lines.push(`   issues: ${issues.join("; ")}`);
      }
    }

    if (tasks.length > 6) {
      lines.push(
        `Additional evidence entries omitted: ${String(tasks.length - 6)}`,
      );
    }

    return renderRawSection("Milestone Evidence Summary", lines);
  }

export function renderMilestoneReviewOutputGuidance(): string | undefined {
    return renderRawSection(promptConfig.sections.output, [
      'Use this exact response envelope: {"taskId":"<task-id>","status":"succeeded|failed","summary":"","outputs":{"decision":"pass|patch","summary":"","metAcceptanceCriteria":[],"missingOrBrokenItems":[],"patchMilestone":{"title":"","goal":"","description":"","scope":[],"acceptanceCriteria":[]}},"artifacts":[],"errors":[]}',
      'Set outputs.decision to either "pass" or "patch".',
      "Ground the decision in the milestone scope, acceptance criteria, and the evidence summary above.",
      "If the milestone passes, briefly explain which acceptance criteria were met.",
      "If a patch is needed, include outputs.patchMilestone with only the smallest valid follow-up milestone needed to satisfy the current milestone.",
      "outputs.patchMilestone must include: title (string), goal (string), description (string), scope (string[]), acceptanceCriteria (string[]).",
      'Recommended outputs shape: {"decision":"pass|patch","summary":"","metAcceptanceCriteria":[],"missingOrBrokenItems":[],"patchMilestone":{"title":"","goal":"","description":"","scope":[],"acceptanceCriteria":[]}}',
      "Do not create execution tasks. Only approve the milestone or define the patch milestone.",
    ]);
  }

export function readEnrichmentPayload(
    value: unknown,
  ): Record<string, unknown> | undefined {
    const record = readRecord(value);
    if (!record) {
      return undefined;
    }

    return readRecord(record.enrichment) ?? record;
  }

export function renderSourceTask(
    sourceTask?: Record<string, unknown>,
  ): string | undefined {
    if (!sourceTask) {
      return undefined;
    }

    return renderRawSection("Source Task", [
      stringifyValue(sourceTask),
    ]);
  }

export function renderTaskPlan(
    tasks?: Record<string, unknown>[],
  ): string | undefined {
    if (!tasks || tasks.length === 0) {
      return undefined;
    }

    const lines: string[] = [];

    for (const [index, task] of tasks.entries()) {
      if (index >= 12) {
        break;
      }

      const localId =
        readString(task.localId) ??
        readString(task.taskId) ??
        `task-${index + 1}`;
      const intent = readString(task.intent) ?? "unknown";
      const targetAgentId =
        readString(task.targetAgentId) ??
        readString(readRecord(task.target)?.agentId);
      const dependsOn = readStringArray(task.dependsOn);
      const prompt = readString(readRecord(task.inputs)?.prompt);

      lines.push(
        `${index + 1}. ${localId}: intent=${intent}${targetAgentId ? `, target=${targetAgentId}` : ""}${dependsOn.length > 0 ? `, dependsOn=${dependsOn.join(", ")}` : ""}`,
      );

      if (prompt) {
        lines.push(`   prompt: ${compactTaskReminder(prompt) ?? prompt}`);
      }
    }

    if (tasks.length > 12) {
      lines.push(
        `Additional planned tasks omitted: ${String(tasks.length - 12)}`,
      );
    }

    return renderRawSection("Task Plan", lines);
  }

export function renderDependencyTaskContext(
    tasks?: Record<string, unknown>[],
  ): string | undefined {
    if (!tasks || tasks.length === 0) {
      return undefined;
    }

    const lines: string[] = [];

    for (const [index, task] of tasks.entries()) {
      if (index >= 8) {
        break;
      }

      const taskId =
        readString(task.taskId) ??
        readString(task._id) ??
        readString(task.localId) ??
        `dependency-${index + 1}`;
      const intent = readString(task.intent);
      const status = readString(task.status);
      const summary =
        readString(task.summary) ??
        readString(task.resultSummary) ??
        readString(task.findingSummary);

      lines.push(
        `${index + 1}. ${taskId}${intent ? `: intent=${intent}` : ""}${status ? `, status=${status}` : ""}`,
      );

      if (summary) {
        lines.push(`   summary: ${summary}`);
      }
    }

    if (tasks.length > 8) {
      lines.push(
        `Additional dependency context entries omitted: ${String(tasks.length - 8)}`,
      );
    }

    return renderRawSection("Dependency Task Context", lines);
  }

export function renderMilestoneTaskGraphSection(
    graph?: Record<string, unknown>,
  ): string | undefined {
    if (!graph) {
      return undefined;
    }

    const currentTaskId = readString(graph.currentTaskId);
    const tasks = readRecordArray(graph.tasks);
    const lines = [
      ...optionalLine("Current task id", currentTaskId),
      ...(tasks.length > 0
        ? [`Task graph size: ${String(tasks.length)}`]
        : ["Task graph size: 0"]),
    ];

    return renderRawSection("Milestone Task Graph", lines);
  }

export function renderEnrichmentContext(
    enrichment?: Record<string, unknown>,
  ): string | undefined {
    if (!enrichment) {
      return undefined;
    }

    return renderRawSection("Enrichment Context", [
      stringifyValue(enrichment),
    ]);
  }

export function renderRelatedEnrichmentTask(
    enrichmentTask?: Record<string, unknown>,
  ): string | undefined {
    if (!enrichmentTask) {
      return undefined;
    }

    return renderRawSection("Related Enrichment Task", [
      stringifyValue(enrichmentTask),
    ]);
  }

export function readString(value: unknown): string | undefined {
    return typeof value === "string" && value.trim().length > 0
      ? value.trim()
      : undefined;
  }

export function readNumber(value: unknown): number | undefined {
    return typeof value === "number" && Number.isFinite(value)
      ? value
      : undefined;
  }

export function readStringArray(value: unknown): string[] {
    if (!Array.isArray(value)) {
      return [];
    }

    return value.filter((item): item is string => typeof item === "string");
  }

export function readRecordArray(value: unknown): Record<string, unknown>[] {
    if (!Array.isArray(value)) {
      return [];
    }

    return value.filter(
      (item): item is Record<string, unknown> =>
        Boolean(item) && typeof item === "object" && !Array.isArray(item),
    );
  }

export function readRecord(value: unknown): Record<string, unknown> | undefined {
    return value && typeof value === "object" && !Array.isArray(value)
      ? (value as Record<string, unknown>)
      : undefined;
  }

export function optionalLine(label: string, value?: string): string[] {
    return value ? [`${label}: ${value}`] : [];
  }

export function readTaskPrompt(inputs: Record<string, unknown>): string {
    const promptValue = inputs.prompt;

    if (typeof promptValue === "string" && promptValue.trim().length > 0) {
      return promptValue.trim();
    }

    const fallback = [
      readString(inputs.taskPrompt),
      readString(inputs.description),
      readString(inputs.summary),
      readString(inputs.instruction),
      readString(inputs.instructions),
      readString(inputs.userPrompt),
    ].find((value) => Boolean(value));

    if (fallback) {
      return fallback;
    }

    return "Complete the assigned task using the provided acceptance criteria and constraints.";
  }

export function normalizeConstraints(constraints?: {
    toolProfile?: string;
    sandbox?: "off" | "non-main" | "all";
    maxTokens?: number;
    maxCost?: number;
  }): string[] | undefined {
    if (!constraints) {
      return undefined;
    }

    const items = [
      ...(constraints.toolProfile
        ? [`Tool profile: ${constraints.toolProfile}`]
        : []),
      ...(constraints.sandbox ? [`Sandbox mode: ${constraints.sandbox}`] : []),
      ...(typeof constraints.maxTokens === "number"
        ? [`Max tokens: ${constraints.maxTokens}`]
        : []),
      ...(typeof constraints.maxCost === "number"
        ? [`Max cost: ${constraints.maxCost}`]
        : []),
    ];

    return items.length > 0 ? items : undefined;
  }

export function renderList(
    title: string,
    items?: readonly string[],
  ): string | undefined {
    if (!items || items.length === 0) {
      return undefined;
    }

    return `${title}:\n${items.map((item) => `- ${item}`).join("\n")}`;
  }

export function renderRawSection(
    title: string,
    lines?: readonly string[],
  ): string | undefined {
    if (!lines || lines.length === 0) {
      return undefined;
    }

    const cleaned = lines.filter((line) => line !== undefined && line !== null);
    if (cleaned.length === 0) {
      return undefined;
    }

    return `${title}:\n${cleaned.join("\n")}`;
  }

export function joinSections(sections: Array<string | undefined>): string {
    return sections
      .filter((section): section is string => Boolean(section))
      .join("\n\n");
  }

export function stringifyValue(value: unknown): string {
    if (typeof value === "string") {
      return value;
    }

    try {
      return JSON.stringify(value, null, 2);
    } catch {
      return String(value);
    }
  }

export function indent(value: string, prefix = "  "): string {
    return value
      .split("\n")
      .map((line) => `${prefix}${line}`)
      .join("\n");
  }
