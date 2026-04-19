import { promptConfig, type PromptGlobalLayerKey } from "./prompt.config";

export type PromptRetryInput = {
  attemptNumber: number;
  failureType?: string;
  failureMessage?: string;
  previousSummary?: string;
  previousOutputs?: Record<string, unknown>;
  previousArtifacts?: string[];
  previousErrors?: string[];
};

export type PromptBuildInput = {
  agentId: string;
  intent?: string;
  taskPrompt: string;
  acceptanceCriteria?: string[];
  testingCriteria?: string[];
  constraints?: string[];
  projectPath?: string;
  retry?: PromptRetryInput;
};

export type PromptContextValue =
  | string
  | number
  | boolean
  | null
  | Record<string, unknown>
  | unknown[];

export type ProjectPhasePlanningPromptInput = {
  projectName: string;
  userPrompt: string;
  appType?: PromptContextValue;
  stack?: PromptContextValue;
  deployment?: PromptContextValue;
};

export type MilestoneTaskPlanningPromptInput = {
  projectName: string;
  milestoneId?: string;
  milestoneTitle?: string;
  milestoneGoal?: string;
  milestoneDescription?: string;
  milestoneScope?: string[];
  milestoneAcceptanceCriteria?: string[];
  projectSummary?: string;
  userPrompt?: string;
  title?: string;
  description?: string;
  goal?: string;
  scope?: string[];
  acceptanceCriteria?: string[];
  dependsOnMilestoneId?: string;
  order?: number;
  status?: string;
};

export type OpenClawTaskPayloadLike = {
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

export type OpenClawPromptBuildInput = {
  agentId: string;
  payload: OpenClawTaskPayloadLike;
};

type PromptAgentKey = keyof typeof promptConfig.agents;
type PromptIntentKey = keyof typeof promptConfig.intents;
type PromptRetryKey = keyof typeof promptConfig.retries;
type OutputReminderKey = keyof typeof promptConfig.outputReminders;

export class PromptService {
  buildTaskPrompt(input: PromptBuildInput): string {
    const agentKey = this.resolveAgentKey(input.agentId);
    const intentKey = this.resolveIntentKey(input.intent);

    if (this.isRetry(input.retry)) {
      return this.joinSections([
        this.renderGlobalLayers("session", "execution"),
        this.renderList(
          promptConfig.sections.role,
          this.getAgentRules(agentKey).role,
        ),
        this.renderList(
          promptConfig.sections.intent,
          this.getIntentRules(intentKey),
        ),
        this.renderRetryTaskContext(input),
        this.buildRetryBlock(input.retry),
        this.renderRequirements(input),
        this.renderOutputGuidance(agentKey),
      ]);
    }

    return this.joinSections([
      this.renderGlobalLayers("session", "execution"),
      this.renderList(
        promptConfig.sections.role,
        this.getAgentRules(agentKey).role,
      ),
      this.renderList(
        promptConfig.sections.intent,
        this.getIntentRules(intentKey),
      ),
      this.renderTaskContext(input),
      this.buildRetryBlock(input.retry),
      this.renderRequirements(input),
      this.renderOutputGuidance(agentKey),
    ]);
  }

  buildProjectPhasePlanningPrompt(
    input: ProjectPhasePlanningPromptInput,
  ): string {
    const details = [
      `Project name: ${input.projectName}`,
      ...this.optionalLine("App type", this.formatPromptValue(input.appType)),
      ...this.optionalLine("Stack", this.formatPromptValue(input.stack)),
      ...this.optionalLine(
        "Deployment",
        this.formatPromptValue(input.deployment),
      ),
      "",
      "Project request:",
      input.userPrompt,
    ];

    return this.joinSections([
      this.renderGlobalLayers("session", "planning"),
      this.renderList(
        promptConfig.sections.role,
        this.getAgentRules("product_owner").role,
      ),
      this.renderList(
        promptConfig.sections.intent,
        this.getIntentRules("plan_project_phases"),
      ),
      this.renderRawSection(promptConfig.sections.task, details),
      this.renderList(
        promptConfig.sections.output,
        this.getOutputReminder("planner"),
      ),
    ]);
  }

  buildMilestoneTaskPlanningPrompt(
    input: MilestoneTaskPlanningPromptInput,
  ): string {
    const milestoneTitle =
      input.milestoneTitle ?? input.title ?? "Unnamed Milestone";
    const milestoneGoal = input.milestoneGoal ?? input.goal;
    const milestoneDescription =
      input.milestoneDescription ?? input.description;
    const milestoneScope =
      input.milestoneScope && input.milestoneScope.length > 0
        ? input.milestoneScope
        : input.scope;
    const milestoneAcceptanceCriteria =
      input.milestoneAcceptanceCriteria &&
      input.milestoneAcceptanceCriteria.length > 0
        ? input.milestoneAcceptanceCriteria
        : input.acceptanceCriteria;

    const details = [
      `Project name: ${input.projectName}`,
      ...this.optionalLine("Milestone id", input.milestoneId),
      `Milestone title: ${milestoneTitle}`,
      ...this.optionalLine("Milestone goal", milestoneGoal),
      ...this.optionalLine("Milestone description", milestoneDescription),
      ...this.optionalLine(
        "Depends on milestone id",
        input.dependsOnMilestoneId,
      ),
      ...(typeof input.order === "number"
        ? [`Milestone order: ${String(input.order)}`]
        : []),
      ...this.optionalLine("Milestone status", input.status),
      ...(input.projectSummary
        ? ["", "Project summary:", input.projectSummary]
        : []),
    ];

    return this.joinSections([
      this.renderGlobalLayers("session", "planning"),
      this.renderList(
        promptConfig.sections.role,
        this.getAgentRules("project_manager").role,
      ),
      this.renderList(
        promptConfig.sections.intent,
        this.getIntentRules("plan_phase_tasks"),
      ),
      this.renderRawSection(promptConfig.sections.task, details),
      this.renderList("Milestone Scope", milestoneScope),
      this.renderList(
        "Milestone Acceptance Criteria",
        milestoneAcceptanceCriteria,
      ),
      this.renderList(
        promptConfig.sections.output,
        this.getOutputReminder("planner"),
      ),
    ]);
  }

  public buildPhaseTaskPlanningPrompt(
    input: MilestoneTaskPlanningPromptInput,
  ): string {
    return this.buildMilestoneTaskPlanningPrompt(input);
  }

  buildResponsesPrompt(input: OpenClawPromptBuildInput): string {
    const normalizedInputs = this.normalizePayloadInputs(input.payload.inputs);
    const normalizedPayload =
      normalizedInputs === input.payload.inputs
        ? input.payload
        : { ...input.payload, inputs: normalizedInputs };
    const normalizedInput =
      normalizedPayload === input.payload
        ? input
        : { ...input, payload: normalizedPayload };

    const constraints = this.normalizeConstraints(
      normalizedPayload.constraints,
    );
    const retry = this.buildRetryInput(normalizedPayload);
    const explicitIntent = this.readString(normalizedPayload.intent);

    if (explicitIntent === "plan_project_phases") {
      return this.buildProjectPhasePlanningPromptFromPayload(
        normalizedInput,
        constraints,
        retry,
      );
    }

    if (this.isPhaseTaskPlanningIntent(explicitIntent)) {
      return this.buildMilestoneTaskPlanningPromptFromPayload(
        normalizedInput,
        constraints,
        retry,
      );
    }

    if (!explicitIntent) {
      if (this.shouldUseProjectPhasePlanningPrompt(normalizedPayload.inputs)) {
        return this.buildProjectPhasePlanningPromptFromPayload(
          normalizedInput,
          constraints,
          retry,
        );
      }

      if (this.shouldUseMilestoneTaskPlanningPrompt(normalizedPayload.inputs)) {
        return this.buildMilestoneTaskPlanningPromptFromPayload(
          normalizedInput,
          constraints,
          retry,
        );
      }
    }

    const taskPrompt = this.readTaskPrompt(normalizedPayload.inputs);

    return this.buildTaskPrompt({
      agentId: input.agentId,
      intent: normalizedPayload.intent,
      taskPrompt,
      ...(normalizedPayload.acceptanceCriteria
        ? { acceptanceCriteria: normalizedPayload.acceptanceCriteria }
        : {}),
      ...(constraints ? { constraints } : {}),
      ...(retry ? { retry } : {}),
    });
  }

  public buildProjectPhasePlanningPromptFromPayload(
    input: OpenClawPromptBuildInput,
    constraints?: string[],
    _retry?: PromptRetryInput,
  ): string {
    const payloadInputs = input.payload.inputs;
    const retry = this.buildRetryInput(input.payload);

    const projectName =
      this.readString(payloadInputs.projectName) ?? "Unnamed Project";
    const rawProjectRequest =
      this.readRawProjectRequest(payloadInputs) ??
      this.readString(payloadInputs.userPrompt) ??
      "No project request was provided.";

    const details = [
      `Project name: ${projectName}`,
      ...this.optionalLine(
        "App type",
        this.readPromptValue(payloadInputs.appType),
      ),
      ...this.optionalLine("Stack", this.readPromptValue(payloadInputs.stack)),
      ...this.optionalLine(
        "Deployment",
        this.readPromptValue(payloadInputs.deployment),
      ),
      "",
      "Project request:",
      rawProjectRequest,
    ];

    const requirements = this.renderRequirements({
      agentId: input.agentId,
      taskPrompt: "",
      ...(constraints ? { constraints } : {}),
      ...(input.payload.acceptanceCriteria
        ? { acceptanceCriteria: input.payload.acceptanceCriteria }
        : {}),
    });

    if (this.isRetry(retry)) {
      return this.joinSections([
        // this.renderGlobalLayers("session", "planning"),
        // this.renderList(
        //   promptConfig.sections.role,
        //   this.getAgentRules("product_owner").role,
        // ),
        // this.renderList(
        //   promptConfig.sections.intent,
        //   this.getIntentRules("plan_project_phases"),
        // ),
        this.renderRawSection(promptConfig.sections.task, [
          "Continue the same phase-planning task in this session.",
          `Project name: ${projectName}`,
          "Use the existing session context for the full project brief and prior work.",
          "Focus on correcting the specific failure below instead of rebuilding the full plan from scratch.",
        ]),
        this.buildRetryBlock(retry),
        requirements,
        this.renderList(
          promptConfig.sections.output,
          this.getOutputReminder("planner"),
        ),
      ]);
    }

    return this.joinSections([
      this.renderGlobalLayers("session", "planning"),
      this.renderList(
        promptConfig.sections.role,
        this.getAgentRules("product_owner").role,
      ),
      this.renderList(
        promptConfig.sections.intent,
        this.getIntentRules("plan_project_phases"),
      ),
      this.renderRawSection(promptConfig.sections.task, details),
      this.buildRetryBlock(retry),
      requirements,
      this.renderList(
        promptConfig.sections.output,
        this.getOutputReminder("planner"),
      ),
    ]);
  }

  public buildMilestoneTaskPlanningPromptFromPayload(
    input: OpenClawPromptBuildInput,
    constraints?: string[],
    _retry?: PromptRetryInput,
  ): string {
    const payloadInputs = input.payload.inputs;
    const retry = this.buildRetryInput(input.payload);
    const milestone = this.readRecord(payloadInputs.milestone);
    const phase = this.readRecord(payloadInputs.phase);

    const projectName =
      this.readString(payloadInputs.projectName) ?? "Unnamed Project";
    const projectSummary =
      this.readString(payloadInputs.projectSummary) ??
      this.readString(milestone?.projectSummary);
    const milestoneId =
      this.readString(payloadInputs.milestoneId) ??
      this.readString(payloadInputs._id) ??
      this.readString(payloadInputs.id) ??
      this.readString(milestone?._id) ??
      this.readString(milestone?.milestoneId) ??
      this.readString(milestone?.id);
    const milestoneTitle =
      this.readString(payloadInputs.milestoneTitle) ??
      this.readString(payloadInputs.title) ??
      this.readString(milestone?.title) ??
      this.readString(milestone?.name);
    const milestoneGoal =
      this.readString(payloadInputs.milestoneGoal) ??
      this.readString(payloadInputs.goal) ??
      this.readString(milestone?.goal);
    const milestoneDescription =
      this.readString(payloadInputs.milestoneDescription) ??
      this.readString(payloadInputs.description) ??
      this.readString(milestone?.description);
    const milestoneOrder =
      this.readNumber(payloadInputs.order) ?? this.readNumber(milestone?.order);
    const milestoneStatus =
      this.readString(payloadInputs.status) ??
      this.readString(milestone?.status);
    const dependsOnMilestoneId =
      this.readString(payloadInputs.dependsOnMilestoneId) ??
      this.readString(milestone?.dependsOnMilestoneId);
    const scope = this.readStringArray(payloadInputs.milestoneScope);
    const scopeFromRecordFields =
      scope.length > 0 ? scope : this.readStringArray(payloadInputs.scope);
    const milestoneScope =
      scopeFromRecordFields.length > 0
        ? scopeFromRecordFields
        : this.readStringArray(milestone?.scope);
    const milestoneAcceptanceInput = this.readStringArray(
      payloadInputs.milestoneAcceptanceCriteria,
    );
    const milestoneAcceptanceFromRecordFields =
      milestoneAcceptanceInput.length > 0
        ? milestoneAcceptanceInput
        : this.readStringArray(payloadInputs.acceptanceCriteria);
    const milestoneAcceptanceCriteria =
      milestoneAcceptanceFromRecordFields.length > 0
        ? milestoneAcceptanceFromRecordFields
        : this.readStringArray(milestone?.acceptanceCriteria);

    const phaseId =
      this.readString(payloadInputs.phaseId) ??
      this.readString(phase?.phaseId) ??
      this.readString(phase?.id);
    const phaseName =
      this.readString(payloadInputs.phaseName) ??
      this.readString(phase?.name) ??
      "Unnamed Phase";
    const phaseGoal =
      this.readString(payloadInputs.phaseGoal) ?? this.readString(phase?.goal);
    const phaseDescription =
      this.readString(payloadInputs.phaseDescription) ??
      this.readString(phase?.description);
    const dependsOn = this.readStringArray(payloadInputs.phaseDependsOn);
    const phaseDependsOn =
      dependsOn.length > 0 ? dependsOn : this.readStringArray(phase?.dependsOn);
    const deliverables = this.readStringArray(payloadInputs.phaseDeliverables);
    const phaseDeliverables =
      deliverables.length > 0
        ? deliverables
        : this.readStringArray(phase?.deliverables);
    const exitCriteria = this.readStringArray(payloadInputs.phaseExitCriteria);
    const phaseExitCriteria =
      exitCriteria.length > 0
        ? exitCriteria
        : this.readStringArray(phase?.exitCriteria);

    const details = [
      `Project name: ${projectName}`,
      ...this.optionalLine("Milestone id", milestoneId),
      ...this.optionalLine("Milestone title", milestoneTitle),
      ...this.optionalLine("Milestone goal", milestoneGoal),
      ...this.optionalLine("Milestone description", milestoneDescription),
      ...this.optionalLine("Depends on milestone id", dependsOnMilestoneId),
      ...(typeof milestoneOrder === "number"
        ? [`Milestone order: ${String(milestoneOrder)}`]
        : []),
      ...this.optionalLine("Milestone status", milestoneStatus),
      ...(projectSummary ? ["", "Project summary:", projectSummary] : []),
      ...(milestoneTitle ||
      milestoneGoal ||
      milestoneDescription ||
      dependsOnMilestoneId ||
      typeof milestoneOrder === "number" ||
      milestoneStatus ||
      projectSummary
        ? [""]
        : []),
      ...this.optionalLine("Phase id", phaseId),
      `Phase name: ${phaseName}`,
      ...this.optionalLine("Phase goal", phaseGoal),
      ...this.optionalLine("Phase description", phaseDescription),
      ...(phaseDependsOn.length > 0
        ? ["", "Phase dependencies:", ...phaseDependsOn]
        : []),
    ];

    const requirements = this.renderRequirements({
      agentId: input.agentId,
      taskPrompt: "",
      ...(constraints ? { constraints } : {}),
      ...(input.payload.acceptanceCriteria
        ? { acceptanceCriteria: input.payload.acceptanceCriteria }
        : {}),
    });

    const phaseTaskIntentKey = this.resolvePhaseTaskPlanningIntentKey();

    if (this.isRetry(retry)) {
      return this.joinSections([
        // this.renderGlobalLayers("session", "planning"),
        // this.renderList(
        //   promptConfig.sections.role,
        //   this.getAgentRules("project_manager").role,
        // ),
        // this.renderList(
        //   promptConfig.sections.intent,
        //   this.getIntentRules(phaseTaskIntentKey),
        // ),
        this.renderRawSection(promptConfig.sections.task, [
          "Continue the same phase-task planning task in this session.",
          `Project name: ${projectName}`,
          ...this.optionalLine("Milestone title", milestoneTitle),
          `Phase name: ${phaseName}`,
          "Use the existing session context for the full milestone and phase details.",
          "Focus on correcting the specific failure below instead of rebuilding the entire task plan from scratch.",
        ]),
        this.buildRetryBlock(retry),
        requirements,
        this.renderList(
          promptConfig.sections.output,
          this.getOutputReminder("planner"),
        ),
      ]);
    }

    return this.joinSections([
      this.renderGlobalLayers("session", "planning"),
      this.renderList(
        promptConfig.sections.role,
        this.getAgentRules("project_manager").role,
      ),
      this.renderList(
        promptConfig.sections.intent,
        this.getIntentRules(phaseTaskIntentKey),
      ),
      this.renderRawSection(promptConfig.sections.task, details),
      this.renderList("Milestone Scope", milestoneScope),
      this.renderList(
        "Milestone Acceptance Criteria",
        milestoneAcceptanceCriteria,
      ),
      this.renderList("Phase Deliverables", phaseDeliverables),
      this.renderList("Phase Exit Criteria", phaseExitCriteria),
      this.buildRetryBlock(retry),
      requirements,
      this.renderList(
        promptConfig.sections.output,
        this.getOutputReminder("planner"),
      ),
    ]);
  }

  public buildPhaseTaskPlanningPromptFromPayload(
    input: OpenClawPromptBuildInput,
    constraints?: string[],
    _retry?: PromptRetryInput,
  ): string {
    return this.buildMilestoneTaskPlanningPromptFromPayload(input, constraints);
  }

  private normalizePayloadInputs(
    inputs: Record<string, unknown>,
  ): Record<string, unknown> {
    const embeddedInputs = this.readEmbeddedPromptRecord(inputs);

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

  private shouldUseProjectPhasePlanningPrompt(
    inputs: Record<string, unknown>,
  ): boolean {
    return Boolean(
      this.readString(inputs.projectName) &&
      (this.readString(inputs.projectRequest) ||
        this.readString(inputs.userPrompt)) &&
      !this.shouldUseMilestoneTaskPlanningPrompt(inputs),
    );
  }

  private shouldUseMilestoneTaskPlanningPrompt(
    inputs: Record<string, unknown>,
  ): boolean {
    const phase = this.readRecord(inputs.phase);

    return Boolean(
      this.readString(inputs.milestoneId) &&
      (this.readString(inputs.phaseId) ||
        this.readString(inputs.phaseName) ||
        this.readString(inputs.phaseGoal) ||
        phase),
    );
  }

  private isPhaseTaskPlanningIntent(intent?: string): boolean {
    return intent === "plan_milestone_tasks" || intent === "plan_phase_tasks";
  }

  private resolvePhaseTaskPlanningIntentKey(): PromptIntentKey {
    return (
      "plan_phase_tasks" in promptConfig.intents
        ? "plan_phase_tasks"
        : "plan_milestone_tasks"
    ) as PromptIntentKey;
  }

  private readPromptValue(value: unknown): string | undefined {
    if (typeof value === "undefined") {
      return undefined;
    }

    return this.formatPromptValue(value);
  }

  private formatPromptValue(value: unknown): string | undefined {
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

      return this.stringifyValue(value);
    }

    if (this.readRecord(value)) {
      return this.stringifyValue(value);
    }

    return undefined;
  }

  private buildRetryInput(
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

  buildRetryBlock(input?: PromptRetryInput): string | undefined {
    if (!input) {
      return undefined;
    }

    const retryRules = [
      ...this.getGlobalLayer("retry"),
      ...promptConfig.retries.generic,
      ...this.getRetryRules(input.failureType),
    ];

    const lines: string[] = [
      ...retryRules.map((rule) => `- ${rule}`),
      `- Attempt number: ${input.attemptNumber}`,
      "- This is the same task in the same session. Reuse the existing session context instead of restarting the full task.",
    ];

    if (input.failureType) {
      lines.push(`- Failure type: ${input.failureType}`);
    }

    if (input.failureMessage) {
      lines.push(`- Failure message: ${input.failureMessage}`);
    }

    if (input.previousSummary) {
      lines.push(`- Previous summary: ${input.previousSummary}`);
    }

    if (input.previousArtifacts && input.previousArtifacts.length > 0) {
      lines.push(`- Previous artifacts: ${input.previousArtifacts.join(", ")}`);
    }

    const previousErrors = this.compactRetryErrors(input.previousErrors);
    if (previousErrors.length > 0) {
      lines.push(...previousErrors.map((error) => `- Prior error: ${error}`));
    }

    if (
      input.previousOutputs &&
      Object.keys(input.previousOutputs).length > 0
    ) {
      const outputKeys = Object.keys(input.previousOutputs);
      lines.push(
        `- Previous outputs were returned already and remain in session context. Output keys: ${outputKeys.join(", ")}`,
      );
    }

    return `${promptConfig.sections.retry}:
${lines.join("\n")}`;
  }

  private isRetry(input?: PromptRetryInput): boolean {
    return Boolean(input && input.attemptNumber > 1);
  }

  private renderRetryTaskContext(input: PromptBuildInput): string {
    const reminder = this.compactTaskReminder(input.taskPrompt);

    const lines = [
      "Continue the same assigned task in this session.",
      "Use the existing session context for the full task details and any prior work.",
      "Focus on correcting the specific failure below instead of restarting from scratch.",
      ...(reminder ? ["", `Task reminder: ${reminder}`] : []),
      ...(input.projectPath ? ["", `Project path: ${input.projectPath}`] : []),
    ];

    return this.renderRawSection(promptConfig.sections.task, lines) ?? "";
  }

  private compactTaskReminder(taskPrompt: string): string | undefined {
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

  private compactRetryErrors(errors?: string[]): string[] {
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

  private renderTaskContext(input: PromptBuildInput): string {
    const lines = [
      "Task prompt:",
      input.taskPrompt,
      ...(input.projectPath ? ["", `Project path: ${input.projectPath}`] : []),
    ];

    return this.renderRawSection(promptConfig.sections.task, lines) ?? "";
  }

  private renderRequirements(input: PromptBuildInput): string | undefined {
    return this.joinSections([
      this.renderList("Acceptance Criteria", input.acceptanceCriteria),
      this.renderList("Testing Criteria", input.testingCriteria),
      this.renderList("Constraints", input.constraints),
    ]);
  }

  private renderOutputGuidance(agentKey: PromptAgentKey): string | undefined {
    const agentRules = this.getAgentRules(agentKey);
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
      ...this.getOutputReminder(reminderKey),
    ];

    return this.renderList(promptConfig.sections.output, items);
  }

  private renderGlobalLayers(
    ...layers: PromptGlobalLayerKey[]
  ): string | undefined {
    const items = layers.flatMap((layer) => this.getGlobalLayer(layer));
    return this.renderList(promptConfig.sections.global, items);
  }

  private getGlobalLayer(layer: PromptGlobalLayerKey): string[] {
    return [...promptConfig.global[layer]];
  }

  private resolveAgentKey(agentId: string): PromptAgentKey {
    const normalized = agentId.replace(/-/g, "_");
    return (
      normalized in promptConfig.agents ? normalized : "default"
    ) as PromptAgentKey;
  }

  private resolveIntentKey(intent?: string): PromptIntentKey {
    if (!intent) {
      return "default";
    }

    const normalizedIntent =
      intent === "plan_phase_tasks" && !(intent in promptConfig.intents)
        ? "plan_milestone_tasks"
        : intent;

    return (
      normalizedIntent in promptConfig.intents ? normalizedIntent : "default"
    ) as PromptIntentKey;
  }

  private getAgentRules(agentId: PromptAgentKey) {
    return promptConfig.agents[agentId] ?? promptConfig.agents.default;
  }

  private getIntentRules(intent: PromptIntentKey): string[] {
    return [...promptConfig.intents[intent]];
  }

  private getRetryRules(failureType?: string): string[] {
    if (!failureType) {
      return [...promptConfig.retries.default];
    }

    const retryKey = this.resolveRetryKey(failureType);
    return [...promptConfig.retries[retryKey]];
  }

  private resolveRetryKey(failureType: string): PromptRetryKey {
    return (
      failureType in promptConfig.retries ? failureType : "default"
    ) as PromptRetryKey;
  }

  private getOutputReminder(key: OutputReminderKey): string[] {
    return [...promptConfig.outputReminders[key]];
  }

  private readEmbeddedPromptRecord(
    inputs: Record<string, unknown>,
  ): Record<string, unknown> | undefined {
    const promptValue = this.readString(inputs.prompt);
    if (!promptValue) {
      return undefined;
    }

    return this.extractEmbeddedJsonRecord(promptValue);
  }

  private extractEmbeddedJsonRecord(
    value: string,
  ): Record<string, unknown> | undefined {
    const trimmed = value.trim();

    const directRecord = this.parseJsonRecord(trimmed);
    if (directRecord) {
      return directRecord;
    }

    const marker = "Task prompt:";
    const markerIndex = trimmed.indexOf(marker);
    if (markerIndex !== -1) {
      const afterMarker = trimmed.slice(markerIndex + marker.length).trim();
      const markedRecord = this.extractFirstJsonRecord(afterMarker);
      if (markedRecord) {
        return markedRecord;
      }
    }

    return this.extractFirstJsonRecord(trimmed);
  }

  private extractFirstJsonRecord(
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
          return this.parseJsonRecord(candidate);
        }
      }
    }

    return undefined;
  }

  private parseJsonRecord(value: string): Record<string, unknown> | undefined {
    try {
      const parsed = JSON.parse(value);
      return this.readRecord(parsed);
    } catch {
      return undefined;
    }
  }

  private readRawProjectRequest(
    inputs: Record<string, unknown>,
  ): string | undefined {
    const directProjectRequest = this.readString(inputs.projectRequest);
    if (directProjectRequest) {
      return this.extractProjectRequestSection(directProjectRequest);
    }

    const promptValue = this.readString(inputs.prompt);
    if (promptValue) {
      return this.extractProjectRequestSection(promptValue);
    }

    return undefined;
  }

  private extractProjectRequestSection(value: string): string {
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

  private readString(value: unknown): string | undefined {
    return typeof value === "string" && value.trim().length > 0
      ? value.trim()
      : undefined;
  }

  private readNumber(value: unknown): number | undefined {
    return typeof value === "number" && Number.isFinite(value)
      ? value
      : undefined;
  }

  private readStringArray(value: unknown): string[] {
    if (!Array.isArray(value)) {
      return [];
    }

    return value.filter((item): item is string => typeof item === "string");
  }

  private readRecord(value: unknown): Record<string, unknown> | undefined {
    return value && typeof value === "object" && !Array.isArray(value)
      ? (value as Record<string, unknown>)
      : undefined;
  }

  private optionalLine(label: string, value?: string): string[] {
    return value ? [`${label}: ${value}`] : [];
  }

  private readTaskPrompt(inputs: Record<string, unknown>): string {
    const promptValue = inputs.prompt;

    if (typeof promptValue === "string" && promptValue.trim().length > 0) {
      return promptValue.trim();
    }

    const fallback = [
      this.readString(inputs.taskPrompt),
      this.readString(inputs.description),
      this.readString(inputs.summary),
      this.readString(inputs.instruction),
      this.readString(inputs.instructions),
      this.readString(inputs.userPrompt),
    ].find((value) => Boolean(value));

    if (fallback) {
      return fallback;
    }

    return "Complete the assigned task using the provided acceptance criteria and constraints.";
  }

  private normalizeConstraints(constraints?: {
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

  private renderList(
    title: string,
    items?: readonly string[],
  ): string | undefined {
    if (!items || items.length === 0) {
      return undefined;
    }

    return `${title}:\n${items.map((item) => `- ${item}`).join("\n")}`;
  }

  private renderRawSection(
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

  private joinSections(sections: Array<string | undefined>): string {
    return sections
      .filter((section): section is string => Boolean(section))
      .join("\n\n");
  }

  private stringifyValue(value: unknown): string {
    if (typeof value === "string") {
      return value;
    }

    try {
      return JSON.stringify(value, null, 2);
    } catch {
      return String(value);
    }
  }

  private indent(value: string, prefix = "  "): string {
    return value
      .split("\n")
      .map((line) => `${prefix}${line}`)
      .join("\n");
  }
}

export default PromptService;
