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
  systemTaskType?: string;
  phaseName?: string;
  phaseGoal?: string;
  plannedTaskIntent?: string;
  taskPlan?: Record<string, unknown>[];
  milestoneTaskGraph?: Record<string, unknown>;
  dependencyTaskContext?: Record<string, unknown>[];
  sourceTask?: Record<string, unknown>;
  enrichment?: Record<string, unknown>;
  enrichmentTask?: Record<string, unknown>;
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

    if (explicitIntent === "review_milestone") {
      return this.buildMilestoneReviewPromptFromPayload(
        normalizedInput,
        constraints,
        retry,
      );
    }

    if (explicitIntent === "enrich_task") {
      return this.buildTaskEnrichmentPromptFromPayload(
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

      if (this.shouldUseMilestoneReviewPrompt(normalizedPayload.inputs)) {
        return this.buildMilestoneReviewPromptFromPayload(
          normalizedInput,
          constraints,
          retry,
        );
      }

      if (
        this.readString(normalizedPayload.inputs.systemTaskType) ===
        "enrichment"
      ) {
        return this.buildTaskEnrichmentPromptFromPayload(
          normalizedInput,
          constraints,
          retry,
        );
      }
    }

    const taskPrompt = this.readTaskPrompt(normalizedPayload.inputs);
    const payloadAcceptanceCriteria =
      normalizedPayload.acceptanceCriteria &&
      normalizedPayload.acceptanceCriteria.length > 0
        ? normalizedPayload.acceptanceCriteria
        : this.readStringArray(normalizedPayload.inputs.acceptanceCriteria);
    const payloadTestingCriteria = this.readStringArray(
      normalizedPayload.inputs.testingCriteria,
    );
    const systemTaskType = this.readString(
      normalizedPayload.inputs.systemTaskType,
    );
    const phaseName = this.readString(normalizedPayload.inputs.phaseName);
    const phaseGoal = this.readString(normalizedPayload.inputs.phaseGoal);
    const plannedTaskIntent = this.readString(
      normalizedPayload.inputs.plannedTaskIntent,
    );
    const taskPlan = this.readRecordArray(normalizedPayload.inputs.taskPlan);
    const milestoneTaskGraph = this.readRecord(
      normalizedPayload.inputs.milestoneTaskGraph,
    );
    const dependencyTaskContext = this.readRecordArray(
      normalizedPayload.inputs.dependencyTaskContext,
    );
    const sourceTask = this.readRecord(normalizedPayload.inputs.sourceTask);
    const enrichment = this.readEnrichmentPayload(
      normalizedPayload.inputs.enrichment,
    );
    const enrichmentTask = this.readRecord(
      normalizedPayload.inputs.enrichmentTask,
    );

    return this.buildTaskPrompt({
      agentId: input.agentId,
      intent: normalizedPayload.intent,
      taskPrompt,
      ...(payloadAcceptanceCriteria.length > 0
        ? { acceptanceCriteria: payloadAcceptanceCriteria }
        : {}),
      ...(payloadTestingCriteria.length > 0
        ? { testingCriteria: payloadTestingCriteria }
        : {}),
      ...(constraints ? { constraints } : {}),
      ...(retry ? { retry } : {}),
      ...(systemTaskType ? { systemTaskType } : {}),
      ...(phaseName ? { phaseName } : {}),
      ...(phaseGoal ? { phaseGoal } : {}),
      ...(plannedTaskIntent ? { plannedTaskIntent } : {}),
      ...(taskPlan.length > 0 ? { taskPlan } : {}),
      ...(milestoneTaskGraph ? { milestoneTaskGraph } : {}),
      ...(dependencyTaskContext.length > 0 ? { dependencyTaskContext } : {}),
      ...(sourceTask ? { sourceTask } : {}),
      ...(enrichment ? { enrichment } : {}),
      ...(enrichmentTask ? { enrichmentTask } : {}),
    });
  }

  public buildTaskEnrichmentPromptFromPayload(
    input: OpenClawPromptBuildInput,
    constraints?: string[],
    _retry?: PromptRetryInput,
  ): string {
    const payloadInputs = input.payload.inputs;
    const retry = this.buildRetryInput(input.payload);
    const projectName =
      this.readString(payloadInputs.projectName) ?? "Unnamed Project";
    const phaseName = this.readString(payloadInputs.phaseName);
    const phaseGoal = this.readString(payloadInputs.phaseGoal);
    const sourceTask = this.readRecord(payloadInputs.sourceTask);
    const taskPlan = this.readRecordArray(payloadInputs.taskPlan);
    const dependencyTaskContext = this.readRecordArray(
      payloadInputs.dependencyTaskContext,
    );
    const milestoneTaskGraph = this.readRecord(
      payloadInputs.milestoneTaskGraph,
    );

    const details = [
      `Project name: ${projectName}`,
      ...this.optionalLine("Phase name", phaseName),
      ...this.optionalLine("Phase goal", phaseGoal),
      "",
      "Enrich exactly this task without changing the approved plan:",
      this.stringifyValue(sourceTask ?? {}),
    ];

    const sourceAcceptanceCriteria = this.readStringArray(
      sourceTask?.acceptanceCriteria,
    );
    const sourceTestingCriteria = this.readStringArray(
      this.readRecord(sourceTask?.inputs)?.testingCriteria,
    );

    const requirements = this.renderRequirements({
      agentId: input.agentId,
      taskPrompt: this.readTaskPrompt(payloadInputs),
      ...(sourceAcceptanceCriteria.length > 0
        ? { acceptanceCriteria: sourceAcceptanceCriteria }
        : {}),
      ...(sourceTestingCriteria.length > 0
        ? { testingCriteria: sourceTestingCriteria }
        : {}),
      ...(constraints ? { constraints } : {}),
      ...(taskPlan.length > 0 ? { taskPlan } : {}),
      ...(milestoneTaskGraph ? { milestoneTaskGraph } : {}),
      ...(dependencyTaskContext.length > 0 ? { dependencyTaskContext } : {}),
      ...(sourceTask ? { sourceTask } : {}),
      systemTaskType: "enrichment",
      ...(phaseName ? { phaseName } : {}),
      ...(phaseGoal ? { phaseGoal } : {}),
    });

    if (this.isRetry(retry)) {
      return this.joinSections([
        this.renderRawSection(promptConfig.sections.task, [
          "Continue the same task-enrichment work in this session.",
          `Project name: ${projectName}`,
          ...(phaseName ? [`Phase name: ${phaseName}`] : []),
          "Use the existing session context, the source task, and the full task plan to improve this one task only.",
          "Focus on correcting the specific failure below without changing the approved plan.",
        ]),
        this.renderSourceTask(sourceTask),
        this.renderTaskPlan(taskPlan),
        this.renderDependencyTaskContext(dependencyTaskContext),
        this.renderMilestoneTaskGraphSection(milestoneTaskGraph),
        this.buildRetryBlock(retry),
        requirements,
        this.renderList(
          promptConfig.sections.output,
          this.getOutputReminder("enrichment"),
        ),
      ]);
    }

    return this.joinSections([
      this.renderGlobalLayers("session", "execution"),
      this.renderList(
        promptConfig.sections.role,
        this.getAgentRules("project_manager").role,
      ),
      this.renderList(
        promptConfig.sections.intent,
        this.getIntentRules("enrich_task" as PromptIntentKey),
      ),
      this.renderRawSection(promptConfig.sections.task, details),
      this.renderTaskPlan(taskPlan),
      this.renderDependencyTaskContext(dependencyTaskContext),
      this.renderMilestoneTaskGraphSection(milestoneTaskGraph),
      requirements,
      this.buildRetryBlock(retry),
      this.renderList(
        promptConfig.sections.output,
        this.getOutputReminder("enrichment"),
      ),
    ]);
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

  public buildMilestoneReviewPromptFromPayload(
    input: OpenClawPromptBuildInput,
    constraints?: string[],
    _retry?: PromptRetryInput,
  ): string {
    const payloadInputs = input.payload.inputs;
    const retry = this.buildRetryInput(input.payload);
    const milestone = this.readRecord(payloadInputs.milestone);
    const phase = this.readRecord(payloadInputs.phase);
    const reviewContract = this.readRecord(payloadInputs.reviewContract);
    const milestoneExecution = this.readRecord(
      payloadInputs.milestoneExecution,
    );

    const projectName =
      this.readString(payloadInputs.projectName) ?? "Unnamed Project";
    const projectBrief = this.readMilestoneReviewProjectBrief(payloadInputs);
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
      this.readString(milestone?.name) ??
      "Unnamed Milestone";
    const milestoneGoal =
      this.readString(payloadInputs.milestoneGoal) ??
      this.readString(payloadInputs.goal) ??
      this.readString(milestone?.goal);
    const milestoneDescription =
      this.readString(payloadInputs.milestoneDescription) ??
      this.readString(payloadInputs.description) ??
      this.readString(milestone?.description);
    const milestoneOrder =
      this.readNumber(payloadInputs.milestoneOrder) ??
      this.readNumber(payloadInputs.order) ??
      this.readNumber(milestone?.order);
    const milestoneStatus =
      this.readString(payloadInputs.milestoneStatus) ??
      this.readString(payloadInputs.status) ??
      this.readString(milestone?.status);
    const dependsOnMilestoneId =
      this.readString(payloadInputs.dependsOnMilestoneId) ??
      this.readString(milestone?.dependsOnMilestoneId);

    const milestoneScopeInput = this.readStringArray(
      payloadInputs.milestoneScope,
    );
    const milestoneScope =
      milestoneScopeInput.length > 0
        ? milestoneScopeInput
        : this.readStringArray(payloadInputs.scope).length > 0
          ? this.readStringArray(payloadInputs.scope)
          : this.readStringArray(milestone?.scope);

    const milestoneAcceptanceInput = this.readStringArray(
      payloadInputs.milestoneAcceptanceCriteria,
    );
    const milestoneAcceptanceCriteria =
      milestoneAcceptanceInput.length > 0
        ? milestoneAcceptanceInput
        : this.readStringArray(payloadInputs.acceptanceCriteria).length > 0
          ? this.readStringArray(payloadInputs.acceptanceCriteria)
          : this.readStringArray(milestone?.acceptanceCriteria);

    const phaseId =
      this.readString(payloadInputs.phaseId) ??
      this.readString(phase?.phaseId) ??
      this.readString(phase?.id);
    const phaseName =
      this.readString(payloadInputs.phaseName) ??
      this.readString(phase?.name) ??
      "Milestone Review";
    const phaseGoal =
      this.readString(payloadInputs.phaseGoal) ?? this.readString(phase?.goal);

    const reviewEvidence = this.readRecordArray(
      milestoneExecution?.tasks ?? payloadInputs.milestoneExecution,
    );
    const completedTaskCount =
      this.readNumber(payloadInputs.completedTaskCount) ??
      this.readNumber(milestoneExecution?.completedTaskCount) ??
      reviewEvidence.length;
    const allowedDecisions = this.readStringArray(
      reviewContract?.allowedDecisions ?? payloadInputs.reviewDecisionOptions,
    );
    const reviewDecisionOptions =
      allowedDecisions.length > 0 ? allowedDecisions : ["pass", "patch"];
    const patchRule =
      this.readString(reviewContract?.patchRule) ??
      this.readString(payloadInputs.patchRule);

    const details = [
      `Project name: ${projectName}`,
      ...this.optionalLine("Milestone id", milestoneId),
      `Milestone title: ${milestoneTitle}`,
      ...this.optionalLine("Milestone goal", milestoneGoal),
      ...this.optionalLine("Milestone description", milestoneDescription),
      ...this.optionalLine("Depends on milestone id", dependsOnMilestoneId),
      ...(typeof milestoneOrder === "number"
        ? [`Milestone order: ${String(milestoneOrder)}`]
        : []),
      ...this.optionalLine("Milestone status", milestoneStatus),
      ...this.optionalLine("Phase id", phaseId),
      `Phase name: ${phaseName}`,
      ...this.optionalLine("Phase goal", phaseGoal),
      ...(typeof completedTaskCount === "number"
        ? [`Completed milestone task count: ${String(completedTaskCount)}`]
        : []),
      ...(projectBrief ? ["", "Original project brief:", projectBrief] : []),
    ];

    const reviewInstructions = [
      "Review only the stated milestone scope and acceptance criteria.",
      "Decide whether the milestone passes as-is or needs a patch milestone.",
      `Allowed decisions: ${reviewDecisionOptions.join(" | ")}`,
      ...(patchRule ? [patchRule] : []),
      "Do not expand the scope.",
      "If a patch is needed, define only the smallest valid follow-up milestone required to satisfy the current milestone acceptance criteria.",
      "Base the decision on the milestone evidence summary below and the milestone acceptance criteria.",
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
        this.renderRawSection(promptConfig.sections.task, [
          "Continue the same milestone review in this session.",
          `Project name: ${projectName}`,
          `Milestone title: ${milestoneTitle}`,
          "Use the existing session context for detailed evidence and prior checks.",
          "Focus on producing a clean pass-or-patch decision grounded in the milestone acceptance criteria.",
        ]),
        this.renderList(promptConfig.sections.intent, reviewInstructions),
        this.renderList("Milestone Scope", milestoneScope),
        this.renderList(
          "Milestone Acceptance Criteria",
          milestoneAcceptanceCriteria,
        ),
        this.renderMilestoneExecutionEvidence(reviewEvidence),
        this.buildRetryBlock(retry),
        requirements,
        this.renderMilestoneReviewOutputGuidance(),
      ]);
    }

    return this.joinSections([
      this.renderGlobalLayers("session", "execution"),
      this.renderList(
        promptConfig.sections.role,
        this.getAgentRules("product_owner").role,
      ),
      this.renderList(promptConfig.sections.intent, reviewInstructions),
      this.renderRawSection(promptConfig.sections.task, details),
      this.renderList("Milestone Scope", milestoneScope),
      this.renderList(
        "Milestone Acceptance Criteria",
        milestoneAcceptanceCriteria,
      ),
      this.renderMilestoneExecutionEvidence(reviewEvidence),
      this.buildRetryBlock(retry),
      requirements,
      this.renderMilestoneReviewOutputGuidance(),
    ]);
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

  private shouldUseMilestoneReviewPrompt(
    inputs: Record<string, unknown>,
  ): boolean {
    const milestone = this.readRecord(inputs.milestone);
    const reviewContract = this.readRecord(inputs.reviewContract);
    const milestoneExecution = this.readRecord(inputs.milestoneExecution);

    return (
      Boolean(
        this.readString(inputs.milestoneId) ||
        this.readString(inputs.milestoneTitle) ||
        milestone,
      ) && Boolean(reviewContract || milestoneExecution)
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

    const taskPlanSection = this.shouldRenderGenericTaskPlan(input)
      ? this.renderTaskPlan(input.taskPlan)
      : undefined;

    return (
      this.joinSections([
        this.renderRawSection(promptConfig.sections.task, lines),
        this.renderSourceTask(input.sourceTask),
        taskPlanSection,
        this.renderDependencyTaskContext(input.dependencyTaskContext),
        this.renderMilestoneTaskGraphSection(input.milestoneTaskGraph),
        this.renderEnrichmentContext(input.enrichment),
        this.renderRelatedEnrichmentTask(input.enrichmentTask),
      ]) ?? ""
    );
  }

  private shouldRenderGenericTaskPlan(input: PromptBuildInput): boolean {
    if (!input.taskPlan || input.taskPlan.length === 0) {
      return false;
    }

    const agentKey = this.resolveAgentKey(input.agentId);

    if (agentKey === "project_manager" || agentKey === "product_owner") {
      return true;
    }

    if (
      input.intent === "plan_project_phases" ||
      input.intent === "plan_phase_tasks" ||
      input.intent === "plan_next_tasks" ||
      input.intent === "review_milestone" ||
      input.intent === "enrich_task"
    ) {
      return true;
    }

    return false;
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

  private readMilestoneReviewProjectBrief(
    inputs: Record<string, unknown>,
  ): string | undefined {
    const rawProjectRequest =
      this.readRawProjectRequest(inputs) ??
      this.readString(inputs.request) ??
      this.readString(inputs.userPrompt);

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

  private renderMilestoneExecutionEvidence(
    tasks: Record<string, unknown>[],
  ): string | undefined {
    if (tasks.length === 0) {
      return this.renderRawSection("Milestone Evidence Summary", [
        "No milestone execution evidence was provided. Base the review on the stated scope and acceptance criteria, and explain any uncertainty clearly.",
      ]);
    }

    const lines: string[] = [];

    for (const [index, task] of tasks.entries()) {
      if (index >= 6) {
        break;
      }

      const taskId =
        this.readString(task.taskId) ??
        this.readString(task._id) ??
        `task-${index + 1}`;
      const intent = this.readString(task.intent);
      const targetAgentId = this.readString(task.targetAgentId);
      const status = this.readString(task.status) ?? "unknown";
      const summary =
        this.readString(task.summary) ??
        this.readString(task.resultSummary) ??
        this.readString(task.findingSummary);

      lines.push(
        `${index + 1}. Task ${taskId}: status=${status}${intent ? `, intent=${intent}` : ""}${targetAgentId ? `, target=${targetAgentId}` : ""}`,
      );

      if (summary) {
        lines.push(`   summary: ${summary}`);
      }

      const acceptanceCriteria = this.readStringArray(
        task.acceptanceCriteria,
      ).slice(0, 2);
      if (acceptanceCriteria.length > 0) {
        lines.push(`   acceptance checks: ${acceptanceCriteria.join("; ")}`);
      }

      const outputs = this.readRecord(task.outputs);
      if (outputs) {
        const outputKeys = Object.keys(outputs);
        if (outputKeys.length > 0) {
          lines.push(`   output keys: ${outputKeys.slice(0, 6).join(", ")}`);
        }
      }

      const artifacts = this.readStringArray(task.artifacts);
      if (artifacts.length > 0) {
        lines.push(`   artifacts: ${artifacts.slice(0, 4).join(", ")}`);
      }

      const errors = this.readStringArray(task.errors);
      const lastError = this.readString(task.lastError);
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

    return this.renderRawSection("Milestone Evidence Summary", lines);
  }

  private renderMilestoneReviewOutputGuidance(): string | undefined {
    return this.renderRawSection(promptConfig.sections.output, [
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

  private readEnrichmentPayload(
    value: unknown,
  ): Record<string, unknown> | undefined {
    const record = this.readRecord(value);
    if (!record) {
      return undefined;
    }

    return this.readRecord(record.enrichment) ?? record;
  }

  private renderSourceTask(
    sourceTask?: Record<string, unknown>,
  ): string | undefined {
    if (!sourceTask) {
      return undefined;
    }

    return this.renderRawSection("Source Task", [
      this.stringifyValue(sourceTask),
    ]);
  }

  private renderTaskPlan(
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
        this.readString(task.localId) ??
        this.readString(task.taskId) ??
        `task-${index + 1}`;
      const intent = this.readString(task.intent) ?? "unknown";
      const targetAgentId =
        this.readString(task.targetAgentId) ??
        this.readString(this.readRecord(task.target)?.agentId);
      const dependsOn = this.readStringArray(task.dependsOn);
      const prompt = this.readString(this.readRecord(task.inputs)?.prompt);

      lines.push(
        `${index + 1}. ${localId}: intent=${intent}${targetAgentId ? `, target=${targetAgentId}` : ""}${dependsOn.length > 0 ? `, dependsOn=${dependsOn.join(", ")}` : ""}`,
      );

      if (prompt) {
        lines.push(`   prompt: ${this.compactTaskReminder(prompt) ?? prompt}`);
      }
    }

    if (tasks.length > 12) {
      lines.push(
        `Additional planned tasks omitted: ${String(tasks.length - 12)}`,
      );
    }

    return this.renderRawSection("Task Plan", lines);
  }

  private renderDependencyTaskContext(
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
        this.readString(task.taskId) ??
        this.readString(task._id) ??
        this.readString(task.localId) ??
        `dependency-${index + 1}`;
      const intent = this.readString(task.intent);
      const status = this.readString(task.status);
      const summary =
        this.readString(task.summary) ??
        this.readString(task.resultSummary) ??
        this.readString(task.findingSummary);

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

    return this.renderRawSection("Dependency Task Context", lines);
  }

  private renderMilestoneTaskGraphSection(
    graph?: Record<string, unknown>,
  ): string | undefined {
    if (!graph) {
      return undefined;
    }

    const currentTaskId = this.readString(graph.currentTaskId);
    const tasks = this.readRecordArray(graph.tasks);
    const lines = [
      ...this.optionalLine("Current task id", currentTaskId),
      ...(tasks.length > 0
        ? [`Task graph size: ${String(tasks.length)}`]
        : ["Task graph size: 0"]),
    ];

    return this.renderRawSection("Milestone Task Graph", lines);
  }

  private renderEnrichmentContext(
    enrichment?: Record<string, unknown>,
  ): string | undefined {
    if (!enrichment) {
      return undefined;
    }

    return this.renderRawSection("Enrichment Context", [
      this.stringifyValue(enrichment),
    ]);
  }

  private renderRelatedEnrichmentTask(
    enrichmentTask?: Record<string, unknown>,
  ): string | undefined {
    if (!enrichmentTask) {
      return undefined;
    }

    return this.renderRawSection("Related Enrichment Task", [
      this.stringifyValue(enrichmentTask),
    ]);
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

  private readRecordArray(value: unknown): Record<string, unknown>[] {
    if (!Array.isArray(value)) {
      return [];
    }

    return value.filter(
      (item): item is Record<string, unknown> =>
        Boolean(item) && typeof item === "object" && !Array.isArray(item),
    );
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
