import { promptConfig } from "./prompt.config";
import {
  buildMinimalPayloadRetryPrompt,
  buildRetryBlock,
  buildRetryInput,
  formatPromptValue,
  getAgentRules,
  getIntentRules,
  getOutputReminder,
  isPhaseTaskPlanningIntent,
  isRetry,
  joinSections,
  normalizeConstraints,
  normalizePayloadInputs,
  optionalLine,
  readEnrichmentPayload,
  readMilestoneReviewProjectBrief,
  readNumber,
  readPromptValue,
  readRawProjectRequest,
  readRawUpdateRequest,
  readRecord,
  readRecordArray,
  readString,
  readStringArray,
  readTaskPrompt,
  renderDependencyTaskContext,
  renderGlobalLayers,
  renderList,
  renderMilestoneExecutionEvidence,
  renderMilestoneReviewOutputGuidance,
  renderMilestoneTaskGraphSection,
  renderOutputGuidance,
  renderRawSection,
  renderRequirements,
  renderRetryTaskContext,
  renderSourceTask,
  renderTaskContext,
  renderTaskPlan,
  resolveAgentKey,
  resolveIntentKey,
  resolvePhaseTaskPlanningIntentKey,
  resolveProjectUpdatePlanningIntentKey,
  shouldUseMilestoneReviewPrompt,
  shouldUseMilestoneTaskPlanningPrompt,
  shouldUseProjectPhasePlanningPrompt,
  shouldUseProjectUpdatePlanningPrompt,
  stringifyValue,
} from "./prompt.helpers";
import type {
  MilestoneTaskPlanningPromptInput,
  OpenClawPromptBuildInput,
  PromptBuildInput,
  PromptRetryInput,
  ProjectPhasePlanningPromptInput,
  ProjectUpdatePlanningPromptInput,
} from "./prompt.types";

const promptContent = promptConfig.content;
const promptDefaults = promptContent.defaults;
const promptLabels = promptContent.labels;
const retryTaskContexts = promptContent.retryTaskContexts;
const milestoneReviewCopy = promptContent.milestoneReview;

function renderLabeledValue(label: string, value: string | number): string {
  return `${label}: ${String(value)}`;
}

export class PromptService {
  buildTaskPrompt(input: PromptBuildInput): string {
    const agentKey = resolveAgentKey(input.agentId);
    const intentKey = resolveIntentKey(input.intent);

    if (isRetry(input.retry)) {
      return joinSections([
        renderRetryTaskContext(input),
        buildRetryBlock(input.retry),
      ]);
    }

    return joinSections([
      renderGlobalLayers("session", "execution"),
      renderList(promptConfig.sections.role, getAgentRules(agentKey).role),
      renderList(promptConfig.sections.intent, getIntentRules(intentKey)),
      renderTaskContext(input),
      buildRetryBlock(input.retry),
      renderRequirements(input),
      renderOutputGuidance(agentKey),
    ]);
  }

  buildProjectPhasePlanningPrompt(
    input: ProjectPhasePlanningPromptInput,
  ): string {
    const details = [
      renderLabeledValue(promptLabels.projectName, input.projectName),
      ...optionalLine(promptLabels.appType, formatPromptValue(input.appType)),
      ...optionalLine(promptLabels.stack, formatPromptValue(input.stack)),
      ...optionalLine(
        promptLabels.deployment,
        formatPromptValue(input.deployment),
      ),
      "",
      promptLabels.projectRequest,
      input.userPrompt,
    ];

    return joinSections([
      renderGlobalLayers("session", "planning"),
      renderList(
        promptConfig.sections.role,
        getAgentRules("product_owner").role,
      ),
      renderList(
        promptConfig.sections.intent,
        getIntentRules("plan_project_phases"),
      ),
      renderRawSection(promptConfig.sections.task, details),
      renderList(
        promptConfig.sections.output,
        getOutputReminder("project_owner"),
      ),
    ]);
  }

  buildProjectUpdatePlanningPrompt(
    input: ProjectUpdatePlanningPromptInput,
  ): string {
    const details = [
      ...optionalLine(promptLabels.projectId, input.projectId),
      renderLabeledValue(promptLabels.projectName, input.projectName),
      ...optionalLine(promptLabels.requestType, input.requestType),
      ...optionalLine(
        promptLabels.canonicalProjectRoot,
        input.canonicalProjectRoot,
      ),
      ...optionalLine(promptLabels.appType, formatPromptValue(input.appType)),
      ...optionalLine(promptLabels.stack, formatPromptValue(input.stack)),
      ...optionalLine(
        promptLabels.deployment,
        formatPromptValue(input.deployment),
      ),
      ...(input.latestAcceptedMilestoneSummary
        ? [
            "",
            promptLabels.latestAcceptedMilestoneSummary,
            input.latestAcceptedMilestoneSummary,
          ]
        : []),
      ...(input.latestReviewOutcome
        ? ["", promptLabels.latestReviewOutcome, input.latestReviewOutcome]
        : []),
      "",
      promptLabels.updateRequest,
      input.userRequest,
    ];

    return joinSections([
      renderGlobalLayers("session", "planning"),
      renderList(
        promptConfig.sections.role,
        getAgentRules("product_owner").role,
      ),
      renderList(
        promptConfig.sections.intent,
        getIntentRules(resolveProjectUpdatePlanningIntentKey()),
      ),
      renderRawSection(promptConfig.sections.task, details),
      renderList(
        promptConfig.sections.output,
        getOutputReminder("project_owner"),
      ),
    ]);
  }

  buildMilestoneTaskPlanningPrompt(
    input: MilestoneTaskPlanningPromptInput,
  ): string {
    const milestoneTitle =
      input.milestoneTitle ?? input.title ?? promptDefaults.unnamedMilestone;
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
      renderLabeledValue(promptLabels.projectName, input.projectName),
      ...optionalLine(promptLabels.milestoneId, input.milestoneId),
      renderLabeledValue(promptLabels.milestoneTitle, milestoneTitle),
      ...optionalLine(promptLabels.milestoneGoal, milestoneGoal),
      ...optionalLine(promptLabels.milestoneDescription, milestoneDescription),
      ...optionalLine(
        promptLabels.dependsOnMilestoneId,
        input.dependsOnMilestoneId,
      ),
      ...(typeof input.order === "number"
        ? [renderLabeledValue(promptLabels.milestoneOrder, input.order)]
        : []),
      ...optionalLine(promptLabels.milestoneStatus, input.status),
      ...(input.projectSummary
        ? ["", promptLabels.projectSummary, input.projectSummary]
        : []),
    ];

    return joinSections([
      renderGlobalLayers("session", "planning"),
      renderList(
        promptConfig.sections.role,
        getAgentRules("project_manager").role,
      ),
      renderList(
        promptConfig.sections.intent,
        getIntentRules("plan_phase_tasks"),
      ),
      renderRawSection(promptConfig.sections.task, details),
      renderList(promptLabels.milestoneScope, milestoneScope),
      renderList(
        promptLabels.milestoneAcceptanceCriteria,
        milestoneAcceptanceCriteria,
      ),
      renderList(promptConfig.sections.output, getOutputReminder("planner")),
    ]);
  }

  public buildPhaseTaskPlanningPrompt(
    input: MilestoneTaskPlanningPromptInput,
  ): string {
    return this.buildMilestoneTaskPlanningPrompt(input);
  }

  buildResponsesPrompt(input: OpenClawPromptBuildInput): string {
    const normalizedInputs = normalizePayloadInputs(input.payload.inputs);
    const normalizedPayload =
      normalizedInputs === input.payload.inputs
        ? input.payload
        : { ...input.payload, inputs: normalizedInputs };
    const normalizedInput =
      normalizedPayload === input.payload
        ? input
        : { ...input, payload: normalizedPayload };

    const constraints = normalizeConstraints(normalizedPayload.constraints);
    const retry = buildRetryInput(normalizedPayload);
    const explicitIntent = readString(normalizedPayload.intent);

    if (isRetry(retry)) {
      return buildMinimalPayloadRetryPrompt(normalizedInput, retry);
    }

    const isProjectUpdatePlanning =
      explicitIntent === "plan_project_update" ||
      ((explicitIntent === "plan_project_phases" || !explicitIntent) &&
        shouldUseProjectUpdatePlanningPrompt(normalizedPayload.inputs));

    if (isProjectUpdatePlanning) {
      return this.buildProjectUpdatePlanningPromptFromPayload(
        normalizedInput,
        constraints,
        retry,
      );
    }

    if (explicitIntent === "plan_project_phases") {
      return this.buildProjectPhasePlanningPromptFromPayload(
        normalizedInput,
        constraints,
        retry,
      );
    }

    if (isPhaseTaskPlanningIntent(explicitIntent)) {
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
      if (shouldUseProjectPhasePlanningPrompt(normalizedPayload.inputs)) {
        return this.buildProjectPhasePlanningPromptFromPayload(
          normalizedInput,
          constraints,
          retry,
        );
      }

      if (shouldUseMilestoneTaskPlanningPrompt(normalizedPayload.inputs)) {
        return this.buildMilestoneTaskPlanningPromptFromPayload(
          normalizedInput,
          constraints,
          retry,
        );
      }

      if (shouldUseMilestoneReviewPrompt(normalizedPayload.inputs)) {
        return this.buildMilestoneReviewPromptFromPayload(
          normalizedInput,
          constraints,
          retry,
        );
      }

      if (
        readString(normalizedPayload.inputs.systemTaskType) === "enrichment"
      ) {
        return this.buildTaskEnrichmentPromptFromPayload(
          normalizedInput,
          constraints,
          retry,
        );
      }
    }

    const taskPrompt = readTaskPrompt(normalizedPayload.inputs);
    const payloadAcceptanceCriteria =
      normalizedPayload.acceptanceCriteria &&
      normalizedPayload.acceptanceCriteria.length > 0
        ? normalizedPayload.acceptanceCriteria
        : readStringArray(normalizedPayload.inputs.acceptanceCriteria);
    const payloadTestingCriteria = readStringArray(
      normalizedPayload.inputs.testingCriteria,
    );
    const systemTaskType = readString(normalizedPayload.inputs.systemTaskType);
    const phaseName = readString(normalizedPayload.inputs.phaseName);
    const phaseGoal = readString(normalizedPayload.inputs.phaseGoal);
    const plannedTaskIntent = readString(
      normalizedPayload.inputs.plannedTaskIntent,
    );
    const taskPlan = readRecordArray(normalizedPayload.inputs.taskPlan);
    const milestoneTaskGraph = readRecord(
      normalizedPayload.inputs.milestoneTaskGraph,
    );
    const dependencyTaskContext = readRecordArray(
      normalizedPayload.inputs.dependencyTaskContext,
    );
    const sourceTask = readRecord(normalizedPayload.inputs.sourceTask);
    const enrichment = readEnrichmentPayload(
      normalizedPayload.inputs.enrichment,
    );
    const enrichmentTask = readRecord(normalizedPayload.inputs.enrichmentTask);

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
    const retry = buildRetryInput(input.payload);
    const projectName =
      readString(payloadInputs.projectName) ?? promptDefaults.unnamedProject;
    const phaseName = readString(payloadInputs.phaseName);
    const phaseGoal = readString(payloadInputs.phaseGoal);
    const sourceTask = readRecord(payloadInputs.sourceTask);
    const taskPlan = readRecordArray(payloadInputs.taskPlan);
    const dependencyTaskContext = readRecordArray(
      payloadInputs.dependencyTaskContext,
    );
    const milestoneTaskGraph = readRecord(payloadInputs.milestoneTaskGraph);

    const details = [
      renderLabeledValue(promptLabels.projectName, projectName),
      ...optionalLine(promptLabels.phaseName, phaseName),
      ...optionalLine(promptLabels.phaseGoal, phaseGoal),
      "",
      promptContent.taskContextLines.enrichSourceTask,
      stringifyValue(sourceTask ?? {}),
    ];

    const sourceAcceptanceCriteria = readStringArray(
      sourceTask?.acceptanceCriteria,
    );
    const sourceTestingCriteria = readStringArray(
      readRecord(sourceTask?.inputs)?.testingCriteria,
    );

    const requirements = renderRequirements({
      agentId: input.agentId,
      taskPrompt: readTaskPrompt(payloadInputs),
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

    if (isRetry(retry)) {
      return joinSections([
        renderRawSection(promptConfig.sections.task, [
          retryTaskContexts.enrichment[0],
          renderLabeledValue(promptLabels.projectName, projectName),
          ...(phaseName
            ? [renderLabeledValue(promptLabels.phaseName, phaseName)]
            : []),
          ...retryTaskContexts.enrichment.slice(1),
        ]),
        renderSourceTask(sourceTask),
        renderTaskPlan(taskPlan),
        renderDependencyTaskContext(dependencyTaskContext),
        renderMilestoneTaskGraphSection(milestoneTaskGraph),
        buildRetryBlock(retry),
        requirements,
        renderList(
          promptConfig.sections.output,
          getOutputReminder("enrichment"),
        ),
      ]);
    }

    return joinSections([
      renderGlobalLayers("session", "execution"),
      renderList(
        promptConfig.sections.role,
        getAgentRules("project_manager").role,
      ),
      renderList(promptConfig.sections.intent, getIntentRules("enrich_task")),
      renderRawSection(promptConfig.sections.task, details),
      renderTaskPlan(taskPlan),
      renderDependencyTaskContext(dependencyTaskContext),
      renderMilestoneTaskGraphSection(milestoneTaskGraph),
      requirements,
      buildRetryBlock(retry),
      renderList(promptConfig.sections.output, getOutputReminder("enrichment")),
    ]);
  }

  public buildProjectUpdatePlanningPromptFromPayload(
    input: OpenClawPromptBuildInput,
    constraints?: string[],
    _retry?: PromptRetryInput,
  ): string {
    const payloadInputs = input.payload.inputs;
    const retry = buildRetryInput(input.payload);

    const projectId =
      readString(payloadInputs.projectId) ??
      readString(input.payload.projectId);
    const projectName =
      readString(payloadInputs.projectName) ?? promptDefaults.unnamedProject;
    const requestType =
      readString(payloadInputs.requestType) ??
      readString(payloadInputs.updateRequestType);
    const canonicalProjectRoot =
      readString(payloadInputs.canonicalProjectRoot) ??
      readString(payloadInputs.projectPath);
    const rawUpdateRequest =
      readRawUpdateRequest(payloadInputs) ??
      readRawProjectRequest(payloadInputs) ??
      readString(payloadInputs.userRequest) ??
      readString(payloadInputs.request) ??
      promptDefaults.missingUpdateRequest;
    const latestAcceptedMilestoneSummary =
      readString(payloadInputs.latestAcceptedMilestoneSummary) ??
      readString(payloadInputs.latestMilestoneSummary) ??
      readString(payloadInputs.projectSummary);
    const latestReviewOutcome =
      readPromptValue(payloadInputs.latestReviewOutcome) ??
      readPromptValue(payloadInputs.reviewOutcome);

    const details = [
      ...optionalLine(promptLabels.projectId, projectId),
      renderLabeledValue(promptLabels.projectName, projectName),
      ...optionalLine(promptLabels.requestType, requestType),
      ...optionalLine(promptLabels.canonicalProjectRoot, canonicalProjectRoot),
      ...optionalLine(
        promptLabels.appType,
        readPromptValue(payloadInputs.appType),
      ),
      ...optionalLine(promptLabels.stack, readPromptValue(payloadInputs.stack)),
      ...optionalLine(
        promptLabels.deployment,
        readPromptValue(payloadInputs.deployment),
      ),
      ...(latestAcceptedMilestoneSummary
        ? [
            "",
            promptLabels.latestAcceptedMilestoneSummary,
            latestAcceptedMilestoneSummary,
          ]
        : []),
      ...(latestReviewOutcome
        ? ["", promptLabels.latestReviewOutcome, latestReviewOutcome]
        : []),
      "",
      promptLabels.updateRequest,
      rawUpdateRequest,
    ];

    const requirements = renderRequirements({
      agentId: input.agentId,
      taskPrompt: "",
      ...(constraints ? { constraints } : {}),
      ...(input.payload.acceptanceCriteria
        ? { acceptanceCriteria: input.payload.acceptanceCriteria }
        : {}),
    });

    if (isRetry(retry)) {
      return joinSections([
        renderRawSection(promptConfig.sections.task, [
          retryTaskContexts.projectUpdate[0],
          ...(projectId
            ? [renderLabeledValue(promptLabels.projectId, projectId)]
            : []),
          renderLabeledValue(promptLabels.projectName, projectName),
          ...(requestType
            ? [renderLabeledValue(promptLabels.requestType, requestType)]
            : []),
          ...retryTaskContexts.projectUpdate.slice(1),
        ]),
        buildRetryBlock(retry),
        requirements,
        renderList(promptConfig.sections.output, getOutputReminder("planner")),
      ]);
    }

    return joinSections([
      renderGlobalLayers("session", "planning"),
      renderList(
        promptConfig.sections.role,
        getAgentRules("product_owner").role,
      ),
      renderList(
        promptConfig.sections.intent,
        getIntentRules(resolveProjectUpdatePlanningIntentKey()),
      ),
      renderRawSection(promptConfig.sections.task, details),
      buildRetryBlock(retry),
      requirements,
      renderList(
        promptConfig.sections.output,
        getOutputReminder("project_owner"),
      ),
    ]);
  }

  public buildProjectPhasePlanningPromptFromPayload(
    input: OpenClawPromptBuildInput,
    constraints?: string[],
    _retry?: PromptRetryInput,
  ): string {
    const payloadInputs = input.payload.inputs;
    const retry = buildRetryInput(input.payload);

    const projectName =
      readString(payloadInputs.projectName) ?? promptDefaults.unnamedProject;
    const rawProjectRequest =
      readRawProjectRequest(payloadInputs) ??
      readString(payloadInputs.userPrompt) ??
      promptDefaults.missingProjectRequest;

    const details = [
      renderLabeledValue(promptLabels.projectName, projectName),
      ...optionalLine(
        promptLabels.appType,
        readPromptValue(payloadInputs.appType),
      ),
      ...optionalLine(promptLabels.stack, readPromptValue(payloadInputs.stack)),
      ...optionalLine(
        promptLabels.deployment,
        readPromptValue(payloadInputs.deployment),
      ),
      "",
      promptLabels.projectRequest,
      rawProjectRequest,
    ];

    const requirements = renderRequirements({
      agentId: input.agentId,
      taskPrompt: "",
      ...(constraints ? { constraints } : {}),
      ...(input.payload.acceptanceCriteria
        ? { acceptanceCriteria: input.payload.acceptanceCriteria }
        : {}),
    });

    if (isRetry(retry)) {
      return joinSections([
        // renderGlobalLayers("session", "planning"),
        // renderList(
        //   promptConfig.sections.role,
        //   getAgentRules("product_owner").role,
        // ),
        // renderList(
        //   promptConfig.sections.intent,
        //   getIntentRules("plan_project_phases"),
        // ),
        renderRawSection(promptConfig.sections.task, [
          retryTaskContexts.projectPhases[0],
          renderLabeledValue(promptLabels.projectName, projectName),
          ...retryTaskContexts.projectPhases.slice(1),
        ]),
        buildRetryBlock(retry),
        requirements,
        renderList(
          promptConfig.sections.output,
          getOutputReminder("project_owner"),
        ),
      ]);
    }

    return joinSections([
      renderGlobalLayers("session", "planning"),
      renderList(
        promptConfig.sections.role,
        getAgentRules("product_owner").role,
      ),
      renderList(
        promptConfig.sections.intent,
        getIntentRules("plan_project_phases"),
      ),
      renderRawSection(promptConfig.sections.task, details),
      buildRetryBlock(retry),
      requirements,
      renderList(
        promptConfig.sections.output,
        getOutputReminder("project_owner"),
      ),
    ]);
  }

  public buildMilestoneTaskPlanningPromptFromPayload(
    input: OpenClawPromptBuildInput,
    constraints?: string[],
    _retry?: PromptRetryInput,
  ): string {
    const payloadInputs = input.payload.inputs;
    const retry = buildRetryInput(input.payload);
    const milestone = readRecord(payloadInputs.milestone);
    const phase = readRecord(payloadInputs.phase);

    const projectName =
      readString(payloadInputs.projectName) ?? promptDefaults.unnamedProject;
    const projectSummary =
      readString(payloadInputs.projectSummary) ??
      readString(milestone?.projectSummary);
    const milestoneId =
      readString(payloadInputs.milestoneId) ??
      readString(payloadInputs._id) ??
      readString(payloadInputs.id) ??
      readString(milestone?._id) ??
      readString(milestone?.milestoneId) ??
      readString(milestone?.id);
    const milestoneTitle =
      readString(payloadInputs.milestoneTitle) ??
      readString(payloadInputs.title) ??
      readString(milestone?.title) ??
      readString(milestone?.name);
    const milestoneGoal =
      readString(payloadInputs.milestoneGoal) ??
      readString(payloadInputs.goal) ??
      readString(milestone?.goal);
    const milestoneDescription =
      readString(payloadInputs.milestoneDescription) ??
      readString(payloadInputs.description) ??
      readString(milestone?.description);
    const milestoneOrder =
      readNumber(payloadInputs.order) ?? readNumber(milestone?.order);
    const milestoneStatus =
      readString(payloadInputs.status) ?? readString(milestone?.status);
    const dependsOnMilestoneId =
      readString(payloadInputs.dependsOnMilestoneId) ??
      readString(milestone?.dependsOnMilestoneId);
    const scope = readStringArray(payloadInputs.milestoneScope);
    const scopeFromRecordFields =
      scope.length > 0 ? scope : readStringArray(payloadInputs.scope);
    const milestoneScope =
      scopeFromRecordFields.length > 0
        ? scopeFromRecordFields
        : readStringArray(milestone?.scope);
    const milestoneAcceptanceInput = readStringArray(
      payloadInputs.milestoneAcceptanceCriteria,
    );
    const milestoneAcceptanceFromRecordFields =
      milestoneAcceptanceInput.length > 0
        ? milestoneAcceptanceInput
        : readStringArray(payloadInputs.acceptanceCriteria);
    const milestoneAcceptanceCriteria =
      milestoneAcceptanceFromRecordFields.length > 0
        ? milestoneAcceptanceFromRecordFields
        : readStringArray(milestone?.acceptanceCriteria);

    const phaseId =
      readString(payloadInputs.phaseId) ??
      readString(phase?.phaseId) ??
      readString(phase?.id);
    const phaseName =
      readString(payloadInputs.phaseName) ??
      readString(phase?.name) ??
      promptDefaults.unnamedPhase;
    const phaseGoal =
      readString(payloadInputs.phaseGoal) ?? readString(phase?.goal);
    const phaseDescription =
      readString(payloadInputs.phaseDescription) ??
      readString(phase?.description);
    const dependsOn = readStringArray(payloadInputs.phaseDependsOn);
    const phaseDependsOn =
      dependsOn.length > 0 ? dependsOn : readStringArray(phase?.dependsOn);
    const deliverables = readStringArray(payloadInputs.phaseDeliverables);
    const phaseDeliverables =
      deliverables.length > 0
        ? deliverables
        : readStringArray(phase?.deliverables);
    const exitCriteria = readStringArray(payloadInputs.phaseExitCriteria);
    const phaseExitCriteria =
      exitCriteria.length > 0
        ? exitCriteria
        : readStringArray(phase?.exitCriteria);

    const details = [
      renderLabeledValue(promptLabels.projectName, projectName),
      ...optionalLine(promptLabels.milestoneId, milestoneId),
      ...optionalLine(promptLabels.milestoneTitle, milestoneTitle),
      ...optionalLine(promptLabels.milestoneGoal, milestoneGoal),
      ...optionalLine(promptLabels.milestoneDescription, milestoneDescription),
      ...optionalLine(promptLabels.dependsOnMilestoneId, dependsOnMilestoneId),
      ...(typeof milestoneOrder === "number"
        ? [renderLabeledValue(promptLabels.milestoneOrder, milestoneOrder)]
        : []),
      ...optionalLine(promptLabels.milestoneStatus, milestoneStatus),
      ...(projectSummary
        ? ["", promptLabels.projectSummary, projectSummary]
        : []),
      ...(milestoneTitle ||
      milestoneGoal ||
      milestoneDescription ||
      dependsOnMilestoneId ||
      typeof milestoneOrder === "number" ||
      milestoneStatus ||
      projectSummary
        ? [""]
        : []),
      ...optionalLine(promptLabels.phaseId, phaseId),
      renderLabeledValue(promptLabels.phaseName, phaseName),
      ...optionalLine(promptLabels.phaseGoal, phaseGoal),
      ...optionalLine(promptLabels.phaseDescription, phaseDescription),
      ...(phaseDependsOn.length > 0
        ? ["", promptLabels.phaseDependencies, ...phaseDependsOn]
        : []),
    ];

    const requirements = renderRequirements({
      agentId: input.agentId,
      taskPrompt: "",
      ...(constraints ? { constraints } : {}),
      ...(input.payload.acceptanceCriteria
        ? { acceptanceCriteria: input.payload.acceptanceCriteria }
        : {}),
    });

    const phaseTaskIntentKey = resolvePhaseTaskPlanningIntentKey();

    if (isRetry(retry)) {
      return joinSections([
        // renderGlobalLayers("session", "planning"),
        // renderList(
        //   promptConfig.sections.role,
        //   getAgentRules("project_manager").role,
        // ),
        // renderList(
        //   promptConfig.sections.intent,
        //   getIntentRules(phaseTaskIntentKey),
        // ),
        renderRawSection(promptConfig.sections.task, [
          retryTaskContexts.phaseTasks[0],
          renderLabeledValue(promptLabels.projectName, projectName),
          ...optionalLine(promptLabels.milestoneTitle, milestoneTitle),
          renderLabeledValue(promptLabels.phaseName, phaseName),
          ...retryTaskContexts.phaseTasks.slice(1),
        ]),
        buildRetryBlock(retry),
        requirements,
        renderList(promptConfig.sections.output, getOutputReminder("planner")),
      ]);
    }

    return joinSections([
      renderGlobalLayers("session", "planning"),
      renderList(
        promptConfig.sections.role,
        getAgentRules("project_manager").role,
      ),
      renderList(
        promptConfig.sections.intent,
        getIntentRules(phaseTaskIntentKey),
      ),
      renderRawSection(promptConfig.sections.task, details),
      renderList(promptLabels.milestoneScope, milestoneScope),
      renderList(
        promptLabels.milestoneAcceptanceCriteria,
        milestoneAcceptanceCriteria,
      ),
      renderList(promptLabels.phaseDeliverables, phaseDeliverables),
      renderList(promptLabels.phaseExitCriteria, phaseExitCriteria),
      buildRetryBlock(retry),
      requirements,
      renderList(promptConfig.sections.output, getOutputReminder("planner")),
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
    const retry = buildRetryInput(input.payload);
    const milestone = readRecord(payloadInputs.milestone);
    const phase = readRecord(payloadInputs.phase);
    const reviewContract = readRecord(payloadInputs.reviewContract);
    const milestoneExecution = readRecord(payloadInputs.milestoneExecution);

    const projectName =
      readString(payloadInputs.projectName) ?? promptDefaults.unnamedProject;
    const projectBrief = readMilestoneReviewProjectBrief(payloadInputs);
    const milestoneId =
      readString(payloadInputs.milestoneId) ??
      readString(payloadInputs._id) ??
      readString(payloadInputs.id) ??
      readString(milestone?._id) ??
      readString(milestone?.milestoneId) ??
      readString(milestone?.id);
    const milestoneTitle =
      readString(payloadInputs.milestoneTitle) ??
      readString(payloadInputs.title) ??
      readString(milestone?.title) ??
      readString(milestone?.name) ??
      promptDefaults.unnamedMilestone;
    const milestoneGoal =
      readString(payloadInputs.milestoneGoal) ??
      readString(payloadInputs.goal) ??
      readString(milestone?.goal);
    const milestoneDescription =
      readString(payloadInputs.milestoneDescription) ??
      readString(payloadInputs.description) ??
      readString(milestone?.description);
    const milestoneOrder =
      readNumber(payloadInputs.milestoneOrder) ??
      readNumber(payloadInputs.order) ??
      readNumber(milestone?.order);
    const milestoneStatus =
      readString(payloadInputs.milestoneStatus) ??
      readString(payloadInputs.status) ??
      readString(milestone?.status);
    const dependsOnMilestoneId =
      readString(payloadInputs.dependsOnMilestoneId) ??
      readString(milestone?.dependsOnMilestoneId);

    const milestoneScopeInput = readStringArray(payloadInputs.milestoneScope);
    const milestoneScope =
      milestoneScopeInput.length > 0
        ? milestoneScopeInput
        : readStringArray(payloadInputs.scope).length > 0
          ? readStringArray(payloadInputs.scope)
          : readStringArray(milestone?.scope);

    const milestoneAcceptanceInput = readStringArray(
      payloadInputs.milestoneAcceptanceCriteria,
    );
    const milestoneAcceptanceCriteria =
      milestoneAcceptanceInput.length > 0
        ? milestoneAcceptanceInput
        : readStringArray(payloadInputs.acceptanceCriteria).length > 0
          ? readStringArray(payloadInputs.acceptanceCriteria)
          : readStringArray(milestone?.acceptanceCriteria);

    const phaseId =
      readString(payloadInputs.phaseId) ??
      readString(phase?.phaseId) ??
      readString(phase?.id);
    const phaseName =
      readString(payloadInputs.phaseName) ??
      readString(phase?.name) ??
      promptDefaults.milestoneReviewTitle;
    const phaseGoal =
      readString(payloadInputs.phaseGoal) ?? readString(phase?.goal);

    const reviewEvidence = readRecordArray(
      milestoneExecution?.tasks ?? payloadInputs.milestoneExecution,
    );
    const completedTaskCount =
      readNumber(payloadInputs.completedTaskCount) ??
      readNumber(milestoneExecution?.completedTaskCount) ??
      reviewEvidence.length;
    const allowedDecisions = readStringArray(
      reviewContract?.allowedDecisions ?? payloadInputs.reviewDecisionOptions,
    );
    const reviewDecisionOptions =
      allowedDecisions.length > 0
        ? allowedDecisions
        : [...milestoneReviewCopy.defaultAllowedDecisions];
    const patchRule =
      readString(reviewContract?.patchRule) ??
      readString(payloadInputs.patchRule);

    const details = [
      renderLabeledValue(promptLabels.projectName, projectName),
      ...optionalLine(promptLabels.milestoneId, milestoneId),
      renderLabeledValue(promptLabels.milestoneTitle, milestoneTitle),
      ...optionalLine(promptLabels.milestoneGoal, milestoneGoal),
      ...optionalLine(promptLabels.milestoneDescription, milestoneDescription),
      ...optionalLine(promptLabels.dependsOnMilestoneId, dependsOnMilestoneId),
      ...(typeof milestoneOrder === "number"
        ? [renderLabeledValue(promptLabels.milestoneOrder, milestoneOrder)]
        : []),
      ...optionalLine(promptLabels.milestoneStatus, milestoneStatus),
      ...optionalLine(promptLabels.phaseId, phaseId),
      renderLabeledValue(promptLabels.phaseName, phaseName),
      ...optionalLine(promptLabels.phaseGoal, phaseGoal),
      ...(typeof completedTaskCount === "number"
        ? [
            renderLabeledValue(
              promptLabels.completedMilestoneTaskCount,
              completedTaskCount,
            ),
          ]
        : []),
      ...(projectBrief
        ? ["", promptLabels.originalProjectBrief, projectBrief]
        : []),
    ];

    const reviewInstructions = [
      milestoneReviewCopy.instructions[0],
      milestoneReviewCopy.instructions[1],
      renderLabeledValue(
        milestoneReviewCopy.allowedDecisionPrefix,
        reviewDecisionOptions.join(" | "),
      ),
      ...(patchRule ? [patchRule] : []),
      ...milestoneReviewCopy.instructions.slice(2),
    ];

    const requirements = renderRequirements({
      agentId: input.agentId,
      taskPrompt: "",
      ...(constraints ? { constraints } : {}),
      ...(input.payload.acceptanceCriteria
        ? { acceptanceCriteria: input.payload.acceptanceCriteria }
        : {}),
    });

    if (isRetry(retry)) {
      return joinSections([
        renderRawSection(promptConfig.sections.task, [
          retryTaskContexts.milestoneReview[0],
          renderLabeledValue(promptLabels.projectName, projectName),
          renderLabeledValue(promptLabels.milestoneTitle, milestoneTitle),
          ...retryTaskContexts.milestoneReview.slice(1),
        ]),
        renderList(promptConfig.sections.intent, reviewInstructions),
        renderList(promptLabels.milestoneScope, milestoneScope),
        renderList(
          promptLabels.milestoneAcceptanceCriteria,
          milestoneAcceptanceCriteria,
        ),
        renderMilestoneExecutionEvidence(reviewEvidence),
        buildRetryBlock(retry),
        requirements,
        renderMilestoneReviewOutputGuidance(),
      ]);
    }

    return joinSections([
      renderGlobalLayers("session", "execution"),
      renderList(
        promptConfig.sections.role,
        getAgentRules("product_owner").role,
      ),
      renderList(promptConfig.sections.intent, reviewInstructions),
      renderRawSection(promptConfig.sections.task, details),
      renderList(promptLabels.milestoneScope, milestoneScope),
      renderList(
        promptLabels.milestoneAcceptanceCriteria,
        milestoneAcceptanceCriteria,
      ),
      renderMilestoneExecutionEvidence(reviewEvidence),
      buildRetryBlock(retry),
      requirements,
      renderMilestoneReviewOutputGuidance(),
    ]);
  }

  buildRetryBlock(input?: PromptRetryInput): string | undefined {
    return buildRetryBlock(input);
  }
}

export default PromptService;
