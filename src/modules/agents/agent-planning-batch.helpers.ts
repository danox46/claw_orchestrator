import type { MilestoneRecord } from "../milestones/milestone.service";
import type { TaskRecord } from "../tasks/task.service";
import type {
  AtomicMilestonePlanningBatch,
  AtomicMilestonePlanningBatchCreate,
  AtomicMilestonePlanningBatchResult,
  AtomicMilestonePlanningBatchUpdate,
  PlannedTaskDefinition,
  PlanningValidationIssue,
  PreparedConcreteTask,
} from "./agent.types";
import {
  asCreateTaskIntent,
  asRecord,
  areStringArraysEquivalent,
  areValuesEquivalent,
  buildConcreteTaskInputs,
  buildPlannedTaskReferenceKeys,
  buildTaskConstraints,
  canSyncConcreteTaskDefinition,
  createPlannedTaskValidationIssue,
  createServiceError,
  describePlannedTask,
  getEnrichmentTaskSequence,
  getExecutionTaskSequence,
  normalizeStringArray,
  readOptionalString,
  shouldResetConcreteTaskToQueued,
} from "./agent.helpers";

export type PlanningTargetAgentIds = {
  projectOwnerAgentId: string;
  projectManagerAgentId: string;
  implementerAgentId: string;
  qaAgentId: string;
};

export type PlannedTaskMetadata = {
  index: number;
  plannedTask: PlannedTaskDefinition;
  executionIdempotencyKey: string;
  enrichmentIdempotencyKey: string;
  executionReferenceKey: string;
  enrichmentReferenceKey: string;
  rawReferenceKeys: string[];
};

export type PlannedTaskReferenceOwner = Omit<
  PlannedTaskMetadata,
  "rawReferenceKeys"
>;

export type ValidatePlannedTaskListResult = {
  plannedTaskMetadata: PlannedTaskMetadata[];
  rawReferenceOwnerByKey: Map<string, PlannedTaskReferenceOwner>;
  issues: PlanningValidationIssue[];
};

export type NormalizedPlannedTask = {
  localId: string;
  intent: string;
  targetAgentId: string;
  dependsOn: string[];
  acceptanceCriteria: string[];
  testingCriteria: string[];
  prompt?: string;
};

export type NormalizedPlannedTaskPlan = NormalizedPlannedTask[];

export function resolvePlanningTargetAgentId(
  input: PlanningTargetAgentIds & {
    intent: string;
    explicitTargetAgentId?: string | undefined;
  },
): string {
  if (
    typeof input.explicitTargetAgentId === "string" &&
    input.explicitTargetAgentId.trim().length > 0
  ) {
    return input.explicitTargetAgentId.trim();
  }

  switch (input.intent) {
    case "plan_project_phases":
    case "review_milestone":
      return input.projectOwnerAgentId;
    case "plan_phase_tasks":
    case "plan_next_tasks":
    case "enrich_task":
      return input.projectManagerAgentId;
    case "run_tests":
    case "review_security":
      return input.qaAgentId;
    case "design_architecture":
    case "generate_scaffold":
    case "implement_feature":
    case "prepare_staging":
    default:
      return input.implementerAgentId;
  }
}

export function prepareConcreteMilestoneTasks(input: {
  plannerTask: TaskRecord;
  plannedTasks: PlannedTaskDefinition[];
  plannedTaskMetadata: PlannedTaskMetadata[];
  rawReferenceOwnerByKey: Map<string, PlannedTaskReferenceOwner>;
  existingByIdempotencyKey: Map<string, TaskRecord>;
  phaseId: string;
  phaseName: string;
  phaseGoal?: string | undefined;
} & PlanningTargetAgentIds): PreparedConcreteTask[] {
  const preparedTasks: PreparedConcreteTask[] = [];
  const targetAgentIds = toTargetAgentIds(input);
  const taskPlan = buildNormalizedTaskPlan(input.plannedTasks, targetAgentIds);

  for (const metadata of input.plannedTaskMetadata) {
    const targetAgentId = resolvePlanningTargetAgentId({
      ...targetAgentIds,
      intent: metadata.plannedTask.intent,
      explicitTargetAgentId: metadata.plannedTask.targetAgentId,
    });
    const baseExecutionInputs = buildConcreteTaskInputs({
      parentTask: input.plannerTask,
      plannedTask: metadata.plannedTask,
      phaseId: input.phaseId,
      phaseName: input.phaseName,
      ...(input.phaseGoal ? { phaseGoal: input.phaseGoal } : {}),
    });
    const enrichmentTask: PreparedConcreteTask = {
      index: metadata.index,
      variant: "enrichment",
      plannedTask: metadata.plannedTask,
      idempotencyKey: metadata.enrichmentIdempotencyKey,
      sequence: getEnrichmentTaskSequence(
        input.plannerTask.sequence,
        metadata.index,
      ),
      targetAgentId: input.projectManagerAgentId,
      createIntent: asCreateTaskIntent("enrich_task"),
      inputs: buildEnrichmentTaskInputs({
        executionInputs: baseExecutionInputs,
        plannedTask: metadata.plannedTask,
        taskPlan,
        executionIdempotencyKey: metadata.executionIdempotencyKey,
        enrichmentIdempotencyKey: metadata.enrichmentIdempotencyKey,
        phaseId: input.phaseId,
        phaseName: input.phaseName,
        targetAgentIds,
        ...(input.phaseGoal ? { phaseGoal: input.phaseGoal } : {}),
      }),
      constraints: buildTaskConstraints(input.plannerTask.constraints),
      referenceKeys: [
        metadata.enrichmentReferenceKey,
        metadata.enrichmentIdempotencyKey,
      ],
      dependencyRefs: metadata.plannedTask.dependsOn.map((dependencyRef) => {
        const owner = input.rawReferenceOwnerByKey.get(dependencyRef)!;
        return owner.enrichmentReferenceKey;
      }),
      ...(input.existingByIdempotencyKey.get(
        metadata.enrichmentIdempotencyKey,
      )
        ? {
            existingTask: input.existingByIdempotencyKey.get(
              metadata.enrichmentIdempotencyKey,
            )!,
          }
        : {}),
    };

    const executionTask: PreparedConcreteTask = {
      index: metadata.index,
      variant: "execution",
      plannedTask: metadata.plannedTask,
      idempotencyKey: metadata.executionIdempotencyKey,
      sequence: getExecutionTaskSequence(
        input.plannerTask.sequence,
        metadata.index,
      ),
      targetAgentId,
      createIntent: asCreateTaskIntent(metadata.plannedTask.intent),
      inputs: buildExecutionTaskInputs({
        executionInputs: baseExecutionInputs,
        plannedTask: metadata.plannedTask,
        taskPlan,
        executionIdempotencyKey: metadata.executionIdempotencyKey,
        enrichmentIdempotencyKey: metadata.enrichmentIdempotencyKey,
        phaseId: input.phaseId,
        phaseName: input.phaseName,
        ...(input.phaseGoal ? { phaseGoal: input.phaseGoal } : {}),
      }),
      constraints: buildTaskConstraints(
        input.plannerTask.constraints,
        metadata.plannedTask.constraints,
      ),
      referenceKeys: [
        metadata.executionReferenceKey,
        metadata.executionIdempotencyKey,
      ],
      dependencyRefs: [
        metadata.enrichmentReferenceKey,
        ...metadata.plannedTask.dependsOn.map((dependencyRef) => {
          const owner = input.rawReferenceOwnerByKey.get(dependencyRef)!;
          return owner.executionReferenceKey;
        }),
      ],
      ...(input.existingByIdempotencyKey.get(metadata.executionIdempotencyKey)
        ? {
            existingTask: input.existingByIdempotencyKey.get(
              metadata.executionIdempotencyKey,
            )!,
          }
        : {}),
    };

    preparedTasks.push(enrichmentTask, executionTask);
  }

  return preparedTasks;
}

export function validatePreparedConcreteMilestoneTasks(input: {
  plannerTask: TaskRecord;
  preparedTasks: PreparedConcreteTask[];
}): PlanningValidationIssue[] {
  const issues: PlanningValidationIssue[] = [];

  for (const preparedTask of input.preparedTasks) {
    const existingTaskIssue = verifyExistingConcreteTaskRecord({
      plannerTask: input.plannerTask,
      preparedTask,
    });

    if (existingTaskIssue) {
      issues.push(existingTaskIssue);
    }
  }

  issues.push(
    ...validateExpandedPreparedTaskGraph({
      plannerTask: input.plannerTask,
      preparedTasks: input.preparedTasks,
    }),
  );

  return issues;
}

export function validatePlannedTaskList(input: {
  plannerTask: TaskRecord;
  plannedTasks: PlannedTaskDefinition[];
}): ValidatePlannedTaskListResult {
  const metadata = buildPlannedTaskMetadata(input);
  const issues = [...metadata.issues];

  issues.push(
    ...validatePlannedTaskDependencyGraph({
      plannerTask: input.plannerTask,
      plannedTaskMetadata: metadata.plannedTaskMetadata,
      rawReferenceOwnerByKey: metadata.rawReferenceOwnerByKey,
    }),
  );

  return {
    plannedTaskMetadata: metadata.plannedTaskMetadata,
    rawReferenceOwnerByKey: metadata.rawReferenceOwnerByKey,
    issues,
  };
}

export function prepareAtomicMilestonePlanningBatch(input: {
  plannerTask: TaskRecord;
  milestone: MilestoneRecord;
  preparedTasks: PreparedConcreteTask[];
  existingByIdempotencyKey: Map<string, TaskRecord>;
  phaseId: string;
  phaseName: string;
  phaseGoal?: string | undefined;
  projectOwnerAgentId: string;
}): AtomicMilestonePlanningBatch {
  const creates: AtomicMilestonePlanningBatchCreate[] = [];
  const updates: AtomicMilestonePlanningBatchUpdate[] = [];

  for (const preparedTask of input.preparedTasks) {
    const dependencyRefs = [...new Set(preparedTask.dependencyRefs)];
    const referenceKeys = [...new Set(preparedTask.referenceKeys)];

    if (!preparedTask.existingTask) {
      creates.push({
        kind: "create",
        referenceKeys,
        dependencyTaskIds: [],
        dependencyRefs,
        task: {
          jobId: input.plannerTask.jobId,
          projectId: input.plannerTask.projectId,
          milestoneId: input.plannerTask.milestoneId,
          parentTaskId: input.plannerTask._id,
          dependencies: [],
          issuer: {
            kind: "system",
            id: "app-factory-orchestrator",
            role: "orchestrator",
          },
          target: {
            agentId: preparedTask.targetAgentId,
          },
          intent: preparedTask.createIntent,
          inputs: preparedTask.inputs,
          constraints: preparedTask.constraints,
          requiredArtifacts:
            preparedTask.variant === "execution"
              ? preparedTask.plannedTask.requiredArtifacts
              : [],
          acceptanceCriteria:
            preparedTask.variant === "execution"
              ? preparedTask.plannedTask.acceptanceCriteria
              : [
                  `Produce enriched task context for ${describePlannedTask(preparedTask.plannedTask, preparedTask.index)} without changing the approved scope.`,
                ],
          idempotencyKey: preparedTask.idempotencyKey,
          sequence: preparedTask.sequence,
        },
      });
      continue;
    }

    const shouldUpdateSequence =
      preparedTask.existingTask.sequence !== preparedTask.sequence;
    const shouldResetQueued = shouldResetConcreteTaskToQueued(
      preparedTask.existingTask,
    );
    const canSyncExistingTask = canSyncConcreteTaskDefinition(
      preparedTask.existingTask,
    );
    const concreteRequiredArtifacts =
      preparedTask.variant === "execution"
        ? [...preparedTask.plannedTask.requiredArtifacts]
        : [];
    const concreteAcceptanceCriteria =
      preparedTask.variant === "execution"
        ? [...preparedTask.plannedTask.acceptanceCriteria]
        : [
            `Produce enriched task context for ${describePlannedTask(preparedTask.plannedTask, preparedTask.index)} without changing the approved scope.`,
          ];
    const shouldUpdateTargetAgent =
      preparedTask.existingTask.target.agentId !== preparedTask.targetAgentId;
    const plannedTaskInputs = asRecord(preparedTask.plannedTask.inputs);
    const existingTaskInputs = asRecord(preparedTask.existingTask.inputs);
    const nextExecutionPrompt = readOptionalString(plannedTaskInputs, [
      "prompt",
    ]);
    const existingExecutionPrompt = readOptionalString(existingTaskInputs, [
      "prompt",
    ]);
    const nextExecutionTestingCriteria = normalizeStringArray(
      plannedTaskInputs.testingCriteria,
    );
    const existingExecutionTestingCriteria = normalizeStringArray(
      existingTaskInputs.testingCriteria,
    );
    const existingExecutionAcceptanceCriteria = normalizeStringArray(
      existingTaskInputs.acceptanceCriteria,
    );
    const shouldUpdateInputs =
      preparedTask.variant === "enrichment"
        ? !areValuesEquivalent(preparedTask.existingTask.inputs, preparedTask.inputs)
        : false;
    const shouldUpdateConcretePrompt =
      preparedTask.variant === "execution" &&
      nextExecutionPrompt !== existingExecutionPrompt;
    const shouldUpdateConcreteTestingCriteria =
      preparedTask.variant === "execution" &&
      !areStringArraysEquivalent(
        existingExecutionTestingCriteria,
        nextExecutionTestingCriteria,
      );
    const shouldUpdateConcreteInputAcceptanceCriteria =
      preparedTask.variant === "execution" &&
      !areStringArraysEquivalent(
        existingExecutionAcceptanceCriteria,
        concreteAcceptanceCriteria,
      );
    const shouldUpdateConstraints = !areValuesEquivalent(
      preparedTask.existingTask.constraints as Record<string, unknown>,
      preparedTask.constraints as Record<string, unknown>,
    );
    const shouldUpdateRequiredArtifacts = !areStringArraysEquivalent(
      preparedTask.existingTask.requiredArtifacts,
      concreteRequiredArtifacts,
    );
    const shouldUpdateAcceptanceCriteria = !areStringArraysEquivalent(
      preparedTask.existingTask.acceptanceCriteria,
      concreteAcceptanceCriteria,
    );

    if (!canSyncExistingTask) {
      continue;
    }

    if (
      !shouldUpdateSequence &&
      !shouldResetQueued &&
      !shouldUpdateTargetAgent &&
      !shouldUpdateInputs &&
      !shouldUpdateConcretePrompt &&
      !shouldUpdateConcreteTestingCriteria &&
      !shouldUpdateConcreteInputAcceptanceCriteria &&
      !shouldUpdateConstraints &&
      !shouldUpdateRequiredArtifacts &&
      !shouldUpdateAcceptanceCriteria
    ) {
      continue;
    }

    updates.push({
      kind: "update",
      taskId: preparedTask.existingTask._id,
      referenceKeys,
      dependencyTaskIds: [],
      dependencyRefs,
      patch: {
        ...(shouldUpdateTargetAgent
          ? {
              target: {
                agentId: preparedTask.targetAgentId,
              },
            }
          : {}),
        ...(shouldUpdateSequence ? { sequence: preparedTask.sequence } : {}),
        ...(shouldUpdateInputs ? { inputs: preparedTask.inputs } : {}),
        ...(shouldUpdateConcretePrompt &&
        typeof nextExecutionPrompt === "string"
          ? { prompt: nextExecutionPrompt }
          : {}),
        ...(shouldUpdateConcreteTestingCriteria
          ? { testingCriteria: nextExecutionTestingCriteria }
          : {}),
        ...(shouldUpdateConcreteInputAcceptanceCriteria
          ? { inputAcceptanceCriteria: concreteAcceptanceCriteria }
          : {}),
        ...(shouldUpdateConstraints
          ? { constraints: preparedTask.constraints }
          : {}),
        ...(shouldUpdateRequiredArtifacts
          ? { requiredArtifacts: concreteRequiredArtifacts }
          : {}),
        ...(shouldUpdateAcceptanceCriteria
          ? { acceptanceCriteria: concreteAcceptanceCriteria }
          : {}),
        ...(shouldResetQueued ? { status: "queued" } : {}),
      },
    });
  }

  const executionTasks = input.preparedTasks.filter(
    (preparedTask) => preparedTask.variant === "execution",
  );
  const reviewIdempotencyKey = `${input.plannerTask.jobId}:milestone:${input.plannerTask.milestoneId}:review_milestone`;
  const reviewReferenceKeys = [
    `review:${reviewIdempotencyKey}`,
    reviewIdempotencyKey,
  ];
  const reviewDependencyRefs = executionTasks.map(
    (preparedTask) => preparedTask.referenceKeys[0]!,
  );
  const reviewSequence = getMilestoneReviewTaskSequenceForPreparedTasks(
    input.plannerTask.sequence,
    executionTasks,
  );
  const reviewInputs = buildMilestoneReviewTaskInputsForPreparedTasks({
    plannerTask: input.plannerTask,
    milestone: input.milestone,
    phaseId: input.phaseId,
    phaseName: input.phaseName,
    ...(input.phaseGoal ? { phaseGoal: input.phaseGoal } : {}),
    concreteTasks: executionTasks,
  });
  const existingReviewTask = input.existingByIdempotencyKey.get(
    reviewIdempotencyKey,
  );

  if (!existingReviewTask) {
    creates.push({
      kind: "create",
      referenceKeys: reviewReferenceKeys,
      dependencyTaskIds: [input.plannerTask._id],
      dependencyRefs: reviewDependencyRefs,
      task: {
        jobId: input.plannerTask.jobId,
        projectId: input.plannerTask.projectId,
        milestoneId: input.plannerTask.milestoneId,
        parentTaskId: input.plannerTask._id,
        dependencies: [],
        issuer: {
          kind: "system",
          id: "app-factory-orchestrator",
          role: "orchestrator",
        },
        target: {
          agentId: input.projectOwnerAgentId,
        },
        intent: asCreateTaskIntent("review_milestone"),
        inputs: reviewInputs,
        constraints: buildTaskConstraints(input.plannerTask.constraints),
        requiredArtifacts: [],
        acceptanceCriteria: [...input.milestone.acceptanceCriteria],
        idempotencyKey: reviewIdempotencyKey,
        sequence: reviewSequence,
      },
    });
  } else {
    const shouldRequeueFailedReviewTask =
      existingReviewTask.status === "failed" ||
      existingReviewTask.status === "canceled";
    const canSyncReviewTask =
      existingReviewTask.status === "queued" ||
      existingReviewTask.status === "qa" ||
      shouldRequeueFailedReviewTask;

    if (canSyncReviewTask) {
      updates.push({
        kind: "update",
        taskId: existingReviewTask._id,
        referenceKeys: reviewReferenceKeys,
        dependencyTaskIds: [input.plannerTask._id],
        dependencyRefs: reviewDependencyRefs,
        patch: {
          target: {
            agentId: input.projectOwnerAgentId,
          },
          sequence: reviewSequence,
          inputs: reviewInputs,
          acceptanceCriteria: [...input.milestone.acceptanceCriteria],
          ...(shouldRequeueFailedReviewTask ? { status: "queued" } : {}),
        },
      });
    } else {
      throw createServiceError({
        message: `Existing milestone review task ${existingReviewTask._id} is in non-syncable status "${existingReviewTask.status}".`,
        code: "REVIEW_TASK_STATE_INVALID_FOR_SYNC",
        statusCode: 409,
        retryable: false,
        details: {
          plannerTaskId: input.plannerTask._id,
          milestoneId: input.plannerTask.milestoneId,
          reviewTaskId: existingReviewTask._id,
          reviewTaskStatus: existingReviewTask.status,
          reviewTaskIntent: existingReviewTask.intent,
        },
      });
    }
  }

  return {
    plannerTaskId: input.plannerTask._id,
    jobId: input.plannerTask.jobId,
    projectId: input.plannerTask.projectId,
    milestoneId: input.plannerTask.milestoneId,
    creates,
    updates,
  };
}

export function assertMilestonePlanningBatchCommitResult(input: {
  plannerTask: TaskRecord;
  batch: AtomicMilestonePlanningBatch;
  batchResult: AtomicMilestonePlanningBatchResult;
}): void {
  const expectedCreatedCount = input.batch.creates.length;
  const expectedUpdatedCount = input.batch.updates.length;
  const actualCreatedCount = input.batchResult.createdTaskIds.length;
  const actualUpdatedCount = input.batchResult.updatedTaskIds.length;
  const createdTasks = Array.isArray(input.batchResult.createdTasks)
    ? input.batchResult.createdTasks
    : [];
  const updatedTasks = Array.isArray(input.batchResult.updatedTasks)
    ? input.batchResult.updatedTasks
    : [];
  const reviewMutationCount =
    (input.batchResult.reviewTaskCreated ? 1 : 0) +
    (input.batchResult.reviewTaskUpdated ? 1 : 0);

  if (actualCreatedCount !== expectedCreatedCount) {
    throw createServiceError({
      message: `Milestone planning batch expected ${expectedCreatedCount} created task(s) but commit reported ${actualCreatedCount}.`,
      code: "TASK_BATCH_COMMIT_CREATED_COUNT_MISMATCH",
      statusCode: 500,
      retryable: true,
      details: {
        plannerTaskId: input.plannerTask._id,
        expectedCreatedCount,
        actualCreatedCount,
        batchResult: input.batchResult,
      },
    });
  }

  if (createdTasks.length !== actualCreatedCount) {
    throw createServiceError({
      message: `Milestone planning batch reported ${actualCreatedCount} created task id(s) but returned ${createdTasks.length} created task detail entr${createdTasks.length === 1 ? "y" : "ies"}.`,
      code: "TASK_BATCH_COMMIT_CREATED_DETAIL_COUNT_MISMATCH",
      statusCode: 500,
      retryable: true,
      details: {
        plannerTaskId: input.plannerTask._id,
        actualCreatedCount,
        actualCreatedDetailCount: createdTasks.length,
        batchResult: input.batchResult,
      },
    });
  }

  if (actualUpdatedCount !== expectedUpdatedCount) {
    throw createServiceError({
      message: `Milestone planning batch expected ${expectedUpdatedCount} updated task(s) but commit reported ${actualUpdatedCount}.`,
      code: "TASK_BATCH_COMMIT_UPDATED_COUNT_MISMATCH",
      statusCode: 500,
      retryable: true,
      details: {
        plannerTaskId: input.plannerTask._id,
        expectedUpdatedCount,
        actualUpdatedCount,
        batchResult: input.batchResult,
      },
    });
  }

  if (updatedTasks.length !== actualUpdatedCount) {
    throw createServiceError({
      message: `Milestone planning batch reported ${actualUpdatedCount} updated task id(s) but returned ${updatedTasks.length} updated task detail entr${updatedTasks.length === 1 ? "y" : "ies"}.`,
      code: "TASK_BATCH_COMMIT_UPDATED_DETAIL_COUNT_MISMATCH",
      statusCode: 500,
      retryable: true,
      details: {
        plannerTaskId: input.plannerTask._id,
        actualUpdatedCount,
        actualUpdatedDetailCount: updatedTasks.length,
        batchResult: input.batchResult,
      },
    });
  }

  const createdTaskIdSet = new Set(input.batchResult.createdTaskIds);
  for (const createdTask of createdTasks) {
    if (
      typeof createdTask.taskId !== "string" ||
      createdTask.taskId.trim().length === 0 ||
      !createdTaskIdSet.has(createdTask.taskId)
    ) {
      throw createServiceError({
        message:
          "Milestone planning batch returned an invalid created task detail entry.",
        code: "TASK_BATCH_COMMIT_CREATED_DETAIL_INVALID",
        statusCode: 500,
        retryable: true,
        details: {
          plannerTaskId: input.plannerTask._id,
          createdTask,
          batchResult: input.batchResult,
        },
      });
    }
  }

  const updatedTaskIdSet = new Set(input.batchResult.updatedTaskIds);
  for (const updatedTask of updatedTasks) {
    if (
      typeof updatedTask.taskId !== "string" ||
      updatedTask.taskId.trim().length === 0 ||
      !updatedTaskIdSet.has(updatedTask.taskId)
    ) {
      throw createServiceError({
        message:
          "Milestone planning batch returned an invalid updated task detail entry.",
        code: "TASK_BATCH_COMMIT_UPDATED_DETAIL_INVALID",
        statusCode: 500,
        retryable: true,
        details: {
          plannerTaskId: input.plannerTask._id,
          updatedTask,
          batchResult: input.batchResult,
        },
      });
    }
  }

  if (!input.batchResult.reviewTaskId || reviewMutationCount !== 1) {
    throw createServiceError({
      message:
        "Milestone planning batch must commit exactly one review task mutation and return its task id.",
      code: "TASK_BATCH_COMMIT_REVIEW_TASK_INVALID",
      statusCode: 500,
      retryable: true,
      details: {
        plannerTaskId: input.plannerTask._id,
        reviewTaskId: input.batchResult.reviewTaskId,
        reviewTaskCreated: input.batchResult.reviewTaskCreated,
        reviewTaskUpdated: input.batchResult.reviewTaskUpdated,
        batchResult: input.batchResult,
      },
    });
  }

  const reviewTaskEntry = [...createdTasks, ...updatedTasks].find(
    (task) => task.taskId === input.batchResult.reviewTaskId,
  );

  if (!reviewTaskEntry) {
    throw createServiceError({
      message:
        "Milestone planning batch returned a review task id that does not match any committed mutation entry.",
      code: "TASK_BATCH_COMMIT_REVIEW_TASK_MISSING_DETAIL",
      statusCode: 500,
      retryable: true,
      details: {
        plannerTaskId: input.plannerTask._id,
        reviewTaskId: input.batchResult.reviewTaskId,
        batchResult: input.batchResult,
      },
    });
  }

  if (reviewTaskEntry.taskIntent !== "review_milestone") {
    throw createServiceError({
      message:
        "Milestone planning batch review task detail does not point to a review_milestone task.",
      code: "TASK_BATCH_COMMIT_REVIEW_TASK_INTENT_INVALID",
      statusCode: 500,
      retryable: true,
      details: {
        plannerTaskId: input.plannerTask._id,
        reviewTaskId: input.batchResult.reviewTaskId,
        reviewTaskEntry,
        batchResult: input.batchResult,
      },
    });
  }
}

function buildMilestoneReviewTaskInputsForPreparedTasks(input: {
  plannerTask: TaskRecord;
  milestone: MilestoneRecord;
  phaseId: string;
  phaseName: string;
  phaseGoal?: string | undefined;
  concreteTasks: PreparedConcreteTask[];
}): Record<string, unknown> {
  const plannerInputs = asRecord(input.plannerTask.inputs);

  return {
    ...(typeof plannerInputs.projectName === "string"
      ? { projectName: plannerInputs.projectName }
      : {}),
    ...(typeof plannerInputs.projectRequest === "string"
      ? { projectRequest: plannerInputs.projectRequest }
      : typeof plannerInputs.prompt === "string"
        ? { projectRequest: plannerInputs.prompt }
        : {}),
    ...(typeof plannerInputs.request === "string"
      ? { request: plannerInputs.request }
      : {}),
    ...(typeof plannerInputs.appType === "string"
      ? { appType: plannerInputs.appType }
      : {}),
    ...(typeof plannerInputs.stack === "string"
      ? { stack: plannerInputs.stack }
      : {}),
    ...(typeof plannerInputs.deployment === "string"
      ? { deployment: plannerInputs.deployment }
      : {}),
    milestoneId: input.milestone._id,
    phaseId: input.phaseId,
    phaseName: input.phaseName,
    ...(input.phaseGoal ? { phaseGoal: input.phaseGoal } : {}),
    milestone: {
      id: input.milestone._id,
      title: input.milestone.title,
      ...(input.milestone.description
        ? { description: input.milestone.description }
        : {}),
      order: input.milestone.order,
      ...(input.milestone.goal ? { goal: input.milestone.goal } : {}),
      scope: [...input.milestone.scope],
      acceptanceCriteria: [...input.milestone.acceptanceCriteria],
      ...(input.milestone.dependsOnMilestoneId
        ? { dependsOnMilestoneId: input.milestone.dependsOnMilestoneId }
        : {}),
    },
    reviewContract: {
      allowedDecisions: ["pass", "patch"],
      patchRule:
        "If the milestone is incomplete or broken, define the smallest possible patch milestone needed to satisfy the stated acceptance criteria without adding new scope.",
    },
    milestoneTaskPlan: input.concreteTasks.map((task) => ({
      taskId: task.existingTask?._id ?? task.idempotencyKey,
      intent: String(task.createIntent),
      targetAgentId: task.targetAgentId,
      acceptanceCriteria: [...task.plannedTask.acceptanceCriteria],
    })),
  };
}

function getMilestoneReviewTaskSequenceForPreparedTasks(
  plannerSequence: number,
  concreteTasks: Array<{ sequence: number }>,
): number {
  let maxSequence = plannerSequence;

  for (const concreteTask of concreteTasks) {
    if (concreteTask.sequence > maxSequence) {
      maxSequence = concreteTask.sequence;
    }
  }

  return maxSequence + 10;
}

function buildNormalizedTaskPlan(
  plannedTasks: PlannedTaskDefinition[],
  targetAgentIds: PlanningTargetAgentIds,
): NormalizedPlannedTaskPlan {
  return plannedTasks.map((plannedTask, index) => {
    const plannedInputs = asRecord(plannedTask.inputs);
    const prompt = readOptionalString(plannedInputs, ["prompt"]);
    const testingCriteria = normalizeStringArray(plannedInputs.testingCriteria);

    return {
      localId: plannedTask.localId ?? `task-${index + 1}`,
      intent: plannedTask.intent,
      targetAgentId: resolvePlanningTargetAgentId({
        ...targetAgentIds,
        intent: plannedTask.intent,
        explicitTargetAgentId: plannedTask.targetAgentId,
      }),
      dependsOn: [...plannedTask.dependsOn],
      acceptanceCriteria: [...plannedTask.acceptanceCriteria],
      testingCriteria,
      ...(prompt ? { prompt } : {}),
    };
  });
}

function buildEnrichmentTaskInputs(input: {
  executionInputs: Record<string, unknown>;
  plannedTask: PlannedTaskDefinition;
  taskPlan: NormalizedPlannedTaskPlan;
  executionIdempotencyKey: string;
  enrichmentIdempotencyKey: string;
  phaseId: string;
  phaseName: string;
  phaseGoal?: string | undefined;
  targetAgentIds: PlanningTargetAgentIds;
}): Record<string, unknown> {
  return {
    ...input.executionInputs,
    systemTaskType: "enrichment",
    sourceTask: {
      ...(input.plannedTask.localId ? { localId: input.plannedTask.localId } : {}),
      intent: input.plannedTask.intent,
      targetAgentId: resolvePlanningTargetAgentId({
        ...input.targetAgentIds,
        intent: input.plannedTask.intent,
        explicitTargetAgentId: input.plannedTask.targetAgentId,
      }),
      inputs: input.plannedTask.inputs,
      constraints: input.plannedTask.constraints ?? {},
      requiredArtifacts: [...input.plannedTask.requiredArtifacts],
      acceptanceCriteria: [...input.plannedTask.acceptanceCriteria],
      dependsOn: [...input.plannedTask.dependsOn],
    },
    taskPlan: input.taskPlan,
    executionTaskIdempotencyKey: input.executionIdempotencyKey,
    enrichmentTaskIdempotencyKey: input.enrichmentIdempotencyKey,
    phaseId: input.phaseId,
    phaseName: input.phaseName,
    ...(input.phaseGoal ? { phaseGoal: input.phaseGoal } : {}),
  };
}

function buildExecutionTaskInputs(input: {
  executionInputs: Record<string, unknown>;
  plannedTask: PlannedTaskDefinition;
  taskPlan: NormalizedPlannedTaskPlan;
  executionIdempotencyKey: string;
  enrichmentIdempotencyKey: string;
  phaseId: string;
  phaseName: string;
  phaseGoal?: string | undefined;
}): Record<string, unknown> {
  return {
    ...input.executionInputs,
    systemTaskType: "execution",
    ...(input.plannedTask.localId
      ? { plannedTaskLocalId: input.plannedTask.localId }
      : {}),
    plannedTaskIntent: input.plannedTask.intent,
    taskPlan: input.taskPlan,
    executionTaskIdempotencyKey: input.executionIdempotencyKey,
    enrichmentTaskIdempotencyKey: input.enrichmentIdempotencyKey,
    phaseId: input.phaseId,
    phaseName: input.phaseName,
    ...(input.phaseGoal ? { phaseGoal: input.phaseGoal } : {}),
  };
}

function buildPlannedTaskMetadata(input: {
  plannerTask: TaskRecord;
  plannedTasks: PlannedTaskDefinition[];
}): ValidatePlannedTaskListResult {
  const issues: PlanningValidationIssue[] = [];
  const seenIdempotencyKeys = new Set<string>();
  const rawReferenceOwnerByKey = new Map<string, PlannedTaskReferenceOwner>();

  const plannedTaskMetadata = input.plannedTasks.map((plannedTask, index) => {
    const executionIdempotencyKey =
      plannedTask.idempotencyKey ??
      `${input.plannerTask.jobId}:${input.plannerTask.milestoneId}:planned-task:${index + 1}:${plannedTask.intent}`;
    const enrichmentIdempotencyKey = `${executionIdempotencyKey}:enrich`;
    const executionReferenceKey = `execution:${executionIdempotencyKey}`;
    const enrichmentReferenceKey = `enrichment:${executionIdempotencyKey}`;
    const rawReferenceKeys = buildPlannedTaskReferenceKeys(
      plannedTask,
      executionIdempotencyKey,
    );

    if (seenIdempotencyKeys.has(executionIdempotencyKey)) {
      issues.push(
        createPlannedTaskValidationIssue({
          code: "PLANNED_TASK_DUPLICATE_IDEMPOTENCY_KEY",
          message: `Planned task list contains a duplicate idempotency key "${executionIdempotencyKey}" for ${describePlannedTask(plannedTask, index)}. Each task must be uniquely identifiable.`,
          stage: "planned-task-validation",
          plannerTask: input.plannerTask,
          plannedTask,
          plannedTaskIndex: index,
          details: {
            idempotencyKey: executionIdempotencyKey,
          },
        }),
      );
    }

    if (seenIdempotencyKeys.has(enrichmentIdempotencyKey)) {
      issues.push(
        createPlannedTaskValidationIssue({
          code: "PLANNED_TASK_DUPLICATE_ENRICHMENT_IDEMPOTENCY_KEY",
          message: `The synthesized enrichment idempotency key "${enrichmentIdempotencyKey}" collides with another task for ${describePlannedTask(plannedTask, index)}.`,
          stage: "planned-task-validation",
          plannerTask: input.plannerTask,
          plannedTask,
          plannedTaskIndex: index,
          details: {
            idempotencyKey: enrichmentIdempotencyKey,
          },
        }),
      );
    }

    seenIdempotencyKeys.add(executionIdempotencyKey);
    seenIdempotencyKeys.add(enrichmentIdempotencyKey);

    for (const referenceKey of rawReferenceKeys) {
      const existingOwner = rawReferenceOwnerByKey.get(referenceKey);
      if (existingOwner) {
        issues.push(
          createPlannedTaskValidationIssue({
            code: "PLANNED_TASK_DUPLICATE_REFERENCE",
            message: `Planned task reference "${referenceKey}" is used more than once. ${describePlannedTask(plannedTask, index)} conflicts with ${describePlannedTask(existingOwner.plannedTask, existingOwner.index)}.`,
            stage: "planned-task-validation",
            plannerTask: input.plannerTask,
            plannedTask,
            plannedTaskIndex: index,
            details: {
              referenceKey,
              conflictingTaskIndex: existingOwner.index,
              conflictingTaskLocalId: existingOwner.plannedTask.localId,
              conflictingTaskIntent: existingOwner.plannedTask.intent,
            },
          }),
        );
        continue;
      }

      rawReferenceOwnerByKey.set(referenceKey, {
        index,
        plannedTask,
        executionIdempotencyKey,
        enrichmentIdempotencyKey,
        executionReferenceKey,
        enrichmentReferenceKey,
      });
    }

    return {
      index,
      plannedTask,
      executionIdempotencyKey,
      enrichmentIdempotencyKey,
      executionReferenceKey,
      enrichmentReferenceKey,
      rawReferenceKeys,
    };
  });

  return {
    plannedTaskMetadata,
    rawReferenceOwnerByKey,
    issues,
  };
}

function validatePlannedTaskDependencyGraph(input: {
  plannerTask: TaskRecord;
  plannedTaskMetadata: PlannedTaskMetadata[];
  rawReferenceOwnerByKey: Map<string, PlannedTaskReferenceOwner>;
}): PlanningValidationIssue[] {
  const issues: PlanningValidationIssue[] = [];
  const dependencyGraph = new Map<string, string[]>();

  for (const metadata of input.plannedTaskMetadata) {
    const dependencyNodeIds: string[] = [];

    for (const dependencyRef of metadata.plannedTask.dependsOn) {
      const owner = input.rawReferenceOwnerByKey.get(dependencyRef);

      if (!owner) {
        issues.push(
          createPlannedTaskValidationIssue({
            code: "PLANNED_TASK_DEPENDENCY_UNRESOLVED",
            message: `${describePlannedTask(metadata.plannedTask, metadata.index)} depends on "${dependencyRef}", but no task in the plan matches that reference. Fix the dependsOn values and return the full corrected task list.`,
            stage: "planned-task-graph",
            plannerTask: input.plannerTask,
            plannedTask: metadata.plannedTask,
            plannedTaskIndex: metadata.index,
            field: "dependsOn",
            details: {
              dependencyRef,
              availableReferences: Array.from(input.rawReferenceOwnerByKey.keys()),
            },
          }),
        );
        continue;
      }

      if (owner.executionIdempotencyKey === metadata.executionIdempotencyKey) {
        issues.push(
          createPlannedTaskValidationIssue({
            code: "PLANNED_TASK_SELF_DEPENDENCY",
            message: `${describePlannedTask(metadata.plannedTask, metadata.index)} cannot depend on itself (${dependencyRef}). Remove that self-dependency and return the full corrected task list.`,
            stage: "planned-task-graph",
            plannerTask: input.plannerTask,
            plannedTask: metadata.plannedTask,
            plannedTaskIndex: metadata.index,
            field: "dependsOn",
            details: {
              dependencyRef,
            },
          }),
        );
        continue;
      }

      dependencyNodeIds.push(owner.executionIdempotencyKey);
    }

    dependencyGraph.set(
      metadata.executionIdempotencyKey,
      Array.from(new Set(dependencyNodeIds)),
    );
  }

  issues.push(
    ...assertAcyclicPreparedDependencyGraph({
      plannerTask: input.plannerTask,
      dependencyGraph,
      metadataByNodeId: new Map(
        input.plannedTaskMetadata.map((metadata) => [
          metadata.executionIdempotencyKey,
          {
            plannedTask: metadata.plannedTask,
            plannedTaskIndex: metadata.index,
          },
        ]),
      ),
      stage: "planned-task-graph",
      cycleErrorCode: "PLANNED_TASK_DEPENDENCY_CYCLE",
      cycleMessageFactory: (plannedTask, plannedTaskIndex, cycleNodes) =>
        `${describePlannedTask(plannedTask, plannedTaskIndex)} participates in a dependency cycle (${cycleNodes.join(" -> ")}). Return a full corrected task list with an acyclic dependency order.`,
    }),
  );

  return issues;
}

function validateExpandedPreparedTaskGraph(input: {
  plannerTask: TaskRecord;
  preparedTasks: PreparedConcreteTask[];
}): PlanningValidationIssue[] {
  const issues: PlanningValidationIssue[] = [];
  const referenceOwnerByKey = new Map<string, PreparedConcreteTask>();
  const dependencyGraph = new Map<string, string[]>();
  const metadataByNodeId = new Map<
    string,
    {
      plannedTask: PlannedTaskDefinition;
      plannedTaskIndex: number;
    }
  >();

  for (const preparedTask of input.preparedTasks) {
    metadataByNodeId.set(preparedTask.idempotencyKey, {
      plannedTask: preparedTask.plannedTask,
      plannedTaskIndex: preparedTask.index,
    });

    for (const referenceKey of preparedTask.referenceKeys) {
      const existingOwner = referenceOwnerByKey.get(referenceKey);
      if (existingOwner) {
        issues.push(
          createPlannedTaskValidationIssue({
            code: "PLANNED_TASK_DUPLICATE_EXPANDED_REFERENCE",
            message: `Expanded planning graph reference "${referenceKey}" is used more than once. ${describePlannedTask(preparedTask.plannedTask, preparedTask.index)} (${preparedTask.variant}) conflicts with ${describePlannedTask(existingOwner.plannedTask, existingOwner.index)} (${existingOwner.variant}).`,
            stage: "expanded-task-validation",
            plannerTask: input.plannerTask,
            plannedTask: preparedTask.plannedTask,
            plannedTaskIndex: preparedTask.index,
            taskVariant: preparedTask.variant,
            details: {
              referenceKey,
              conflictingTaskVariant: existingOwner.variant,
            },
          }),
        );
        continue;
      }

      referenceOwnerByKey.set(referenceKey, preparedTask);
    }
  }

  for (const preparedTask of input.preparedTasks) {
    const dependencyNodeIds: string[] = [];

    for (const dependencyRef of preparedTask.dependencyRefs) {
      const owner = referenceOwnerByKey.get(dependencyRef);

      if (!owner) {
        issues.push(
          createPlannedTaskValidationIssue({
            code: "PLANNED_TASK_EXPANDED_DEPENDENCY_UNRESOLVED",
            message: `${describePlannedTask(preparedTask.plannedTask, preparedTask.index)} (${preparedTask.variant}) depends on synthesized reference "${dependencyRef}", but no expanded task matches that reference.`,
            stage: "expanded-task-validation",
            plannerTask: input.plannerTask,
            plannedTask: preparedTask.plannedTask,
            plannedTaskIndex: preparedTask.index,
            taskVariant: preparedTask.variant,
            details: {
              dependencyRef,
              variant: preparedTask.variant,
            },
          }),
        );
        continue;
      }

      if (owner.idempotencyKey === preparedTask.idempotencyKey) {
        issues.push(
          createPlannedTaskValidationIssue({
            code: "PLANNED_TASK_EXPANDED_SELF_DEPENDENCY",
            message: `${describePlannedTask(preparedTask.plannedTask, preparedTask.index)} (${preparedTask.variant}) cannot depend on itself (${dependencyRef}).`,
            stage: "expanded-task-validation",
            plannerTask: input.plannerTask,
            plannedTask: preparedTask.plannedTask,
            plannedTaskIndex: preparedTask.index,
            taskVariant: preparedTask.variant,
            details: {
              dependencyRef,
              variant: preparedTask.variant,
            },
          }),
        );
        continue;
      }

      dependencyNodeIds.push(owner.idempotencyKey);
    }

    dependencyGraph.set(
      preparedTask.idempotencyKey,
      Array.from(new Set(dependencyNodeIds)),
    );
  }

  issues.push(
    ...assertAcyclicPreparedDependencyGraph({
      plannerTask: input.plannerTask,
      dependencyGraph,
      metadataByNodeId,
      stage: "expanded-task-validation",
      cycleErrorCode: "PLANNED_TASK_EXPANDED_DEPENDENCY_CYCLE",
      cycleMessageFactory: (plannedTask, plannedTaskIndex, cycleNodes) =>
        `${describePlannedTask(plannedTask, plannedTaskIndex)} participates in an expanded dependency cycle (${cycleNodes.join(" -> ")}). Return a corrected task list so enrichment and execution dependencies remain acyclic.`,
    }),
  );

  return issues;
}

function verifyExistingConcreteTaskRecord(input: {
  plannerTask: TaskRecord;
  preparedTask: PreparedConcreteTask;
}): PlanningValidationIssue | null {
  const existingTask = input.preparedTask.existingTask;

  if (!existingTask) {
    return null;
  }

  if (
    existingTask.milestoneId !== input.plannerTask.milestoneId ||
    existingTask.jobId !== input.plannerTask.jobId ||
    existingTask.parentTaskId !== input.plannerTask._id
  ) {
    return createPlannedTaskValidationIssue({
      code: "PLANNED_TASK_EXISTING_RECORD_MISMATCH",
      message: `${describePlannedTask(input.preparedTask.plannedTask, input.preparedTask.index)} reuses idempotency key "${input.preparedTask.idempotencyKey}", but the existing task belongs to a different parent or milestone. Return a corrected task list with stable unique task identifiers.`,
      stage: "expanded-task-validation",
      plannerTask: input.plannerTask,
      plannedTask: input.preparedTask.plannedTask,
      plannedTaskIndex: input.preparedTask.index,
      taskVariant: input.preparedTask.variant,
      details: {
        idempotencyKey: input.preparedTask.idempotencyKey,
        existingTaskId: existingTask._id,
        existingMilestoneId: existingTask.milestoneId,
        existingParentTaskId: existingTask.parentTaskId,
      },
    });
  }

  if (
    String(existingTask.intent) !== String(input.preparedTask.createIntent) ||
    existingTask.target.agentId !== input.preparedTask.targetAgentId
  ) {
    return createPlannedTaskValidationIssue({
      code: "PLANNED_TASK_EXISTING_RECORD_SHAPE_MISMATCH",
      message: `${describePlannedTask(input.preparedTask.plannedTask, input.preparedTask.index)} reuses idempotency key "${input.preparedTask.idempotencyKey}", but the existing ${input.preparedTask.variant} task has a different intent or target agent. Return a corrected task list with unique task ids or consistent task shapes.`,
      stage: "expanded-task-validation",
      plannerTask: input.plannerTask,
      plannedTask: input.preparedTask.plannedTask,
      plannedTaskIndex: input.preparedTask.index,
      taskVariant: input.preparedTask.variant,
      details: {
        idempotencyKey: input.preparedTask.idempotencyKey,
        variant: input.preparedTask.variant,
        existingTaskId: existingTask._id,
        existingIntent: String(existingTask.intent),
        existingTargetAgentId: existingTask.target.agentId,
        expectedIntent: String(input.preparedTask.createIntent),
        expectedTargetAgentId: input.preparedTask.targetAgentId,
      },
    });
  }

  return null;
}

function assertAcyclicPreparedDependencyGraph(input: {
  plannerTask: TaskRecord;
  dependencyGraph: Map<string, string[]>;
  metadataByNodeId: Map<
    string,
    {
      plannedTask: PlannedTaskDefinition;
      plannedTaskIndex: number;
    }
  >;
  stage: PlanningValidationIssue["stage"];
  cycleErrorCode: string;
  cycleMessageFactory: (
    plannedTask: PlannedTaskDefinition,
    plannedTaskIndex: number,
    cycleNodes: string[],
  ) => string;
}): PlanningValidationIssue[] {
  const issues: PlanningValidationIssue[] = [];
  const visiting = new Set<string>();
  const visited = new Set<string>();
  const stack: string[] = [];

  const visit = (nodeId: string): void => {
    if (visited.has(nodeId)) {
      return;
    }

    if (visiting.has(nodeId)) {
      const cycleStartIndex = stack.indexOf(nodeId);
      const cycleNodes = [...stack.slice(cycleStartIndex), nodeId];
      const metadata = input.metadataByNodeId.get(nodeId);

      if (!metadata) {
        issues.push({
          code: input.cycleErrorCode,
          message: `Detected a dependency cycle in the milestone planning graph (${cycleNodes.join(" -> ")}), but no metadata was available for node ${nodeId}.`,
          stage: input.stage,
          details: {
            plannerTaskId: input.plannerTask._id,
            milestoneId: input.plannerTask.milestoneId,
            cycleNodes,
          },
        });
        return;
      }

      issues.push(
        createPlannedTaskValidationIssue({
          code: input.cycleErrorCode,
          message: input.cycleMessageFactory(
            metadata.plannedTask,
            metadata.plannedTaskIndex,
            cycleNodes,
          ),
          stage: input.stage,
          plannerTask: input.plannerTask,
          plannedTask: metadata.plannedTask,
          plannedTaskIndex: metadata.plannedTaskIndex,
          details: {
            cycleNodes,
          },
        }),
      );
      return;
    }

    visiting.add(nodeId);
    stack.push(nodeId);

    for (const dependencyNodeId of input.dependencyGraph.get(nodeId) ?? []) {
      visit(dependencyNodeId);
    }

    stack.pop();
    visiting.delete(nodeId);
    visited.add(nodeId);
  };

  for (const nodeId of input.dependencyGraph.keys()) {
    visit(nodeId);
  }

  return issues;
}

function toTargetAgentIds(input: PlanningTargetAgentIds): PlanningTargetAgentIds {
  return {
    projectOwnerAgentId: input.projectOwnerAgentId,
    projectManagerAgentId: input.projectManagerAgentId,
    implementerAgentId: input.implementerAgentId,
    qaAgentId: input.qaAgentId,
  };
}
