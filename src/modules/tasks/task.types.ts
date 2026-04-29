import type { ClientSession, Types } from "mongoose";
import type {
  TaskConstraints,
  TaskIntent,
  TaskIssuer,
  TaskStatus,
  TaskTarget,
  UpdateTaskInput as UpdateTaskRequest,
} from "./task.schemas";

export type {
  TaskConstraints,
  TaskIntent,
  TaskIssuer,
  TaskStatus,
  TaskTarget,
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

export type AssertTaskMilestoneBelongsToProjectInput = {
  projectId: string;
  milestoneId: string;
};

export type AssertTaskDependenciesInput = {
  projectId: string;
  milestoneId: string;
  dependencyIds: string[];
  currentTaskId?: string;
};

export type ResolveTaskBatchDependencyIdsInput = {
  explicitTaskIds: string[];
  dependencyRefs: string[];
  projectId: string;
  milestoneId: string;
  currentTaskId?: string;
};

export type ValidateTaskBatchDependencyTargetsInput = {
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
};

export interface TaskValidationServicePort {
  assertMilestoneBelongsToProject(
    input: AssertTaskMilestoneBelongsToProjectInput,
    session?: ClientSession,
  ): Promise<void>;
  assertDependenciesAreValid(
    input: AssertTaskDependenciesInput,
    session?: ClientSession,
  ): Promise<void>;
  areDependenciesSatisfied(task: TaskRecord): Promise<boolean>;
  validateMilestonePlanningBatch(
    batch: CommitMilestonePlanningBatchInput,
  ): Promise<ValidateMilestonePlanningBatchResult>;
  resolveBatchDependencyIds(
    input: ResolveTaskBatchDependencyIdsInput,
    referenceToTaskId: Map<string, string>,
    session?: ClientSession,
  ): Promise<string[]>;
}

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

export type TaskDocumentLike = {
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

export type TaskInputValidationIssue = {
  code: string;
  field: string;
  message: string;
  details?: Record<string, unknown>;
};

export type TaskServiceErrorDetails = Record<string, unknown>;

export type TaskServiceErrorLike = Error & {
  code?: string;
  statusCode?: number;
  details?: TaskServiceErrorDetails;
  cause?: unknown;
};
