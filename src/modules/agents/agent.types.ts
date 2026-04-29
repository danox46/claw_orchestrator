import type { JobRecord, JobsServicePort } from "../jobs/job.service";
import type { MilestonesServicePort } from "../milestones/milestone.service";
import type { ProjectsServicePort } from "../projects/project.service";
import type {
  CreateTaskInput,
  TaskRecord,
  TasksServicePort,
} from "../tasks/task.service";
import type { OpenClawClient } from "./openclaw.client";

export type AgentDispatchLogger = {
  warn: (bindings: Record<string, unknown>, message: string) => void;
};

export type AgentPlanningBatchLogger = AgentDispatchLogger & {
  info: (bindings: Record<string, unknown>, message: string) => void;
};

export type AgentRetryServiceDependencies = {
  tasksService: TasksServicePort;
  jobsService: JobsServicePort;
  implementerAgentId: string;
};

export type AgentPlanningBatchServiceDependencies = {
  tasksService: TasksServicePort;
  milestonesService: MilestonesServicePort;
  projectOwnerAgentId: string;
  projectManagerAgentId: string;
  implementerAgentId: string;
  qaAgentId: string;
};

export type AgentRetryFailureInput = {
  job: JobRecord;
  task: TaskRecord;
  attemptNumber: number;
  dispatchLogger: AgentDispatchLogger;
  failureMessage: string;
  error?: unknown;
  outputs?: Record<string, unknown>;
  artifacts?: unknown;
};

export type AgentRotateTaskSessionInput = {
  task: TaskRecord;
  dispatchLogger: AgentDispatchLogger;
  failureMessage: string;
  outputs?: Record<string, unknown>;
  artifacts?: string[];
  resetTargetAgentId?: string;
};

export type ServiceError = Error & {
  statusCode?: number;
  code?: string;
  details?: unknown;
  retryable?: boolean;
};

export type PlanningValidationIssue = {
  code: string;
  message: string;
  stage:
    | "planned-task-validation"
    | "planned-task-graph"
    | "expanded-task-validation"
    | "batch-preflight";
  plannedTaskIndex?: number;
  taskLocalId?: string;
  taskIntent?: string;
  taskVariant?: "enrichment" | "execution" | "review";
  operationKind?: "create" | "update";
  operationIndex?: number;
  ownerLabel?: string;
  idempotencyKey?: string;
  field?: string;
  details?: Record<string, unknown>;
};

export type PlannedPhase = {
  phaseId: string;
  name: string;
  goal?: string;
  description?: string;
  dependsOn: string[];
  inputs: Record<string, unknown>;
  deliverables: string[];
  exitCriteria: string[];
  raw: Record<string, unknown>;
};

export type PlannedTaskDefinition = {
  localId?: string;
  intent: string;
  targetAgentId?: string;
  inputs: Record<string, unknown>;
  constraints?: Record<string, unknown>;
  requiredArtifacts: string[];
  acceptanceCriteria: string[];
  idempotencyKey?: string;
  dependsOn: string[];
};

export type PatchMilestonePlan = {
  title: string;
  goal?: string;
  description?: string;
  scope: string[];
  acceptanceCriteria: string[];
};

export type MilestoneReviewOutcome = {
  decision: "pass" | "patch";
  summary?: string;
  metAcceptanceCriteria: string[];
  missingOrBrokenItems: string[];
  patchMilestone?: PatchMilestonePlan;
};

export type PreparedConcreteTask = {
  index: number;
  variant: "enrichment" | "execution";
  plannedTask: PlannedTaskDefinition;
  idempotencyKey: string;
  sequence: number;
  targetAgentId: string;
  createIntent: CreateTaskInput["intent"];
  inputs: Record<string, unknown>;
  constraints: CreateTaskInput["constraints"];
  referenceKeys: string[];
  dependencyRefs: string[];
  existingTask?: TaskRecord;
};

export type AtomicMilestonePlanningBatchMutation = {
  referenceKeys: string[];
  dependencyTaskIds: string[];
  dependencyRefs: string[];
};

export type AtomicMilestonePlanningBatchCreate =
  AtomicMilestonePlanningBatchMutation & {
    kind: "create";
    task: CreateTaskInput;
  };

export type AtomicMilestonePlanningBatchUpdate =
  AtomicMilestonePlanningBatchMutation & {
    kind: "update";
    taskId: string;
    patch: Record<string, unknown>;
  };

export type AtomicMilestonePlanningBatch = {
  plannerTaskId: string;
  jobId: string;
  projectId: string;
  milestoneId: string;
  creates: AtomicMilestonePlanningBatchCreate[];
  updates: AtomicMilestonePlanningBatchUpdate[];
};

export type CommittedMilestonePlanningBatchTask = {
  stage: "validation" | "seed_updates" | "create" | "update" | "finalize";
  operationKind: "create" | "update";
  operationIndex: number;
  taskId: string;
  ownerLabel: string;
  referenceKeys: string[];
  dependencyTaskIds: string[];
  dependencyRefs: string[];
  taskIntent?: string;
  idempotencyKey?: string;
};

export type AtomicMilestonePlanningBatchResult = {
  createdTaskIds: string[];
  updatedTaskIds: string[];
  reviewTaskId: string;
  reviewTaskCreated: boolean;
  reviewTaskUpdated: boolean;
  createdTasks: CommittedMilestonePlanningBatchTask[];
  updatedTasks: CommittedMilestonePlanningBatchTask[];
};

export type AtomicMilestonePlanningBatchCommitter = (
  batch: AtomicMilestonePlanningBatch,
) => Promise<AtomicMilestonePlanningBatchResult>;

export type TaskSessionState = {
  sessionName: string;
  sessionCount: number;
  maxSessions: number;
};

export type AgentDispatchServiceDependencies = {
  openClawClient: OpenClawClient;
  tasksService: TasksServicePort;
  jobsService: JobsServicePort;
  milestonesService: MilestonesServicePort;
  projectsService?: ProjectsServicePort | undefined;
  taskTimeoutMs?: number;
  projectOwnerAgentId?: string;
  projectManagerAgentId?: string;
  implementerAgentId?: string;
  qaAgentId?: string;
};
