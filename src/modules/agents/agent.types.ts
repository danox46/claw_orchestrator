import type { JobRecord, JobsServicePort } from "../jobs/job.service";
import type {
  MilestoneRecord,
  MilestonesServicePort,
} from "../milestones/milestone.service";
import type {
  CreateTaskInput,
  TaskRecord,
  TasksServicePort,
} from "../tasks/task.service";
import type {
  OpenClawClient,
  OpenClawTaskStatusResponse,
} from "./openclaw.client";

export type ServiceError = Error & {
  statusCode?: number;
  code?: string;
  details?: unknown;
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
  taskTimeoutMs?: number;
  projectOwnerAgentId?: string;
  projectManagerAgentId?: string;
  implementerAgentId?: string;
  qaAgentId?: string;
};
