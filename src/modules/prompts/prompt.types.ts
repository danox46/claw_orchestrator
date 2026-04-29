import type { promptConfig } from "./prompt.config";
export type { PromptGlobalLayerKey } from "./prompt.config";

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

export type ProjectUpdatePlanningPromptInput = {
  projectId?: string;
  projectName: string;
  userRequest: string;
  requestType?: string;
  canonicalProjectRoot?: string;
  appType?: PromptContextValue;
  stack?: PromptContextValue;
  deployment?: PromptContextValue;
  latestAcceptedMilestoneSummary?: string;
  latestReviewOutcome?: string;
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

export type PromptAgentKey = keyof typeof promptConfig.agents;
export type PromptIntentKey = keyof typeof promptConfig.intents;
export type PromptRetryKey = keyof typeof promptConfig.retries;
export type OutputReminderKey = keyof typeof promptConfig.outputReminders;
