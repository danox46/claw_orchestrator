import { randomUUID } from "node:crypto";
import type { CreateIntakeRequest } from "./intake.schemas";
import type {
  CreateProjectInput,
  ProjectsServicePort,
} from "../projects/project.service";
import type { JobRecord, JobsServicePort } from "../jobs/job.service";
import type {
  CreateMilestoneInput,
  MilestonesServicePort,
} from "../milestones/milestone.service";
import type { CreateTaskInput, TasksServicePort } from "../tasks/task.service";
import { NotFoundError } from "../../shared/errors/not-found-error";
import { ValidationError } from "../../shared/errors/validation-error";
import { stat } from "node:fs";

export type CreateIntakeJobResult = {
  projectId: string;
  jobId: string;
  milestoneId?: string;
  taskId?: string;
  status: string;
  message?: string;
};

export interface IntakeServicePort {
  createIntakeJob(input: CreateIntakeRequest): Promise<CreateIntakeJobResult>;
}

type IntakeAppType = CreateIntakeRequest["appType"];
type IntakeStack = CreateIntakeRequest["stack"];
type IntakeDeployment = CreateIntakeRequest["deployment"];
type IntakeRequestedBy = CreateIntakeRequest["requestedBy"];

type IntakeMode = "new_project" | "existing_project";
type IntakeRequestType = "fix" | "feature" | "patch" | "cleanup";

type FlexibleCreateIntakeRequest = CreateIntakeRequest & {
  mode?: IntakeMode;
  projectId?: string;
  requestType?: IntakeRequestType;
  request?: string;
  canonicalProjectRoot?: string;
  projectRoot?: string;
  rootPath?: string;
  workspaceRoot?: string;
};

export type CreateJobMetadata = {
  requestedBy?: IntakeRequestedBy;
  appType: IntakeAppType;
  stack: IntakeStack;
  deployment: IntakeDeployment;
  [key: string]: unknown;
};

export type ProjectPhasePlanningPromptInput = {
  projectName: string;
  userPrompt: string;
  appType?: IntakeAppType;
  stack?: IntakeStack;
  deployment?: IntakeDeployment;
};

export type MilestoneTaskPlanningPromptInput = {
  projectName: string;
  milestoneTitle: string;
  milestoneGoal?: string;
  milestoneDescription?: string;
  milestoneScope?: string[];
  milestoneAcceptanceCriteria?: string[];
  projectSummary?: string;
  userPrompt?: string;
};

export interface IntakePromptServicePort {
  buildProjectPhasePlanningPrompt(
    input: ProjectPhasePlanningPromptInput,
  ): string;
  buildMilestoneTaskPlanningPrompt?(
    input: MilestoneTaskPlanningPromptInput,
  ): string;
}

export type IntakeServiceDependencies = {
  projectsService: ProjectsServicePort;
  jobsService: JobsServicePort;
  milestonesService: MilestonesServicePort;
  tasksService: TasksServicePort;
  promptService: IntakePromptServicePort;
  ownerAgentId?: string;
  defaultRepoMode?: "local" | "github";
  defaultSandboxMode?: "off" | "non-main" | "all";
  defaultToolProfile?: string;
  defaultRequiredArtifacts?: string[];
  defaultAcceptanceCriteria?: string[];
};

type ProjectLookupRecord = {
  _id: string;
  name?: string;
  slug?: string;
  appType?: IntakeAppType;
  stack?: IntakeStack;
  repoMode?: "local" | "github";
  canonicalProjectRoot?: string;
  projectRoot?: string;
  rootPath?: string;
  workspaceRoot?: string;
  [key: string]: unknown;
};

type ProjectLookupService = {
  requireProjectById?(projectId: string): Promise<ProjectLookupRecord>;
  getProjectById?(projectId: string): Promise<ProjectLookupRecord | null>;
};

type MilestoneLookupRecord = {
  order?: number;
};

type MilestoneLookupService = {
  listMilestones?(input?: {
    projectId?: string;
    limit?: number;
  }): Promise<MilestoneLookupRecord[]>;
  countMilestones?(input?: { projectId?: string }): Promise<number>;
};

export class IntakeService implements IntakeServicePort {
  private readonly projectsService: ProjectsServicePort;
  private readonly jobsService: JobsServicePort;
  private readonly milestonesService: MilestonesServicePort;
  private readonly tasksService: TasksServicePort;
  private readonly promptService: IntakePromptServicePort;
  private readonly ownerAgentId: string;
  private readonly defaultRepoMode: "local" | "github";
  private readonly defaultSandboxMode: "off" | "non-main" | "all";
  private readonly defaultToolProfile: string;
  private readonly defaultRequiredArtifacts: string[];
  private readonly defaultAcceptanceCriteria: string[];

  constructor(dependencies: IntakeServiceDependencies) {
    this.projectsService = dependencies.projectsService;
    this.jobsService = dependencies.jobsService;
    this.milestonesService = dependencies.milestonesService;
    this.tasksService = dependencies.tasksService;
    this.promptService = dependencies.promptService;
    this.ownerAgentId = dependencies.ownerAgentId ?? "project-owner";
    this.defaultRepoMode = dependencies.defaultRepoMode ?? "local";
    this.defaultSandboxMode = dependencies.defaultSandboxMode ?? "non-main";
    this.defaultToolProfile =
      dependencies.defaultToolProfile ?? "planning-agent-safe";
    this.defaultRequiredArtifacts = dependencies.defaultRequiredArtifacts ?? [];
    this.defaultAcceptanceCriteria = dependencies.defaultAcceptanceCriteria ?? [
      "phases_are_ordered",
      "phases_define_clear_goals",
      "phases_define_completion_criteria",
      "phases_are_implementation_ready",
    ];
  }

  async createIntakeJob(
    input: CreateIntakeRequest,
  ): Promise<CreateIntakeJobResult> {
    const intakeInput = input as FlexibleCreateIntakeRequest;

    if (this.isExistingProjectMode(intakeInput)) {
      return this.createExistingProjectIntakeJob(intakeInput);
    }

    return this.createNewProjectIntakeJob(input);
  }

  private async createNewProjectIntakeJob(
    input: CreateIntakeRequest,
  ): Promise<CreateIntakeJobResult> {
    const projectName = input.name.trim();
    const userPrompt = input.prompt.trim();
    const slug = this.buildProjectSlug(projectName);

    const canonicalProjectRoot = this.requireNewProjectCanonicalProjectRoot(
      input as FlexibleCreateIntakeRequest,
    );

    const projectInput: CreateProjectInput = {
      name: projectName,
      slug,
      appType: input.appType,
      stack: input.stack,
      repoMode: this.defaultRepoMode,
      canonicalProjectRoot,
    };

    const project = await this.projectsService.createProject(projectInput);

    const metadata: CreateJobMetadata = {
      appType: input.appType,
      stack: input.stack,
      deployment: input.deployment,
      ...(typeof input.requestedBy === "string" &&
      input.requestedBy.trim().length > 0
        ? { requestedBy: input.requestedBy.trim() }
        : {}),
      mode: "new_project",
      isProjectUpdate: false,
      canonicalProjectRoot,
    };

    const job = await this.jobsService.createJob({
      projectId: project._id,
      type: "create-app",
      state: "INTAKE",
      prompt: userPrompt,
      metadata,
    });

    const planningMilestoneInput: CreateMilestoneInput = {
      projectId: project._id,
      title: "Project Planning",
      description:
        "Initial planning milestone where the project owner defines the ordered project phases.",
      order: 0,
      status: "ready",
      goal: "Define the ordered milestones required to deliver the requested application.",
      scope: [
        "analyze the project request",
        "define ordered project phases",
        "define milestone goals",
        "define milestone exit criteria",
      ],
      acceptanceCriteria: [
        "milestones are ordered",
        "milestones reflect the requested app scope",
        "each milestone has a clear goal",
        "milestones are ready for PM task planning",
      ],
    };

    const planningMilestone = await this.milestonesService.createMilestone(
      planningMilestoneInput,
    );

    const ownerPlanningPrompt = this.buildOwnerPlanningPrompt({
      projectName,
      userPrompt,
      appType: input.appType,
      stack: input.stack,
      deployment: input.deployment,
    });

    const planningTaskInput: CreateTaskInput = {
      jobId: job._id,
      projectId: project._id,
      milestoneId: planningMilestone._id,
      dependencies: [],
      issuer: {
        kind: "system",
        id: "app-factory-orchestrator",
        role: "orchestrator",
      },
      target: {
        agentId: this.ownerAgentId,
      },
      intent: "plan_project_phases",
      inputs: {
        prompt: ownerPlanningPrompt,
        projectName,
        appType: input.appType,
        stack: input.stack,
        deployment: input.deployment,
        isProjectUpdate: false,
        mode: "new_project",
        canonicalProjectRoot,
      },
      constraints: {
        toolProfile: this.defaultToolProfile,
        sandbox: this.defaultSandboxMode,
      },
      requiredArtifacts: [...this.defaultRequiredArtifacts],
      acceptanceCriteria: [...this.defaultAcceptanceCriteria],
      idempotencyKey: this.buildIdempotencyKey(job._id, "plan_project_phases"),
      status: "queued",
      sequence: 0,
      artifacts: [],
      errors: [],
    };

    const planningTask = await this.tasksService.createTask(planningTaskInput);

    return {
      projectId: project._id,
      jobId: job._id,
      milestoneId: planningMilestone._id,
      taskId: planningTask._id,
      status: "queued",
      message:
        "Project, job, planning milestone, and initial owner planning task created successfully.",
    };
  }

  private async createExistingProjectIntakeJob(
    input: FlexibleCreateIntakeRequest,
  ): Promise<CreateIntakeJobResult> {
    const projectId = input.projectId?.trim();

    if (!projectId) {
      throw new ValidationError({
        message: "projectId is required when mode is existing_project.",
        code: "INTAKE_PROJECT_ID_REQUIRED",
        details: {
          mode: input.mode,
        },
        statusCode: 400,
      });
    }

    const userPrompt = this.resolveExistingProjectRequest(input);
    const requestType = this.resolveRequestType(input.requestType);
    const project = await this.requireProjectById(projectId);
    const projectJobs = await this.jobsService.listJobs({
      projectId,
      limit: 100,
    });

    this.ensureNoActiveUpdateJob(projectId, projectJobs);

    const baselineJob = this.requireBaselineJob(projectId, projectJobs);
    const canonicalProjectRoot = this.requireCanonicalProjectRoot(
      project,
      baselineJob,
    );

    const projectName =
      this.readNonEmptyString(project.name) ??
      this.deriveProjectNameFromJob(baselineJob) ??
      `project-${projectId}`;

    const appType = this.resolveAppType(project, baselineJob, input);
    const stack = this.resolveStack(project, baselineJob, input);
    const deployment = this.resolveDeployment(baselineJob, input);
    const planningMilestoneOrder = await this.getNextMilestoneOrder(projectId);

    const metadata: CreateJobMetadata = {
      appType,
      stack,
      deployment,
      ...(typeof input.requestedBy === "string" &&
      input.requestedBy.trim().length > 0
        ? { requestedBy: input.requestedBy.trim() }
        : {}),
      mode: "existing_project",
      isProjectUpdate: true,
      requestType,
      sourceJobId: baselineJob._id,
      previousSuccessfulJobId: baselineJob._id,
      canonicalProjectRoot,
    };

    await this.ensureProjectIsActive(projectId, project);

    const job = await this.jobsService.createJob({
      projectId,
      type: "update-app",
      state: "INTAKE",
      prompt: userPrompt,
      metadata,
    });

    const planningMilestone = await this.milestonesService.createMilestone({
      projectId,
      title: "Project Update Planning",
      description:
        "Update planning milestone where the project owner selects the smallest milestone needed for the requested change.",
      order: planningMilestoneOrder,
      status: "ready",
      goal: "Define the next valid milestone needed to implement the requested update on the existing project.",
      scope: [
        "inspect the current project context",
        "preserve existing working behavior unless the request changes it",
        "define the smallest valid update milestone",
        "avoid re-planning already accepted scope",
      ],
      acceptanceCriteria: [
        "the update milestone is scoped to the new request",
        "existing accepted behavior is preserved unless intentionally changed",
        "the milestone is small and implementation-ready",
        "the update plan is grounded in the canonical project root",
      ],
    });

    const ownerPlanningPrompt = this.buildProjectUpdatePlanningPrompt({
      projectId,
      projectName,
      canonicalProjectRoot,
      requestType,
      userPrompt,
      baselineJob,
      appType,
      stack,
      deployment,
    });

    const planningTaskInput: CreateTaskInput = {
      jobId: job._id,
      projectId,
      milestoneId: planningMilestone._id,
      dependencies: [],
      issuer: {
        kind: "system",
        id: "app-factory-orchestrator",
        role: "orchestrator",
      },
      target: {
        agentId: this.ownerAgentId,
      },
      intent: "plan_project_update",
      inputs: {
        prompt: ownerPlanningPrompt,
        projectId,
        projectName,
        appType,
        stack,
        deployment,
        requestType,
        isProjectUpdate: true,
        canonicalProjectRoot,
        sourceJobId: baselineJob._id,
        previousSuccessfulJobId: baselineJob._id,
        mode: "existing_project",
      },
      constraints: {
        toolProfile: this.defaultToolProfile,
        sandbox: this.defaultSandboxMode,
      },
      requiredArtifacts: [...this.defaultRequiredArtifacts],
      acceptanceCriteria: [...this.defaultAcceptanceCriteria],
      idempotencyKey: this.buildIdempotencyKey(job._id, "plan_project_update"),
      status: "queued",
      sequence: 0,
      artifacts: [],
      errors: [],
    };

    const planningTask = await this.tasksService.createTask(planningTaskInput);

    return {
      projectId,
      jobId: job._id,
      milestoneId: planningMilestone._id,
      taskId: planningTask._id,
      status: "queued",
      message:
        "Existing-project update job, planning milestone, and update planning task created successfully.",
    };
  }

  private buildOwnerPlanningPrompt(
    input: ProjectPhasePlanningPromptInput,
  ): string {
    return this.promptService.buildProjectPhasePlanningPrompt(input);
  }

  private buildProjectUpdatePlanningPrompt(input: {
    projectId: string;
    projectName: string;
    canonicalProjectRoot: string;
    requestType: IntakeRequestType;
    userPrompt: string;
    baselineJob: JobRecord;
    appType: IntakeAppType;
    stack: IntakeStack;
    deployment: IntakeDeployment;
  }): string {
    const baselineMetadata = this.getJobMetadataRecord(input.baselineJob);
    const latestReviewOutcome = this.pickFirstNonEmptyString([
      this.readMetadataString(baselineMetadata, "latestReviewOutcome"),
      this.readMetadataString(baselineMetadata, "reviewOutcome"),
      input.baselineJob.state,
    ]);
    const latestAcceptedMilestoneSummary = this.pickFirstNonEmptyString([
      this.readMetadataString(
        baselineMetadata,
        "latestAcceptedMilestoneSummary",
      ),
      this.readMetadataString(baselineMetadata, "latestMilestoneSummary"),
      this.readMetadataString(baselineMetadata, "milestoneSummary"),
      this.truncate(input.baselineJob.prompt, 240),
    ]);

    const compactUpdatePrompt = [
      "This is work on an existing project.",
      `Project ID: ${input.projectId}`,
      `Canonical project root: ${input.canonicalProjectRoot}`,
      `Request type: ${input.requestType}`,
      `Previous successful job ID: ${input.baselineJob._id}`,
      `Latest accepted milestone summary: ${latestAcceptedMilestoneSummary}`,
      `Latest review outcome: ${latestReviewOutcome}`,
      `New user request: ${input.userPrompt}`,
      "Preserve existing working behavior unless the request changes it.",
      "Prefer the smallest milestone that satisfies the request.",
      "Avoid re-planning already accepted scope.",
      "Work inside the provided canonicalProjectRoot.",
    ].join("\n");

    return this.promptService.buildProjectPhasePlanningPrompt({
      projectName: input.projectName,
      userPrompt: compactUpdatePrompt,
      appType: input.appType,
      stack: input.stack,
      deployment: input.deployment,
    });
  }

  private isExistingProjectMode(
    input: FlexibleCreateIntakeRequest,
  ): input is FlexibleCreateIntakeRequest & {
    mode: "existing_project";
    projectId: string;
  } {
    return input.mode === "existing_project";
  }

  private resolveExistingProjectRequest(
    input: FlexibleCreateIntakeRequest,
  ): string {
    const request = this.pickFirstNonEmptyString([input.request, input.prompt]);

    if (!request) {
      throw new ValidationError({
        message: "request or prompt is required when mode is existing_project.",
        code: "INTAKE_UPDATE_REQUEST_REQUIRED",
        details: {
          mode: input.mode,
          projectId: input.projectId,
        },
        statusCode: 400,
      });
    }

    return request;
  }

  private resolveRequestType(value?: string): IntakeRequestType {
    if (
      value === "fix" ||
      value === "feature" ||
      value === "patch" ||
      value === "cleanup"
    ) {
      return value;
    }

    return "feature";
  }

  private requireNewProjectCanonicalProjectRoot(
    input: FlexibleCreateIntakeRequest,
  ): string {
    const canonicalProjectRoot = this.pickFirstNonEmptyString([
      input.canonicalProjectRoot,
      input.projectRoot,
      input.rootPath,
      input.workspaceRoot,
    ]);

    if (canonicalProjectRoot) {
      return canonicalProjectRoot;
    }

    throw new ValidationError({
      message: "canonicalProjectRoot is required when creating a new project.",
      code: "INTAKE_CANONICAL_PROJECT_ROOT_REQUIRED",
      details: {
        mode: input.mode ?? "new_project",
      },
      statusCode: 400,
    });
  }

  private async requireProjectById(
    projectId: string,
  ): Promise<ProjectLookupRecord> {
    const projectsService = this.projectsService as ProjectsServicePort &
      ProjectLookupService;

    if (typeof projectsService.requireProjectById === "function") {
      return projectsService.requireProjectById(projectId);
    }

    if (typeof projectsService.getProjectById === "function") {
      const project = await projectsService.getProjectById(projectId);

      if (project) {
        return project;
      }
    }

    throw new NotFoundError({
      message: `Project not found: ${projectId}`,
      code: "PROJECT_NOT_FOUND",
      details: { projectId },
    });
  }

  private async ensureProjectIsActive(
    projectId: string,
    project: ProjectLookupRecord,
  ): Promise<void> {
    const currentStatus = this.readNonEmptyString(project.status);

    if (currentStatus === "active") {
      return;
    }

    await this.projectsService.updateProject(projectId, {
      status: "active",
    });
  }

  private requireBaselineJob(projectId: string, jobs: JobRecord[]): JobRecord {
    const baselineJob = jobs.find(
      (job) =>
        !this.isProjectUpdateJob(job) &&
        this.isSuccessfulBaselineState(job.state),
    );

    if (baselineJob) {
      return baselineJob;
    }

    throw new ValidationError({
      message:
        "Existing-project updates require at least one accepted or successful baseline job.",
      code: "PROJECT_BASELINE_REQUIRED",
      details: {
        projectId,
      },
      statusCode: 400,
    });
  }

  private ensureNoActiveUpdateJob(projectId: string, jobs: JobRecord[]): void {
    const activeUpdateJob = jobs.find(
      (job) =>
        this.isProjectUpdateJob(job) && !this.isTerminalJobState(job.state),
    );

    if (!activeUpdateJob) {
      return;
    }

    throw new ValidationError({
      message:
        "This project already has an active update job. Finish or fail that job before starting another update.",
      code: "ACTIVE_UPDATE_JOB_EXISTS",
      details: {
        projectId,
        activeUpdateJobId: activeUpdateJob._id,
        activeUpdateJobState: activeUpdateJob.state,
      },
      statusCode: 400,
    });
  }

  private requireCanonicalProjectRoot(
    project: ProjectLookupRecord,
    baselineJob: JobRecord,
  ): string {
    const baselineMetadata = this.getJobMetadataRecord(baselineJob);
    const canonicalProjectRoot = this.pickFirstNonEmptyString([
      this.readNonEmptyString(project.canonicalProjectRoot),
      this.readNonEmptyString(project.projectRoot),
      this.readNonEmptyString(project.rootPath),
      this.readNonEmptyString(project.workspaceRoot),
      this.readMetadataString(baselineMetadata, "canonicalProjectRoot"),
      this.readMetadataString(baselineMetadata, "projectRoot"),
      this.readMetadataString(baselineMetadata, "rootPath"),
      this.readMetadataString(baselineMetadata, "workspaceRoot"),
    ]);

    if (!canonicalProjectRoot) {
      throw new ValidationError({
        message:
          "Existing-project updates require a known canonical project root.",
        code: "PROJECT_ROOT_REQUIRED",
        details: {
          projectId: project._id,
          baselineJobId: baselineJob._id,
        },
        statusCode: 400,
      });
    }

    return canonicalProjectRoot;
  }

  private resolveAppType(
    project: ProjectLookupRecord,
    baselineJob: JobRecord,
    input: FlexibleCreateIntakeRequest,
  ): IntakeAppType {
    const metadata = this.getJobMetadataRecord(baselineJob);
    const appType =
      (input as Partial<CreateIntakeRequest>).appType ??
      project.appType ??
      (metadata.appType as IntakeAppType | undefined);

    if (!appType) {
      throw new ValidationError({
        message:
          "Unable to resolve appType for the existing project update job.",
        code: "PROJECT_APP_TYPE_REQUIRED",
        details: {
          projectId: project._id,
          baselineJobId: baselineJob._id,
        },
        statusCode: 400,
      });
    }

    return appType;
  }

  private resolveStack(
    project: ProjectLookupRecord,
    baselineJob: JobRecord,
    input: FlexibleCreateIntakeRequest,
  ): IntakeStack {
    const metadata = this.getJobMetadataRecord(baselineJob);
    const stack =
      (input as Partial<CreateIntakeRequest>).stack ??
      project.stack ??
      (metadata.stack as IntakeStack | undefined);

    if (!stack) {
      throw new ValidationError({
        message: "Unable to resolve stack for the existing project update job.",
        code: "PROJECT_STACK_REQUIRED",
        details: {
          projectId: project._id,
          baselineJobId: baselineJob._id,
        },
        statusCode: 400,
      });
    }

    return stack;
  }

  private resolveDeployment(
    baselineJob: JobRecord,
    input: FlexibleCreateIntakeRequest,
  ): IntakeDeployment {
    const metadata = this.getJobMetadataRecord(baselineJob);
    const deployment =
      (input as Partial<CreateIntakeRequest>).deployment ??
      (metadata.deployment as IntakeDeployment | undefined);

    if (!deployment) {
      throw new ValidationError({
        message:
          "Unable to resolve deployment for the existing project update job.",
        code: "PROJECT_DEPLOYMENT_REQUIRED",
        details: {
          baselineJobId: baselineJob._id,
        },
        statusCode: 400,
      });
    }

    return deployment;
  }

  private deriveProjectNameFromJob(job: JobRecord): string | undefined {
    const metadata = this.getJobMetadataRecord(job);

    return this.pickFirstNonEmptyString([
      this.readMetadataString(metadata, "projectName"),
      this.readMetadataString(metadata, "name"),
    ]);
  }

  private async getNextMilestoneOrder(projectId: string): Promise<number> {
    const milestonesService = this.milestonesService as MilestonesServicePort &
      MilestoneLookupService;

    if (typeof milestonesService.listMilestones === "function") {
      const milestones = await milestonesService.listMilestones({
        projectId,
        limit: 200,
      });
      const maxOrder = milestones.reduce((currentMax, milestone) => {
        return typeof milestone.order === "number"
          ? Math.max(currentMax, milestone.order)
          : currentMax;
      }, -1);

      return maxOrder + 1;
    }

    if (typeof milestonesService.countMilestones === "function") {
      return milestonesService.countMilestones({ projectId });
    }

    return 0;
  }

  private isProjectUpdateJob(job: JobRecord): boolean {
    return job.type !== "create-app";
  }

  private isSuccessfulBaselineState(state: string): boolean {
    return (
      state === "DEPLOYED" ||
      state === "STAGING_READY" ||
      state === "TEST_READY" ||
      state === "SECURITY_READY" ||
      state === "CODE_READY" ||
      state === "SCAFFOLD_READY" ||
      state === "ARCH_READY" ||
      state === "SPEC_READY" ||
      state === "PLAN_READY" ||
      state === "COMPLETED"
    );
  }

  private isTerminalJobState(state: string): boolean {
    return state === "FAILED" || state === "DEPLOYED";
  }

  private getJobMetadataRecord(job: JobRecord): Record<string, unknown> {
    return job.metadata as Record<string, unknown>;
  }

  private readMetadataString(
    metadata: Record<string, unknown>,
    key: string,
  ): string | undefined {
    const value = metadata[key];
    return typeof value === "string" && value.trim().length > 0
      ? value.trim()
      : undefined;
  }

  private readNonEmptyString(value: unknown): string | undefined {
    return typeof value === "string" && value.trim().length > 0
      ? value.trim()
      : undefined;
  }

  private pickFirstNonEmptyString(
    values: Array<string | undefined>,
  ): string | undefined {
    return values.find(
      (value) => typeof value === "string" && value.length > 0,
    );
  }

  private truncate(value: string, maxLength: number): string {
    if (value.length <= maxLength) {
      return value;
    }

    return `${value.slice(0, Math.max(maxLength - 1, 0))}…`;
  }

  private buildProjectSlug(name: string): string {
    const baseSlug = name
      .trim()
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "-")
      .replace(/^-+|-+$/g, "")
      .replace(/-{2,}/g, "-");

    const safeBaseSlug = baseSlug.length > 0 ? baseSlug : "app";
    const shortSuffix = randomUUID().slice(0, 8);

    return `${safeBaseSlug}-${shortSuffix}`;
  }

  private buildIdempotencyKey(jobId: string, intent: string): string {
    return `${jobId}:${intent}`;
  }
}

export default IntakeService;
