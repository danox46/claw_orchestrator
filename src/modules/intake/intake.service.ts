import { randomUUID } from "node:crypto";
import type { CreateIntakeRequest } from "./intake.schemas";
import type {
  CreateProjectInput,
  ProjectsServicePort,
} from "../projects/project.service";
import type { JobsServicePort } from "../jobs/job.service";
import type {
  CreateMilestoneInput,
  MilestonesServicePort,
} from "../milestones/milestone.service";
import type { CreateTaskInput, TasksServicePort } from "../tasks/task.service";

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

export type CreateJobMetadata = {
  requestedBy?: IntakeRequestedBy;
  appType: IntakeAppType;
  stack: IntakeStack;
  deployment: IntakeDeployment;
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
    const projectName = input.name.trim();
    const userPrompt = input.prompt.trim();
    const slug = this.buildProjectSlug(projectName);

    const projectInput: CreateProjectInput = {
      name: projectName,
      slug,
      appType: input.appType,
      stack: input.stack,
      repoMode: this.defaultRepoMode,
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

  private buildOwnerPlanningPrompt(
    input: ProjectPhasePlanningPromptInput,
  ): string {
    return this.promptService.buildProjectPhasePlanningPrompt(input);
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
