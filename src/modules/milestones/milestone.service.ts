import { type QueryFilter, Types } from "mongoose";
import { ConflictError } from "../../shared/errors/conflict-error";
import { NotFoundError } from "../../shared/errors/not-found-error";
import { ValidationError } from "../../shared/errors/validation-error";
import MilestoneModel, { type MilestoneModelType } from "./milestone.model";
import type {
  MilestoneStatus,
  UpdateMilestoneInput as UpdateMilestoneRequest,
} from "./milestone.schemas";

export type CreateMilestoneInput = {
  projectId: string;
  title: string;
  description?: string;
  order: number;
  status?: MilestoneStatus;
  goal?: string;
  scope?: string[];
  acceptanceCriteria?: string[];
  dependsOnMilestoneId?: string;
  startedAt?: Date;
  completedAt?: Date;
  confirmedAt?: Date;
};

export type UpdateMilestoneInput = UpdateMilestoneRequest;

export type MilestoneRecord = {
  _id: string;
  projectId: string;
  title: string;
  description?: string;
  order: number;
  status: MilestoneStatus;
  goal?: string;
  scope: string[];
  acceptanceCriteria: string[];
  dependsOnMilestoneId?: string;
  startedAt?: Date;
  completedAt?: Date;
  confirmedAt?: Date;
  createdAt: Date;
  updatedAt: Date;
};

export type ListMilestonesInput = {
  projectId?: string;
  status?: MilestoneStatus;
  dependsOnMilestoneId?: string;
  limit?: number;
  skip?: number;
};

export type CountMilestonesInput = Pick<
  ListMilestonesInput,
  "projectId" | "status" | "dependsOnMilestoneId"
>;

export interface MilestonesServicePort {
  createMilestone(input: CreateMilestoneInput): Promise<MilestoneRecord>;
  getMilestoneById(milestoneId: string): Promise<MilestoneRecord | null>;
  requireMilestoneById(milestoneId: string): Promise<MilestoneRecord>;
  updateMilestone(
    milestoneId: string,
    updates: UpdateMilestoneInput,
  ): Promise<MilestoneRecord>;
  setStatus(
    milestoneId: string,
    status: MilestoneStatus,
  ): Promise<MilestoneRecord>;
  startMilestone(milestoneId: string): Promise<MilestoneRecord>;
  moveToReview(milestoneId: string): Promise<MilestoneRecord>;
  completeMilestone(milestoneId: string): Promise<MilestoneRecord>;
  cancelMilestone(
    milestoneId: string,
    reason?: string,
  ): Promise<MilestoneRecord>;
  listMilestones(input?: ListMilestonesInput): Promise<MilestoneRecord[]>;
  countMilestones(input?: CountMilestonesInput): Promise<number>;
  getCurrentActiveMilestone(projectId: string): Promise<MilestoneRecord | null>;
  getNextStartableMilestone(projectId: string): Promise<MilestoneRecord | null>;
  canStartMilestone(milestoneId: string): Promise<boolean>;
}

type MilestoneDocumentLike = {
  id: string;
  projectId: Types.ObjectId;
  title: string;
  description?: string | null;
  order: number;
  status: MilestoneStatus;
  goal?: string | null;
  scope?: string[] | null;
  acceptanceCriteria?: string[] | null;
  dependsOnMilestoneId?: Types.ObjectId | null;
  startedAt?: Date | null;
  completedAt?: Date | null;
  confirmedAt?: Date | null;
  createdAt: Date;
  updatedAt: Date;
};

function toObjectId(value: string, fieldName: string): Types.ObjectId {
  if (!Types.ObjectId.isValid(value)) {
    throw new ValidationError({
      message: `Invalid ${fieldName}: ${value}`,
      code: "INVALID_OBJECT_ID",
      statusCode: 400,
      details: {
        fieldName,
        value,
      },
    });
  }

  return new Types.ObjectId(value);
}

function mapMilestone(document: MilestoneDocumentLike): MilestoneRecord {
  return {
    _id: document.id,
    projectId: document.projectId.toString(),
    title: document.title,
    ...(typeof document.description === "string" &&
    document.description.length > 0
      ? { description: document.description }
      : {}),
    order: document.order,
    status: document.status,
    ...(typeof document.goal === "string" && document.goal.length > 0
      ? { goal: document.goal }
      : {}),
    scope: [...(document.scope ?? [])],
    acceptanceCriteria: [...(document.acceptanceCriteria ?? [])],
    ...(document.dependsOnMilestoneId
      ? { dependsOnMilestoneId: document.dependsOnMilestoneId.toString() }
      : {}),
    ...(document.startedAt instanceof Date
      ? { startedAt: document.startedAt }
      : {}),
    ...(document.completedAt instanceof Date
      ? { completedAt: document.completedAt }
      : {}),
    ...(document.confirmedAt instanceof Date
      ? { confirmedAt: document.confirmedAt }
      : {}),
    createdAt: document.createdAt,
    updatedAt: document.updatedAt,
  };
}

function normalizeCreateMilestoneInput(input: CreateMilestoneInput): {
  status: MilestoneStatus;
  scope: string[];
  acceptanceCriteria: string[];
} {
  return {
    status: input.status ?? "draft",
    scope: [...(input.scope ?? [])],
    acceptanceCriteria: [...(input.acceptanceCriteria ?? [])],
  };
}

export class MilestoneService implements MilestonesServicePort {
  async createMilestone(input: CreateMilestoneInput): Promise<MilestoneRecord> {
    try {
      const normalized = normalizeCreateMilestoneInput(input);
      const projectObjectId = toObjectId(input.projectId, "projectId");

      if (typeof input.dependsOnMilestoneId === "string") {
        await this.assertDependencyBelongsToProject({
          projectId: input.projectId,
          dependsOnMilestoneId: input.dependsOnMilestoneId,
        });
      }

      const created = await MilestoneModel.create({
        projectId: projectObjectId,
        title: input.title.trim(),
        ...(typeof input.description === "string"
          ? { description: input.description.trim() }
          : {}),
        order: input.order,
        status: normalized.status,
        ...(typeof input.goal === "string" ? { goal: input.goal.trim() } : {}),
        scope: normalized.scope,
        acceptanceCriteria: normalized.acceptanceCriteria,
        ...(typeof input.dependsOnMilestoneId === "string"
          ? {
              dependsOnMilestoneId: toObjectId(
                input.dependsOnMilestoneId,
                "dependsOnMilestoneId",
              ),
            }
          : {}),
        ...(input.startedAt instanceof Date
          ? { startedAt: input.startedAt }
          : {}),
        ...(input.completedAt instanceof Date
          ? { completedAt: input.completedAt }
          : {}),
        ...(input.confirmedAt instanceof Date
          ? { confirmedAt: input.confirmedAt }
          : {}),
      });

      return mapMilestone(created);
    } catch (error: unknown) {
      if (this.isDuplicateKeyError(error)) {
        throw new ConflictError({
          message: `A milestone with order "${input.order}" already exists for project "${input.projectId}".`,
          code: "MILESTONE_ORDER_ALREADY_EXISTS",
          details: {
            projectId: input.projectId,
            order: input.order,
          },
          cause: error,
        });
      }

      throw error;
    }
  }

  async getMilestoneById(milestoneId: string): Promise<MilestoneRecord | null> {
    const milestone = await MilestoneModel.findById(
      toObjectId(milestoneId, "milestoneId"),
    ).exec();

    return milestone ? mapMilestone(milestone) : null;
  }

  async requireMilestoneById(milestoneId: string): Promise<MilestoneRecord> {
    const milestone = await this.getMilestoneById(milestoneId);

    if (!milestone) {
      throw new NotFoundError({
        message: `Milestone not found: ${milestoneId}`,
        code: "MILESTONE_NOT_FOUND",
        details: { milestoneId },
      });
    }

    return milestone;
  }

  async updateMilestone(
    milestoneId: string,
    updates: UpdateMilestoneInput,
  ): Promise<MilestoneRecord> {
    const current = await this.requireMilestoneById(milestoneId);
    const updatePayload: Record<string, unknown> = {};

    if (typeof updates.title === "string") {
      updatePayload.title = updates.title.trim();
    }

    if (typeof updates.description === "string") {
      updatePayload.description = updates.description.trim();
    }

    if (updates.description === null) {
      updatePayload.description = null;
    }

    if (typeof updates.order === "number") {
      updatePayload.order = updates.order;
    }

    if (typeof updates.status === "string") {
      updatePayload.status = updates.status;
    }

    if (typeof updates.goal === "string") {
      updatePayload.goal = updates.goal.trim();
    }

    if (updates.goal === null) {
      updatePayload.goal = null;
    }

    if (updates.scope !== undefined) {
      updatePayload.scope = [...updates.scope];
    }

    if (updates.acceptanceCriteria !== undefined) {
      updatePayload.acceptanceCriteria = [...updates.acceptanceCriteria];
    }

    if (typeof updates.dependsOnMilestoneId === "string") {
      await this.assertDependencyBelongsToProject({
        projectId: current.projectId,
        dependsOnMilestoneId: updates.dependsOnMilestoneId,
        currentMilestoneId: milestoneId,
      });

      updatePayload.dependsOnMilestoneId = toObjectId(
        updates.dependsOnMilestoneId,
        "dependsOnMilestoneId",
      );
    }

    if (updates.dependsOnMilestoneId === null) {
      updatePayload.dependsOnMilestoneId = null;
    }

    if (updates.startedAt instanceof Date) {
      updatePayload.startedAt = updates.startedAt;
    }

    if (updates.startedAt === null) {
      updatePayload.startedAt = null;
    }

    if (updates.completedAt instanceof Date) {
      updatePayload.completedAt = updates.completedAt;
    }

    if (updates.completedAt === null) {
      updatePayload.completedAt = null;
    }

    if (updates.confirmedAt instanceof Date) {
      updatePayload.confirmedAt = updates.confirmedAt;
    }

    if (updates.confirmedAt === null) {
      updatePayload.confirmedAt = null;
    }

    try {
      const updated = await MilestoneModel.findByIdAndUpdate(
        toObjectId(milestoneId, "milestoneId"),
        updatePayload,
        {
          new: true,
          runValidators: true,
        },
      ).exec();

      if (!updated) {
        throw new NotFoundError({
          message: `Milestone not found: ${milestoneId}`,
          code: "MILESTONE_NOT_FOUND",
          details: { milestoneId },
        });
      }

      return mapMilestone(updated);
    } catch (error: unknown) {
      if (this.isDuplicateKeyError(error)) {
        throw new ConflictError({
          message: `A milestone with order "${updates.order}" already exists for project "${current.projectId}".`,
          code: "MILESTONE_ORDER_ALREADY_EXISTS",
          details: {
            projectId: current.projectId,
            order: updates.order,
          },
          cause: error,
        });
      }

      throw error;
    }
  }

  async setStatus(
    milestoneId: string,
    status: MilestoneStatus,
  ): Promise<MilestoneRecord> {
    return this.updateMilestone(milestoneId, { status });
  }

  async startMilestone(milestoneId: string): Promise<MilestoneRecord> {
    const milestone = await this.requireMilestoneById(milestoneId);
    await this.assertMilestoneCanStart(milestone);

    const updated = await MilestoneModel.findByIdAndUpdate(
      toObjectId(milestoneId, "milestoneId"),
      {
        $set: {
          status: "in_progress",
          startedAt: milestone.startedAt ?? new Date(),
        },
      },
      {
        new: true,
        runValidators: true,
      },
    ).exec();

    if (!updated) {
      throw new NotFoundError({
        message: `Milestone not found: ${milestoneId}`,
        code: "MILESTONE_NOT_FOUND",
        details: { milestoneId },
      });
    }

    return mapMilestone(updated);
  }

  async moveToReview(milestoneId: string): Promise<MilestoneRecord> {
    const milestone = await this.requireMilestoneById(milestoneId);

    if (milestone.status !== "in_progress") {
      throw new ConflictError({
        message: `Milestone ${milestoneId} cannot move to review from status "${milestone.status}".`,
        code: "MILESTONE_INVALID_REVIEW_TRANSITION",
        details: {
          milestoneId,
          currentStatus: milestone.status,
        },
      });
    }

    const updated = await MilestoneModel.findByIdAndUpdate(
      toObjectId(milestoneId, "milestoneId"),
      {
        $set: {
          status: "review",
        },
      },
      {
        new: true,
        runValidators: true,
      },
    ).exec();

    if (!updated) {
      throw new NotFoundError({
        message: `Milestone not found: ${milestoneId}`,
        code: "MILESTONE_NOT_FOUND",
        details: { milestoneId },
      });
    }

    return mapMilestone(updated);
  }

  async completeMilestone(milestoneId: string): Promise<MilestoneRecord> {
    const milestone = await this.requireMilestoneById(milestoneId);

    if (milestone.status !== "in_progress" && milestone.status !== "review") {
      throw new ConflictError({
        message: `Milestone ${milestoneId} cannot be completed from status "${milestone.status}".`,
        code: "MILESTONE_INVALID_COMPLETE_TRANSITION",
        details: {
          milestoneId,
          currentStatus: milestone.status,
        },
      });
    }

    const now = new Date();

    const updated = await MilestoneModel.findByIdAndUpdate(
      toObjectId(milestoneId, "milestoneId"),
      {
        $set: {
          status: "completed",
          completedAt: now,
          confirmedAt: now,
        },
      },
      {
        new: true,
        runValidators: true,
      },
    ).exec();

    if (!updated) {
      throw new NotFoundError({
        message: `Milestone not found: ${milestoneId}`,
        code: "MILESTONE_NOT_FOUND",
        details: { milestoneId },
      });
    }

    return mapMilestone(updated);
  }

  async cancelMilestone(
    milestoneId: string,
    reason?: string,
  ): Promise<MilestoneRecord> {
    const updated = await MilestoneModel.findByIdAndUpdate(
      toObjectId(milestoneId, "milestoneId"),
      {
        $set: {
          status: "cancelled",
          ...(typeof reason === "string" && reason.trim().length > 0
            ? {
                goal: reason.trim(),
              }
            : {}),
        },
      },
      {
        new: true,
        runValidators: true,
      },
    ).exec();

    if (!updated) {
      throw new NotFoundError({
        message: `Milestone not found: ${milestoneId}`,
        code: "MILESTONE_NOT_FOUND",
        details: { milestoneId },
      });
    }

    return mapMilestone(updated);
  }

  async listMilestones(
    input: ListMilestonesInput = {},
  ): Promise<MilestoneRecord[]> {
    const filter = this.buildFilter(input);
    const limit = Math.min(Math.max(input.limit ?? 25, 1), 100);
    const skip = Math.max(input.skip ?? 0, 0);

    const milestones = await MilestoneModel.find(filter)
      .sort({ order: 1, createdAt: 1 })
      .skip(skip)
      .limit(limit)
      .exec();

    return milestones.map(mapMilestone);
  }

  async countMilestones(input: CountMilestonesInput = {}): Promise<number> {
    const filter = this.buildFilter(input);
    return MilestoneModel.countDocuments(filter).exec();
  }

  async getCurrentActiveMilestone(
    projectId: string,
  ): Promise<MilestoneRecord | null> {
    const milestone = await MilestoneModel.findOne({
      projectId: toObjectId(projectId, "projectId"),
      status: "in_progress",
    })
      .sort({ order: 1, createdAt: 1 })
      .exec();

    return milestone ? mapMilestone(milestone) : null;
  }

  async getNextStartableMilestone(
    projectId: string,
  ): Promise<MilestoneRecord | null> {
    const activeMilestone = await this.getCurrentActiveMilestone(projectId);

    if (activeMilestone) {
      return null;
    }

    const candidates = await MilestoneModel.find({
      projectId: toObjectId(projectId, "projectId"),
      status: "ready",
    })
      .sort({ order: 1, createdAt: 1 })
      .exec();

    for (const candidate of candidates) {
      const startable = await this.isMilestoneStartable(
        mapMilestone(candidate),
      );

      if (startable) {
        return mapMilestone(candidate);
      }
    }

    return null;
  }

  async canStartMilestone(milestoneId: string): Promise<boolean> {
    const milestone = await this.requireMilestoneById(milestoneId);
    return this.isMilestoneStartable(milestone);
  }

  private buildFilter(
    input: CountMilestonesInput | ListMilestonesInput,
  ): QueryFilter<MilestoneModelType> {
    const filter: QueryFilter<MilestoneModelType> = {};

    if (typeof input.projectId === "string") {
      filter.projectId = toObjectId(input.projectId, "projectId");
    }

    if (input.status) {
      filter.status = input.status;
    }

    if (typeof input.dependsOnMilestoneId === "string") {
      filter.dependsOnMilestoneId = toObjectId(
        input.dependsOnMilestoneId,
        "dependsOnMilestoneId",
      );
    }

    return filter;
  }

  private async assertDependencyBelongsToProject(input: {
    projectId: string;
    dependsOnMilestoneId: string;
    currentMilestoneId?: string;
  }): Promise<void> {
    if (
      typeof input.currentMilestoneId === "string" &&
      input.currentMilestoneId === input.dependsOnMilestoneId
    ) {
      throw new ValidationError({
        message: "A milestone cannot depend on itself.",
        code: "MILESTONE_SELF_DEPENDENCY",
        statusCode: 400,
        details: {
          milestoneId: input.currentMilestoneId,
          dependsOnMilestoneId: input.dependsOnMilestoneId,
        },
      });
    }

    const dependency = await MilestoneModel.findById(
      toObjectId(input.dependsOnMilestoneId, "dependsOnMilestoneId"),
    ).exec();

    if (!dependency) {
      throw new NotFoundError({
        message: `Dependency milestone not found: ${input.dependsOnMilestoneId}`,
        code: "MILESTONE_DEPENDENCY_NOT_FOUND",
        details: {
          dependsOnMilestoneId: input.dependsOnMilestoneId,
        },
      });
    }

    if (dependency.projectId.toString() !== input.projectId) {
      throw new ValidationError({
        message: "Milestone dependency must belong to the same project.",
        code: "MILESTONE_DEPENDENCY_PROJECT_MISMATCH",
        statusCode: 400,
        details: {
          projectId: input.projectId,
          dependsOnMilestoneId: input.dependsOnMilestoneId,
        },
      });
    }
  }

  private async assertMilestoneCanStart(
    milestone: MilestoneRecord,
  ): Promise<void> {
    if (milestone.status !== "ready") {
      throw new ConflictError({
        message: `Milestone ${milestone._id} cannot be started from status "${milestone.status}".`,
        code: "MILESTONE_NOT_READY",
        details: {
          milestoneId: milestone._id,
          currentStatus: milestone.status,
        },
      });
    }

    const currentActive = await MilestoneModel.findOne({
      projectId: toObjectId(milestone.projectId, "projectId"),
      status: "in_progress",
      _id: { $ne: toObjectId(milestone._id, "milestoneId") },
    }).exec();

    if (currentActive) {
      throw new ConflictError({
        message: `Project ${milestone.projectId} already has an active milestone.`,
        code: "PROJECT_ALREADY_HAS_ACTIVE_MILESTONE",
        details: {
          projectId: milestone.projectId,
          activeMilestoneId: currentActive.id,
        },
      });
    }

    if (typeof milestone.dependsOnMilestoneId === "string") {
      const dependency = await this.requireMilestoneById(
        milestone.dependsOnMilestoneId,
      );

      if (dependency.status !== "completed") {
        throw new ConflictError({
          message: `Milestone ${milestone._id} depends on milestone ${dependency._id}, which is not completed.`,
          code: "MILESTONE_DEPENDENCY_NOT_COMPLETED",
          details: {
            milestoneId: milestone._id,
            dependsOnMilestoneId: dependency._id,
            dependencyStatus: dependency.status,
          },
        });
      }
    }

    const previousMilestone = await MilestoneModel.findOne({
      projectId: toObjectId(milestone.projectId, "projectId"),
      order: { $lt: milestone.order },
    })
      .sort({ order: -1 })
      .exec();

    if (previousMilestone && previousMilestone.status !== "completed") {
      throw new ConflictError({
        message: `Milestone ${milestone._id} cannot start before milestone ${previousMilestone.id} is completed.`,
        code: "PREVIOUS_MILESTONE_NOT_COMPLETED",
        details: {
          milestoneId: milestone._id,
          previousMilestoneId: previousMilestone.id,
          previousMilestoneStatus: previousMilestone.status,
        },
      });
    }
  }

  private async isMilestoneStartable(
    milestone: MilestoneRecord,
  ): Promise<boolean> {
    try {
      await this.assertMilestoneCanStart(milestone);
      return true;
    } catch {
      return false;
    }
  }

  private isDuplicateKeyError(error: unknown): boolean {
    if (
      typeof error === "object" &&
      error !== null &&
      "name" in error &&
      "code" in error
    ) {
      const candidate = error as {
        name?: unknown;
        code?: unknown;
      };

      return candidate.name === "MongoServerError" && candidate.code === 11000;
    }

    return false;
  }
}

export default MilestoneService;
