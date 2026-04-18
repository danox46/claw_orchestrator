import { type QueryFilter, Types } from "mongoose";
import { createLogger } from "../../config/logger";
import AuditModel, { type AuditModelType } from "./audit.model";

const logger = createLogger({
  module: "audit",
  component: "audit-service",
});

export type AuditCategory =
  | "intake"
  | "project"
  | "job"
  | "task"
  | "agent"
  | "artifact"
  | "execution"
  | "staging"
  | "policy"
  | "approval"
  | "security"
  | "system";

export type AuditStatus =
  | "attempted"
  | "succeeded"
  | "failed"
  | "blocked"
  | "canceled";

export type AuditActor = {
  kind: "system" | "agent" | "user";
  id: string;
  role?: string;
};

export type AuditContext = {
  environment?: "development" | "test" | "staging" | "production";
  metadata?: Record<string, unknown>;
};

export type CreateAuditLogInput = {
  projectId?: string;
  jobId?: string;
  taskId?: string;
  action: string;
  category: AuditCategory;
  actor: AuditActor;
  status: AuditStatus;
  message: string;
  context?: AuditContext;
  details?: Record<string, unknown>;
  errorCode?: string;
};

type AuditDocumentLike = {
  id: string;
  projectId?: Types.ObjectId | null;
  jobId?: Types.ObjectId | null;
  taskId?: Types.ObjectId | null;
  action: string;
  category: AuditCategory;
  actor: {
    kind: AuditActor["kind"];
    id: string;
    role?: string | null;
  };
  status: AuditStatus;
  message: string;
  context?: {
    environment?: "development" | "test" | "staging" | "production" | null;
    metadata?: Record<string, unknown> | null;
  } | null;
  details?: Record<string, unknown> | null;
  errorCode?: string | null;
  createdAt: Date;
};

export type AuditLogRecord = {
  _id: string;
  projectId?: string;
  jobId?: string;
  taskId?: string;
  action: string;
  category: AuditCategory;
  actor: AuditActor;
  status: AuditStatus;
  message: string;
  context?: AuditContext;
  details?: Record<string, unknown>;
  errorCode?: string;
  createdAt: Date;
};

type ServiceError = Error & {
  statusCode?: number;
  code?: string;
  details?: unknown;
};

function createServiceError(input: {
  message: string;
  code: string;
  statusCode: number;
  details?: unknown;
}): ServiceError {
  return Object.assign(new Error(input.message), {
    code: input.code,
    statusCode: input.statusCode,
    details: input.details,
  });
}

function toObjectId(value: string, fieldName: string): Types.ObjectId {
  if (!Types.ObjectId.isValid(value)) {
    throw createServiceError({
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

function mapAuditLog(document: AuditDocumentLike): AuditLogRecord {
  return {
    _id: document.id,
    ...(document.projectId ? { projectId: document.projectId.toString() } : {}),
    ...(document.jobId ? { jobId: document.jobId.toString() } : {}),
    ...(document.taskId ? { taskId: document.taskId.toString() } : {}),
    action: document.action,
    category: document.category,
    actor: {
      kind: document.actor.kind,
      id: document.actor.id,
      ...(document.actor.role ? { role: document.actor.role } : {}),
    },
    status: document.status,
    message: document.message,
    ...(document.context
      ? {
          context: {
            ...(document.context.environment
              ? { environment: document.context.environment }
              : {}),
            ...(document.context.metadata &&
            typeof document.context.metadata === "object" &&
            !Array.isArray(document.context.metadata)
              ? {
                  metadata: document.context.metadata as Record<
                    string,
                    unknown
                  >,
                }
              : {}),
          },
        }
      : {}),
    ...(document.details &&
    typeof document.details === "object" &&
    !Array.isArray(document.details)
      ? { details: document.details as Record<string, unknown> }
      : {}),
    ...(document.errorCode ? { errorCode: document.errorCode } : {}),
    createdAt: document.createdAt,
  };
}

export class AuditService {
  async createLog(input: CreateAuditLogInput): Promise<AuditLogRecord> {
    const created = await AuditModel.create({
      ...(input.projectId
        ? { projectId: toObjectId(input.projectId, "projectId") }
        : {}),
      ...(input.jobId ? { jobId: toObjectId(input.jobId, "jobId") } : {}),
      ...(input.taskId ? { taskId: toObjectId(input.taskId, "taskId") } : {}),
      action: input.action.trim(),
      category: input.category,
      actor: {
        kind: input.actor.kind,
        id: input.actor.id.trim(),
        ...(input.actor.role ? { role: input.actor.role.trim() } : {}),
      },
      status: input.status,
      message: input.message.trim(),
      ...(input.context
        ? {
            context: {
              ...(input.context.environment
                ? { environment: input.context.environment }
                : {}),
              ...(input.context.metadata
                ? { metadata: input.context.metadata }
                : {}),
            },
          }
        : {}),
      ...(input.details ? { details: input.details } : {}),
      ...(input.errorCode ? { errorCode: input.errorCode.trim() } : {}),
    });

    const record = mapAuditLog(created);

    logger.info(
      {
        auditId: record._id,
        category: record.category,
        action: record.action,
        status: record.status,
        projectId: record.projectId,
        jobId: record.jobId,
        taskId: record.taskId,
      },
      "Audit log created.",
    );

    return record;
  }

  async logAttempt(
    input: Omit<CreateAuditLogInput, "status">,
  ): Promise<AuditLogRecord> {
    return this.createLog({
      ...input,
      status: "attempted",
    });
  }

  async logSuccess(
    input: Omit<CreateAuditLogInput, "status">,
  ): Promise<AuditLogRecord> {
    return this.createLog({
      ...input,
      status: "succeeded",
    });
  }

  async logFailure(
    input: Omit<CreateAuditLogInput, "status"> & {
      errorCode?: string;
    },
  ): Promise<AuditLogRecord> {
    return this.createLog({
      ...input,
      status: "failed",
    });
  }

  async logBlocked(
    input: Omit<CreateAuditLogInput, "status">,
  ): Promise<AuditLogRecord> {
    return this.createLog({
      ...input,
      status: "blocked",
    });
  }

  async logCanceled(
    input: Omit<CreateAuditLogInput, "status">,
  ): Promise<AuditLogRecord> {
    return this.createLog({
      ...input,
      status: "canceled",
    });
  }

  async getLogById(auditId: string): Promise<AuditLogRecord | null> {
    if (!Types.ObjectId.isValid(auditId)) {
      throw createServiceError({
        message: `Invalid auditId: ${auditId}`,
        code: "INVALID_OBJECT_ID",
        statusCode: 400,
        details: {
          fieldName: "auditId",
          value: auditId,
        },
      });
    }

    const record = await AuditModel.findById(
      new Types.ObjectId(auditId),
    ).exec();
    return record ? mapAuditLog(record) : null;
  }

  async requireLogById(auditId: string): Promise<AuditLogRecord> {
    const record = await this.getLogById(auditId);

    if (!record) {
      throw createServiceError({
        message: `Audit log not found: ${auditId}`,
        code: "AUDIT_LOG_NOT_FOUND",
        statusCode: 404,
        details: {
          auditId,
        },
      });
    }

    return record;
  }

  async listLogs(input?: {
    projectId?: string;
    jobId?: string;
    taskId?: string;
    category?: AuditCategory;
    action?: string;
    status?: AuditStatus;
    actorKind?: AuditActor["kind"];
    actorId?: string;
    limit?: number;
    skip?: number;
  }): Promise<AuditLogRecord[]> {
    const filter: QueryFilter<AuditModelType> = {};

    if (input?.projectId) {
      filter.projectId = toObjectId(input.projectId, "projectId");
    }

    if (input?.jobId) {
      filter.jobId = toObjectId(input.jobId, "jobId");
    }

    if (input?.taskId) {
      filter.taskId = toObjectId(input.taskId, "taskId");
    }

    if (input?.category) {
      filter.category = input.category;
    }

    if (input?.action) {
      filter.action = input.action.trim();
    }

    if (input?.status) {
      filter.status = input.status;
    }

    if (input?.actorKind) {
      filter["actor.kind"] = input.actorKind;
    }

    if (input?.actorId) {
      filter["actor.id"] = input.actorId.trim();
    }

    const limit = Math.min(Math.max(input?.limit ?? 50, 1), 200);
    const skip = Math.max(input?.skip ?? 0, 0);

    const records = await AuditModel.find(filter)
      .sort({ createdAt: -1 })
      .skip(skip)
      .limit(limit)
      .exec();

    return records.map(mapAuditLog);
  }

  async listRecentLogsForJob(input: {
    jobId: string;
    limit?: number;
  }): Promise<AuditLogRecord[]> {
    return this.listLogs({
      jobId: input.jobId,
      limit: input.limit ?? 50,
    });
  }

  async listRecentLogsForProject(input: {
    projectId: string;
    limit?: number;
  }): Promise<AuditLogRecord[]> {
    return this.listLogs({
      projectId: input.projectId,
      limit: input.limit ?? 50,
    });
  }
}

export default AuditService;
