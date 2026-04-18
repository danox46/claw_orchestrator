import {
  Schema,
  Types,
  model,
  type HydratedDocument,
  type InferSchemaType,
} from "mongoose";

const auditActorSchema = new Schema(
  {
    kind: {
      type: String,
      enum: ["system", "agent", "user"],
      required: true,
    },
    id: {
      type: String,
      required: true,
      trim: true,
      minlength: 1,
      maxlength: 120,
    },
    role: {
      type: String,
      trim: true,
      default: undefined,
      maxlength: 120,
    },
  },
  {
    _id: false,
  },
);

const auditContextSchema = new Schema(
  {
    environment: {
      type: String,
      enum: ["development", "test", "staging", "production"],
      default: undefined,
    },
    metadata: {
      type: Schema.Types.Mixed,
      default: undefined,
    },
  },
  {
    _id: false,
  },
);

const auditSchema = new Schema(
  {
    projectId: {
      type: Types.ObjectId,
      ref: "Project",
      default: undefined,
      index: true,
    },

    jobId: {
      type: Types.ObjectId,
      ref: "Job",
      default: undefined,
      index: true,
    },

    taskId: {
      type: Types.ObjectId,
      ref: "Task",
      default: undefined,
      index: true,
    },

    action: {
      type: String,
      required: true,
      trim: true,
      minlength: 1,
      maxlength: 120,
      index: true,
    },

    category: {
      type: String,
      enum: [
        "intake",
        "project",
        "job",
        "task",
        "agent",
        "artifact",
        "execution",
        "staging",
        "policy",
        "approval",
        "security",
        "system",
      ],
      required: true,
      index: true,
    },

    actor: {
      type: auditActorSchema,
      required: true,
    },

    status: {
      type: String,
      enum: ["attempted", "succeeded", "failed", "blocked", "canceled"],
      required: true,
      index: true,
    },

    message: {
      type: String,
      required: true,
      trim: true,
      minlength: 1,
      maxlength: 5000,
    },

    context: {
      type: auditContextSchema,
      default: undefined,
    },

    details: {
      type: Schema.Types.Mixed,
      default: undefined,
    },

    errorCode: {
      type: String,
      trim: true,
      default: undefined,
      maxlength: 120,
    },
  },
  {
    timestamps: {
      createdAt: true,
      updatedAt: false,
    },
    versionKey: false,
    collection: "audit_logs",
  },
);

auditSchema.index({ createdAt: -1 });
auditSchema.index({ projectId: 1, createdAt: -1 });
auditSchema.index({ jobId: 1, createdAt: -1 });
auditSchema.index({ taskId: 1, createdAt: -1 });
auditSchema.index({ category: 1, action: 1, createdAt: -1 });
auditSchema.index({ status: 1, createdAt: -1 });
auditSchema.index({ "actor.kind": 1, "actor.id": 1, createdAt: -1 });

export type AuditDocument = HydratedDocument<
  InferSchemaType<typeof auditSchema>
>;
export type AuditModelType = InferSchemaType<typeof auditSchema>;

export const AuditModel = model("AuditLog", auditSchema);

export default AuditModel;
