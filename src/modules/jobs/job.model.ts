import {
  Schema,
  Types,
  model,
  type HydratedDocument,
  type InferSchemaType,
} from "mongoose";

const jobStackSchema = new Schema(
  {
    frontend: {
      type: String,
      enum: ["react"],
      required: true,
      trim: true,
    },
    backend: {
      type: String,
      enum: ["node"],
      required: true,
      trim: true,
    },
    database: {
      type: String,
      enum: ["mongodb"],
      required: true,
      trim: true,
    },
  },
  {
    _id: false,
  },
);

const jobDeploymentSchema = new Schema(
  {
    target: {
      type: String,
      enum: ["docker"],
      required: true,
      trim: true,
    },
    environment: {
      type: String,
      enum: ["staging"],
      required: true,
      trim: true,
    },
  },
  {
    _id: false,
  },
);

const jobMetadataSchema = new Schema(
  {
    requestedBy: {
      type: String,
      trim: true,
      default: undefined,
    },
    appType: {
      type: String,
      enum: ["internal-crud"],
      required: true,
      default: "internal-crud",
    },
    stack: {
      type: jobStackSchema,
      required: true,
    },
    deployment: {
      type: jobDeploymentSchema,
      required: true,
    },
    requestType: {
      type: String,
      enum: ["fix", "feature", "patch", "cleanup"],
      default: undefined,
      trim: true,
    },
    isProjectUpdate: {
      type: Boolean,
      default: undefined,
    },
    sourceJobId: {
      type: Types.ObjectId,
      ref: "Job",
      default: undefined,
    },
    previousSuccessfulJobId: {
      type: Types.ObjectId,
      ref: "Job",
      default: undefined,
    },
    canonicalProjectRoot: {
      type: String,
      trim: true,
      default: undefined,
    },
  },
  {
    _id: false,
  },
);

const jobSchema = new Schema(
  {
    projectId: {
      type: Types.ObjectId,
      ref: "Project",
      required: true,
      index: true,
    },

    type: {
      type: String,
      enum: ["create-app", "update-app", "deploy"],
      required: true,
      index: true,
    },

    state: {
      type: String,
      enum: [
        "INTAKE",
        "SPEC_READY",
        "ARCH_READY",
        "SCAFFOLD_READY",
        "CODE_READY",
        "TEST_READY",
        "SECURITY_READY",
        "STAGING_READY",
        "DEPLOYED",
        "FAILED",
        "PLAN_READY",
        "COMPLETED",
      ],
      required: true,
      index: true,
    },

    prompt: {
      type: String,
      required: true,
      trim: true,
      minlength: 1,
      maxlength: 10000,
    },

    metadata: {
      type: jobMetadataSchema,
      required: true,
    },

    currentTaskId: {
      type: Types.ObjectId,
      ref: "Task",
      default: undefined,
      index: true,
    },

    error: {
      type: String,
      trim: true,
      default: undefined,
      maxlength: 5000,
    },
  },
  {
    timestamps: true,
    versionKey: false,
    collection: "jobs",
  },
);

jobSchema.index({ projectId: 1, createdAt: -1 });
jobSchema.index({ state: 1, updatedAt: 1 });
jobSchema.index({ type: 1, state: 1 });
jobSchema.index({ currentTaskId: 1 }, { sparse: true });

export type JobDocument = HydratedDocument<InferSchemaType<typeof jobSchema>>;
export type JobModelType = InferSchemaType<typeof jobSchema>;

export const JobModel = model("Job", jobSchema);

export default JobModel;
