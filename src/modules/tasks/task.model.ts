import {
  Schema,
  Types,
  model,
  type HydratedDocument,
  type InferSchemaType,
} from "mongoose";

const issuerSchema = new Schema(
  {
    kind: {
      type: String,
      enum: ["system", "agent"],
      required: true,
    },
    id: {
      type: String,
      required: true,
      trim: true,
      minlength: 1,
      maxlength: 120,
    },
    sessionId: {
      type: String,
      trim: true,
      default: undefined,
      maxlength: 255,
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

const targetSchema = new Schema(
  {
    agentId: {
      type: String,
      required: true,
      trim: true,
      minlength: 1,
      maxlength: 120,
      index: true,
    },
  },
  {
    _id: false,
  },
);

const constraintsSchema = new Schema(
  {
    toolProfile: {
      type: String,
      required: true,
      trim: true,
      minlength: 1,
      maxlength: 120,
    },
    sandbox: {
      type: String,
      enum: ["off", "non-main", "all"],
      required: true,
      default: "non-main",
    },
    maxTokens: {
      type: Number,
      default: undefined,
      min: 1,
    },
    maxCost: {
      type: Number,
      default: undefined,
      min: 0,
    },
  },
  {
    _id: false,
  },
);

const taskSchema = new Schema(
  {
    jobId: {
      type: Types.ObjectId,
      ref: "Job",
      required: true,
      index: true,
    },

    projectId: {
      type: Types.ObjectId,
      ref: "Project",
      required: true,
      index: true,
    },

    milestoneId: {
      type: Types.ObjectId,
      ref: "Milestone",
      required: true,
      index: true,
    },

    parentTaskId: {
      type: Types.ObjectId,
      ref: "Task",
      default: undefined,
      index: true,
    },

    dependencies: {
      type: [
        {
          type: Types.ObjectId,
          ref: "Task",
        },
      ],
      default: [],
    },

    issuer: {
      type: issuerSchema,
      required: true,
    },

    target: {
      type: targetSchema,
      required: true,
    },

    intent: {
      type: String,
      enum: [
        "draft_spec",
        "design_architecture",
        "generate_scaffold",
        "implement_feature",
        "run_tests",
        "review_security",
        "prepare_staging",
        "plan_project_phases",
        "plan_phase_tasks",
        "plan_next_tasks",
        "review_milestone",
      ],
      required: true,
      index: true,
    },

    /**
     * Flexible task payloads.
     * These will be validated at the service/schema layer.
     */
    inputs: {
      type: Schema.Types.Mixed,
      required: true,
      default: {},
    },

    constraints: {
      type: constraintsSchema,
      required: true,
    },

    requiredArtifacts: {
      type: [String],
      required: true,
      default: [],
    },

    acceptanceCriteria: {
      type: [String],
      required: true,
      default: [],
    },

    idempotencyKey: {
      type: String,
      required: true,
      trim: true,
      minlength: 1,
      maxlength: 255,
      unique: true,
      index: true,
    },

    status: {
      type: String,
      enum: ["queued", "running", "qa", "succeeded", "failed", "canceled"],
      required: true,
      default: "queued",
      index: true,
    },

    /**
     * Retry and scheduling fields.
     * - attemptCount starts at 0 before the first dispatch
     * - maxAttempts is the total allowed tries within the current session
     * - sessionName identifies the current agent session, when applicable
     * - sessionCount tracks how many sessions have been used for this task
     * - maxSessions is the total allowed fresh-session escalations
     * - nextRetryAt gates when a queued task becomes runnable again
     * - retryable allows us to opt out for non-retryable tasks later
     * - sequence lets us keep milestone planning/execution ordered
     */
    attemptCount: {
      type: Number,
      required: true,
      default: 0,
      min: 0,
    },

    maxAttempts: {
      type: Number,
      required: true,
      default: 3,
      min: 1,
    },

    sessionName: {
      type: String,
      trim: true,
      default: undefined,
      maxlength: 255,
      index: true,
    },

    sessionCount: {
      type: Number,
      required: true,
      default: 1,
      min: 1,
    },

    maxSessions: {
      type: Number,
      required: true,
      default: 2,
      min: 1,
    },

    nextRetryAt: {
      type: Date,
      default: undefined,
      index: true,
    },

    lastError: {
      type: String,
      trim: true,
      default: undefined,
      maxlength: 4000,
    },

    retryable: {
      type: Boolean,
      required: true,
      default: true,
    },

    sequence: {
      type: Number,
      required: true,
      default: 0,
      min: 0,
      index: true,
    },

    outputs: {
      type: Schema.Types.Mixed,
      default: undefined,
    },

    artifacts: {
      type: [String],
      default: [],
    },

    errors: {
      type: [String],
      default: [],
    },
  },
  {
    timestamps: true,
    versionKey: false,
    collection: "tasks",
  },
);

taskSchema.index({ jobId: 1, createdAt: -1 });
taskSchema.index({ projectId: 1, createdAt: -1 });
taskSchema.index({ projectId: 1, milestoneId: 1, createdAt: -1 });
taskSchema.index({ milestoneId: 1, status: 1, sequence: 1 });
taskSchema.index({ status: 1, nextRetryAt: 1, updatedAt: 1 });
taskSchema.index({ "target.agentId": 1, status: 1, nextRetryAt: 1 });
taskSchema.index({ status: 1, sessionCount: 1, updatedAt: 1 });
taskSchema.index({ sessionName: 1 }, { sparse: true });
taskSchema.index({ parentTaskId: 1 }, { sparse: true });

/**
 * Main runnable-task query index:
 * - same job
 * - same milestone
 * - queued tasks
 * - sequence order first
 * - then retry window
 * - then creation order
 */
taskSchema.index({
  jobId: 1,
  milestoneId: 1,
  status: 1,
  sequence: 1,
  nextRetryAt: 1,
  createdAt: 1,
});

export type TaskDocument = HydratedDocument<InferSchemaType<typeof taskSchema>>;
export type TaskModelType = InferSchemaType<typeof taskSchema>;

export const TaskModel = model("Task", taskSchema);

export default TaskModel;
