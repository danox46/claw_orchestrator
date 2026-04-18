import {
  Schema,
  Types,
  model,
  type HydratedDocument,
  type InferSchemaType,
} from "mongoose";

export const milestoneSchema = new Schema(
  {
    projectId: {
      type: Types.ObjectId,
      ref: "Project",
      required: true,
      index: true,
    },
    title: {
      type: String,
      required: true,
      trim: true,
    },
    description: {
      type: String,
      default: null,
    },
    order: {
      type: Number,
      required: true,
      min: 0,
    },
    status: {
      type: String,
      enum: [
        "draft",
        "planned",
        "ready",
        "in_progress",
        "blocked",
        "review",
        "completed",
        "cancelled",
      ],
      default: "draft",
      index: true,
    },
    dependsOnMilestoneId: {
      type: Types.ObjectId,
      ref: "Milestone",
      default: null,
    },
    goal: {
      type: String,
      default: null,
    },
    scope: {
      type: [String],
      default: [],
    },
    acceptanceCriteria: {
      type: [String],
      default: [],
    },
    startedAt: {
      type: Date,
      default: null,
    },
    completedAt: {
      type: Date,
      default: null,
    },
    confirmedAt: {
      type: Date,
      default: null,
    },
  },
  {
    timestamps: true,
  },
);

milestoneSchema.index({ projectId: 1, order: 1 }, { unique: true });

export type MilestoneDocument = HydratedDocument<
  InferSchemaType<typeof milestoneSchema>
>;
export type MilestoneModelType = InferSchemaType<typeof milestoneSchema>;

export const MilestoneModel = model("Milestone", milestoneSchema);

export default MilestoneModel;
