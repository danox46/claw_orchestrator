import {
  Schema,
  model,
  type InferSchemaType,
  type HydratedDocument,
} from "mongoose";

const projectStackSchema = new Schema(
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

const projectSchema = new Schema(
  {
    name: {
      type: String,
      required: true,
      trim: true,
      minlength: 3,
      maxlength: 120,
    },

    slug: {
      type: String,
      required: true,
      trim: true,
      lowercase: true,
      unique: true,
      index: true,
      minlength: 3,
      maxlength: 160,
    },

    appType: {
      type: String,
      enum: ["internal-crud"],
      required: true,
      default: "internal-crud",
      index: true,
    },

    stack: {
      type: projectStackSchema,
      required: true,
    },

    repoMode: {
      type: String,
      enum: ["local", "github"],
      required: true,
      default: "local",
    },

    repoUrl: {
      type: String,
      trim: true,
      default: undefined,
    },

    canonicalProjectRoot: {
      type: String,
      trim: true,
      default: undefined,
    },

    status: {
      type: String,
      enum: ["active", "archived", "ready_for_review"],
      required: true,
      default: "active",
      index: true,
    },
  },
  {
    timestamps: true,
    versionKey: false,
    collection: "projects",
  },
);

projectSchema.index({ createdAt: -1 });
projectSchema.index({ updatedAt: -1 });
projectSchema.index({ appType: 1, status: 1 });

export type ProjectDocument = HydratedDocument<
  InferSchemaType<typeof projectSchema>
>;
export type ProjectModelType = InferSchemaType<typeof projectSchema>;

export const ProjectModel = model("Project", projectSchema);

export default ProjectModel;
