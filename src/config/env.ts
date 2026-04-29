import "dotenv/config";

type NodeEnvironment = "development" | "test" | "production";
type LogLevel = "fatal" | "error" | "warn" | "info" | "debug" | "trace";
type RepoMode = "local" | "github";
type SandboxMode = "off" | "non-main" | "all";

function readString(
  key: string,
  options?: {
    defaultValue?: string;
    required?: boolean;
    allowEmpty?: boolean;
  },
): string {
  const rawValue = process.env[key];

  if (rawValue === undefined || rawValue === null) {
    if (options?.defaultValue !== undefined) {
      return options.defaultValue;
    }

    if (options?.required) {
      throw new Error(`Missing required environment variable: ${key}`);
    }

    return "";
  }

  const value = rawValue.trim();

  if (!options?.allowEmpty && value.length === 0) {
    if (options?.defaultValue !== undefined) {
      return options.defaultValue;
    }

    if (options?.required) {
      throw new Error(`Environment variable "${key}" cannot be empty`);
    }

    return "";
  }

  return value;
}

function readNumber(
  key: string,
  options?: {
    defaultValue?: number;
    required?: boolean;
    min?: number;
    max?: number;
  },
): number {
  const rawValue = process.env[key];

  if (rawValue === undefined || rawValue === null || rawValue.trim() === "") {
    if (options?.defaultValue !== undefined) {
      return options.defaultValue;
    }

    if (options?.required) {
      throw new Error(`Missing required environment variable: ${key}`);
    }

    throw new Error(`Environment variable "${key}" is not set`);
  }

  const value = Number(rawValue);

  if (Number.isNaN(value)) {
    throw new Error(`Environment variable "${key}" must be a valid number`);
  }

  if (options?.min !== undefined && value < options.min) {
    throw new Error(`Environment variable "${key}" must be >= ${options.min}`);
  }

  if (options?.max !== undefined && value > options.max) {
    throw new Error(`Environment variable "${key}" must be <= ${options.max}`);
  }

  return value;
}

function readBoolean(
  key: string,
  options?: {
    defaultValue?: boolean;
  },
): boolean {
  const rawValue = process.env[key];

  if (rawValue === undefined || rawValue === null || rawValue.trim() === "") {
    return options?.defaultValue ?? false;
  }

  const normalized = rawValue.trim().toLowerCase();

  if (["true", "1", "yes", "y", "on"].includes(normalized)) {
    return true;
  }

  if (["false", "0", "no", "n", "off"].includes(normalized)) {
    return false;
  }

  throw new Error(
    `Environment variable "${key}" must be a boolean (true/false, 1/0, yes/no)`,
  );
}

function readEnum<T extends string>(
  key: string,
  allowedValues: readonly T[],
  options?: {
    defaultValue?: T;
    required?: boolean;
  },
): T {
  const rawValue = process.env[key];

  if (rawValue === undefined || rawValue === null || rawValue.trim() === "") {
    if (options?.defaultValue !== undefined) {
      return options.defaultValue;
    }

    if (options?.required) {
      throw new Error(`Missing required environment variable: ${key}`);
    }

    throw new Error(`Environment variable "${key}" is not set`);
  }

  const value = rawValue.trim() as T;

  if (!allowedValues.includes(value)) {
    throw new Error(
      `Environment variable "${key}" must be one of: ${allowedValues.join(", ")}`,
    );
  }

  return value;
}

const nodeEnv = readEnum<NodeEnvironment>(
  "NODE_ENV",
  ["development", "test", "production"],
  { defaultValue: "development" },
);

const isProduction = nodeEnv === "production";
const isDevelopment = nodeEnv === "development";
const isTest = nodeEnv === "test";

const openclawToken =
  readString("OPENCLAW_TOKEN", {
    defaultValue: "",
    allowEmpty: true,
  }) ||
  readString("OPENCLAW_API_KEY", {
    defaultValue: "",
    allowEmpty: true,
  });

export const env = {
  app: {
    name: readString("APP_NAME", {
      defaultValue: "app-factory-orchestrator",
    }),
    version: readString("APP_VERSION", {
      defaultValue: "0.1.0",
    }),
    nodeEnv,
    isProduction,
    isDevelopment,
    isTest,
  },

  server: {
    host: readString("HOST", {
      defaultValue: "0.0.0.0",
    }),
    port: readNumber("PORT", {
      defaultValue: 3654,
      min: 1,
      max: 65535,
    }),
    corsOrigin: readString("CORS_ORIGIN", {
      defaultValue: "*",
      allowEmpty: false,
    }),
  },

  database: {
    mongoUri: readString("MONGO_URI", {
      required: true,
    }),
    mongoDbName: readString("MONGO_DB_NAME", {
      defaultValue: "app_factory_orchestrator",
    }),
  },

  openclaw: {
    baseUrl: readString("OPENCLAW_BASE_URL", {
      defaultValue: "http://127.0.0.1:18789",
    }),
    apiKey: openclawToken,
    token: openclawToken,
    timeoutMs: readNumber("OPENCLAW_TIMEOUT_MS", {
      defaultValue: 2000_000,
      min: 1_000,
    }),
    defaultModel: readString("OPENCLAW_DEFAULT_MODEL", {
      defaultValue: "openclaw/default",
    }),
    orchestratorAgentId: readString("OPENCLAW_ORCHESTRATOR_AGENT_ID", {
      defaultValue: "main",
    }),
    specAgentId: readString("OPENCLAW_SPEC_AGENT_ID", {
      defaultValue: "spec",
    }),
    architectureAgentId: readString("OPENCLAW_ARCHITECTURE_AGENT_ID", {
      defaultValue: "",
      allowEmpty: true,
    }),
    implementationAgentId: readString("OPENCLAW_IMPLEMENTATION_AGENT_ID", {
      defaultValue: "implementer",
    }),
    securityAgentId: readString("OPENCLAW_SECURITY_AGENT_ID", {
      defaultValue: "",
      allowEmpty: true,
    }),
    infraAgentId: readString("OPENCLAW_INFRA_AGENT_ID", {
      defaultValue: "",
      allowEmpty: true,
    }),
    defaultSandboxMode: readEnum<SandboxMode>(
      "OPENCLAW_DEFAULT_SANDBOX_MODE",
      ["off", "non-main", "all"],
      { defaultValue: "off" },
    ),
  },

  execution: {
    workspaceRoot: readString("WORKSPACE_ROOT", {
      defaultValue: "./data/workspaces",
    }),
    repoRoot: readString("REPO_ROOT", {
      defaultValue: "./data/repos",
    }),
    artifactsRoot: readString("ARTIFACTS_ROOT", {
      defaultValue: "./data/artifacts",
    }),
    defaultRepoMode: readEnum<RepoMode>(
      "DEFAULT_REPO_MODE",
      ["local", "github"],
      { defaultValue: "local" },
    ),
    dockerNetwork: readString("DOCKER_NETWORK", {
      defaultValue: "app-factory-staging",
    }),
    dockerBuildTimeoutMs: readNumber("DOCKER_BUILD_TIMEOUT_MS", {
      defaultValue: 300_000,
      min: 10_000,
    }),
    dockerRunTimeoutMs: readNumber("DOCKER_RUN_TIMEOUT_MS", {
      defaultValue: 300_000,
      min: 10_000,
    }),
    allowHostDockerSocket: readBoolean("ALLOW_HOST_DOCKER_SOCKET", {
      defaultValue: false,
    }),
  },

  staging: {
    baseDomain: readString("STAGING_BASE_DOMAIN", {
      defaultValue: "localhost",
    }),
    publicBaseUrl: readString("STAGING_PUBLIC_BASE_URL", {
      defaultValue: "http://localhost",
    }),
    healthcheckTimeoutMs: readNumber("STAGING_HEALTHCHECK_TIMEOUT_MS", {
      defaultValue: 30_000,
      min: 1_000,
    }),
  },

  policy: {
    autoApproveSafeActions: readBoolean("AUTO_APPROVE_SAFE_ACTIONS", {
      defaultValue: true,
    }),
    requireApprovalForDeploy: readBoolean("REQUIRE_APPROVAL_FOR_DEPLOY", {
      defaultValue: true,
    }),
    requireApprovalForDestructiveActions: readBoolean(
      "REQUIRE_APPROVAL_FOR_DESTRUCTIVE_ACTIONS",
      { defaultValue: true },
    ),
    maxJobRetries: readNumber("MAX_JOB_RETRIES", {
      defaultValue: 2,
      min: 0,
      max: 10,
    }),
    maxTaskRetries: readNumber("MAX_TASK_RETRIES", {
      defaultValue: 2,
      min: 0,
      max: 10,
    }),
  },

  logging: {
    level: readEnum<LogLevel>(
      "LOG_LEVEL",
      ["fatal", "error", "warn", "info", "debug", "trace"],
      {
        defaultValue: isProduction ? "info" : "debug",
      },
    ),
    pretty: readBoolean("LOG_PRETTY", {
      defaultValue: !isProduction,
    }),
  },
} as const;

export type Env = typeof env;
