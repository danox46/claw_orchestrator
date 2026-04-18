import { createLogger } from "../../config/logger";

const logger = createLogger({
  module: "templates",
  component: "template-registry",
});

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

export type TemplateFile = {
  path: string;
  content: string;
};

export type TemplateDefinition = {
  name: string;
  description: string;
  stack: {
    frontend: "react";
    backend: "node";
    database: "mongodb";
  };
  files: TemplateFile[];
};

export class TemplateRegistry {
  private readonly templates = new Map<string, TemplateDefinition>();

  constructor() {
    const baseTemplate = this.buildReactNodeMongoCrudTemplate();
    this.templates.set(baseTemplate.name, baseTemplate);
  }

  async getTemplate(name: string): Promise<TemplateDefinition> {
    const template = this.templates.get(name.trim());

    if (!template) {
      throw createServiceError({
        message: `Template not found: ${name}`,
        code: "TEMPLATE_NOT_FOUND",
        statusCode: 404,
        details: {
          name,
          availableTemplates: Array.from(this.templates.keys()),
        },
      });
    }

    logger.info(
      {
        templateName: template.name,
      },
      "Template loaded.",
    );

    return {
      ...template,
      files: template.files.map((file) => ({
        path: file.path,
        content: file.content,
      })),
    };
  }

  async listTemplates(): Promise<
    Array<{
      name: string;
      description: string;
      stack: TemplateDefinition["stack"];
      fileCount: number;
    }>
  > {
    return Array.from(this.templates.values()).map((template) => ({
      name: template.name,
      description: template.description,
      stack: template.stack,
      fileCount: template.files.length,
    }));
  }

  registerTemplate(template: TemplateDefinition): void {
    if (!template.name.trim()) {
      throw createServiceError({
        message: "Template name cannot be empty.",
        code: "INVALID_TEMPLATE_NAME",
        statusCode: 400,
      });
    }

    this.templates.set(template.name.trim(), {
      ...template,
      name: template.name.trim(),
      description: template.description.trim(),
      files: template.files.map((file) => ({
        path: file.path,
        content: file.content,
      })),
    });

    logger.info(
      {
        templateName: template.name,
      },
      "Template registered.",
    );
  }

  private buildReactNodeMongoCrudTemplate(): TemplateDefinition {
    return {
      name: "react-node-mongo-crud",
      description:
        "Base internal CRUD app with React frontend, Node/Express backend, MongoDB, and Docker staging support.",
      stack: {
        frontend: "react",
        backend: "node",
        database: "mongodb",
      },
      files: [
        {
          path: ".gitignore",
          content: `node_modules
dist
build
coverage
.env
.env.local
npm-debug.log*
yarn-debug.log*
yarn-error.log*
pnpm-debug.log*
`,
        },
        {
          path: "README.md",
          content: `# React + Node + Mongo CRUD

Generated base template for the App Factory Orchestrator.

## Stack

- React + Vite
- Node + Express
- MongoDB
- Docker Compose for staging

## Development

### Install root dependencies
\`\`\`bash
npm install
\`\`\`

### Install workspace dependencies
\`\`\`bash
npm run install:all
\`\`\`

### Run in development
\`\`\`bash
npm run dev
\`\`\`

### Build
\`\`\`bash
npm run build
\`\`\`

## Environment

Copy \`.env.example\` to \`.env\` and adjust values as needed.
`,
        },
        {
          path: ".env.example",
          content: `PORT=4000
MONGO_URI=mongodb://mongo:27017/app_factory_app
JWT_SECRET=change-me
VITE_API_BASE_URL=http://localhost:4000
`,
        },
        {
          path: "package.json",
          content: `{
  "name": "react-node-mongo-crud",
  "private": true,
  "version": "0.1.0",
  "workspaces": [
    "apps/web",
    "apps/api"
  ],
  "scripts": {
    "install:all": "npm install --workspaces",
    "dev": "concurrently \\"npm run dev -w apps/api\\" \\"npm run dev -w apps/web\\"",
    "build": "npm run build -w apps/api && npm run build -w apps/web",
    "lint": "npm run lint -w apps/api && npm run lint -w apps/web",
    "test": "npm run test -w apps/api && npm run test -w apps/web"
  },
  "devDependencies": {
    "concurrently": "^9.0.1"
  }
}
`,
        },
        {
          path: "docker-compose.staging.yml",
          content: `services:
  mongo:
    image: mongo:7
    container_name: app-factory-mongo
    restart: unless-stopped
    ports:
      - "27017:27017"
    volumes:
      - mongo_data:/data/db

  api:
    build:
      context: .
      dockerfile: apps/api/Dockerfile
    container_name: app-factory-api
    env_file:
      - .env
    depends_on:
      - mongo
    ports:
      - "4000:4000"

  web:
    build:
      context: .
      dockerfile: apps/web/Dockerfile
    container_name: app-factory-web
    env_file:
      - .env
    depends_on:
      - api
    ports:
      - "3000:3000"

volumes:
  mongo_data:
`,
        },
        {
          path: "apps/api/package.json",
          content: `{
  "name": "@app-factory/api",
  "version": "0.1.0",
  "private": true,
  "type": "module",
  "main": "dist/server.js",
  "scripts": {
    "dev": "node --watch src/server.js",
    "build": "mkdir -p dist && cp -R src/* dist/",
    "lint": "node -e \\"console.log('lint placeholder: api')\\"",
    "test": "node -e \\"console.log('test placeholder: api')\\""
  },
  "dependencies": {
    "cors": "^2.8.5",
    "dotenv": "^16.4.5",
    "express": "^4.21.0",
    "mongoose": "^8.7.1"
  }
}
`,
        },
        {
          path: "apps/api/Dockerfile",
          content: `FROM node:20-alpine

WORKDIR /app

COPY apps/api/package*.json ./
RUN npm install

COPY apps/api ./

EXPOSE 4000

CMD ["npm", "run", "dev"]
`,
        },
        {
          path: "apps/api/src/server.js",
          content: `import "dotenv/config";
import express from "express";
import cors from "cors";
import mongoose from "mongoose";

const app = express();
const port = Number(process.env.PORT || 4000);

app.use(cors());
app.use(express.json());

app.get("/health", (_req, res) => {
  res.status(200).json({
    ok: true,
    service: "api",
    status: "healthy"
  });
});

app.get("/ready", (_req, res) => {
  res.status(200).json({
    ok: true,
    service: "api",
    status: "ready"
  });
});

app.get("/api/users", async (_req, res) => {
  res.status(200).json({
    ok: true,
    data: []
  });
});

async function bootstrap() {
  const mongoUri = process.env.MONGO_URI || "mongodb://localhost:27017/app_factory_app";
  await mongoose.connect(mongoUri);

  app.listen(port, "0.0.0.0", () => {
    console.log(\`API listening on port \${port}\`);
  });
}

bootstrap().catch((error) => {
  console.error("API bootstrap failed", error);
  process.exit(1);
});
`,
        },
        {
          path: "apps/web/package.json",
          content: `{
  "name": "@app-factory/web",
  "version": "0.1.0",
  "private": true,
  "type": "module",
  "scripts": {
    "dev": "vite --host 0.0.0.0 --port 3000",
    "build": "vite build",
    "lint": "node -e \\"console.log('lint placeholder: web')\\"",
    "test": "node -e \\"console.log('test placeholder: web')\\""
  },
  "dependencies": {
    "react": "^18.3.1",
    "react-dom": "^18.3.1"
  },
  "devDependencies": {
    "@vitejs/plugin-react": "^4.3.1",
    "vite": "^5.4.8"
  }
}
`,
        },
        {
          path: "apps/web/Dockerfile",
          content: `FROM node:20-alpine

WORKDIR /app

COPY apps/web/package*.json ./
RUN npm install

COPY apps/web ./

EXPOSE 3000

CMD ["npm", "run", "dev"]
`,
        },
        {
          path: "apps/web/index.html",
          content: `<!doctype html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>Internal CRUD App</title>
    <script type="module" src="/src/main.jsx"></script>
  </head>
  <body>
    <div id="root"></div>
  </body>
</html>
`,
        },
        {
          path: "apps/web/src/main.jsx",
          content: `import React from "react";
import ReactDOM from "react-dom/client";
import App from "./App";

ReactDOM.createRoot(document.getElementById("root")).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>
);
`,
        },
        {
          path: "apps/web/src/App.jsx",
          content: `import React from "react";

export default function App() {
  return (
    <main style={{ fontFamily: "Arial, sans-serif", padding: 24 }}>
      <h1>Internal CRUD App</h1>
      <p>Base scaffold created by the App Factory Orchestrator.</p>

      <section style={{ marginTop: 24 }}>
        <h2>Users</h2>
        <p>This table and form will be customized by the implementation agent.</p>
      </section>
    </main>
  );
}
`,
        },
        {
          path: "apps/web/vite.config.js",
          content: `import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()]
});
`,
        },
      ],
    };
  }
}

export default TemplateRegistry;
