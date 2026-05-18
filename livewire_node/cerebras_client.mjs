#!/usr/bin/env node

import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

export const DEFAULT_CEREBRAS_BASE_URL = "https://api.cerebras.ai/v1";
export const DEFAULT_CEREBRAS_MODEL = "gpt-oss-120b";
export const DEFAULT_REASONING_EFFORT = "low";

const INCIDENT_REPORT_SCHEMA = {
  type: "object",
  properties: {
    summary: { type: "string" },
    probableCause: { type: "string" },
    proposedSolution: { type: "string" },
    nextSteps: {
      type: "array",
      items: { type: "string" },
    },
  },
  required: ["summary", "probableCause", "proposedSolution", "nextSteps"],
  additionalProperties: false,
};

function escapeRegex(value) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function stripWrappingQuotes(value) {
  if (
    (value.startsWith('"') && value.endsWith('"')) ||
    (value.startsWith("'") && value.endsWith("'"))
  ) {
    return value.slice(1, -1);
  }
  return value;
}

export function extractExportedValue(content, key) {
  const pattern = new RegExp(
    `^\\s*(?:export\\s+)?${escapeRegex(key)}=(.+)\\s*$`,
    "m",
  );
  const match = content.match(pattern);
  if (!match) {
    return null;
  }
  return stripWrappingQuotes(match[1].trim());
}

export async function resolveCerebrasConfig(env = process.env, deps = {}) {
  const readFile = deps.readFile || fs.readFile;
  const homedir = deps.homedir || os.homedir;
  const zshrcPath =
    env.MDW_CEREBRAS_ZSHRC_PATH || path.join(homedir(), ".zshrc");
  const directFreeKey = env.CEREBRAS_API_KEY_FREE;
  const directKey = env.CEREBRAS_API_KEY;

  let zshrcContent = "";
  if (!directFreeKey && !directKey) {
    try {
      zshrcContent = await readFile(zshrcPath, "utf8");
    } catch {
      zshrcContent = "";
    }
  }

  const apiKey =
    directFreeKey ||
    extractExportedValue(zshrcContent, "CEREBRAS_API_KEY_FREE") ||
    directKey ||
    extractExportedValue(zshrcContent, "CEREBRAS_API_KEY");

  if (!apiKey) {
    throw new Error(
      "Missing Cerebras API key. Set CEREBRAS_API_KEY_FREE, CEREBRAS_API_KEY, or export one in ~/.zshrc.",
    );
  }

  return {
    apiKey,
    baseUrl: env.MDW_CEREBRAS_BASE_URL || DEFAULT_CEREBRAS_BASE_URL,
    model: env.MDW_CEREBRAS_MODEL || DEFAULT_CEREBRAS_MODEL,
    reasoningEffort:
      env.MDW_CEREBRAS_REASONING_EFFORT || DEFAULT_REASONING_EFFORT,
    versionPatch: env.MDW_CEREBRAS_VERSION_PATCH || undefined,
  };
}

export function buildIncidentPrompt({
  jobName,
  runDate,
  attempts,
  exitCode,
  repoRoot,
  logFile,
  errorSummary,
  logTail,
}) {
  return [
    "Analyze this scheduled market-data sync failure for a human operator.",
    "Be concise, factual, and avoid inventing unavailable facts.",
    "Return a probable cause only when it is well-supported by the log evidence.",
    "",
    `Job name: ${jobName || "daily_update"}`,
    `Run date: ${runDate || "(unknown)"}`,
    `Attempts: ${attempts ?? "(unknown)"}`,
    `Exit code: ${exitCode ?? "(unknown)"}`,
    `Repository: ${repoRoot || process.cwd()}`,
    `Log file: ${logFile || "(not provided)"}`,
    `Raw error summary: ${errorSummary || "(not provided)"}`,
    "",
    "Recent log tail:",
    logTail || "(no log tail available)",
  ].join("\n");
}

export async function generateHumanReadableIncidentReport(
  incidentContext,
  env = process.env,
  deps = {},
) {
  const fetchImpl = deps.fetch || globalThis.fetch;
  if (typeof fetchImpl !== "function") {
    throw new Error("Global fetch is unavailable in this Node runtime.");
  }

  const config = await resolveCerebrasConfig(env, deps);
  const headers = {
    Authorization: `Bearer ${config.apiKey}`,
    "Content-Type": "application/json",
  };
  if (config.versionPatch) {
    headers["X-Cerebras-Version-Patch"] = config.versionPatch;
  }

  const response = await fetchImpl(
    `${config.baseUrl.replace(/\/$/, "")}/chat/completions`,
    {
      method: "POST",
      headers,
      body: JSON.stringify({
        model: config.model,
        reasoning_effort: config.reasoningEffort,
        messages: [
          {
            role: "system",
            content:
              "You are an SRE assistant writing incident summaries for a scheduled market data sync failure.",
          },
          {
            role: "user",
            content: buildIncidentPrompt(incidentContext),
          },
        ],
        response_format: {
          type: "json_schema",
          json_schema: {
            name: "daily_update_incident_report",
            strict: true,
            description:
              "A concise human-readable failure summary and proposed remediation for a daily market data sync failure.",
            schema: INCIDENT_REPORT_SCHEMA,
          },
        },
      }),
    },
  );

  if (!response.ok) {
    const errorBody = await response.text();
    throw new Error(
      `Cerebras request failed with ${response.status}: ${errorBody || response.statusText}`,
    );
  }

  const payload = await response.json();
  const content = payload?.choices?.[0]?.message?.content;
  if (!content) {
    throw new Error("Cerebras response did not include message content.");
  }

  return {
    ...JSON.parse(content),
    model: payload.model || config.model,
  };
}
