import test from "node:test";
import assert from "node:assert/strict";

import {
  DEFAULT_CEREBRAS_BASE_URL,
  DEFAULT_CEREBRAS_MODEL,
  DEFAULT_REASONING_EFFORT,
  buildIncidentPrompt,
  extractExportedValue,
  generateHumanReadableIncidentReport,
  resolveCerebrasConfig,
} from "../../livewire_node/cerebras_client.mjs";

test("extractExportedValue reads exported and quoted values", () => {
  const content = [
    "export CEREBRAS_API_KEY_FREE='free-key'",
    'export CEREBRAS_API_KEY="paid-key"',
  ].join("\n");

  assert.equal(extractExportedValue(content, "CEREBRAS_API_KEY_FREE"), "free-key");
  assert.equal(extractExportedValue(content, "CEREBRAS_API_KEY"), "paid-key");
  assert.equal(extractExportedValue(content, "MISSING_KEY"), null);
});

test("resolveCerebrasConfig prefers explicit free env key and defaults", async () => {
  const config = await resolveCerebrasConfig({
    CEREBRAS_API_KEY_FREE: "free-key",
  });

  assert.deepEqual(config, {
    apiKey: "free-key",
    baseUrl: DEFAULT_CEREBRAS_BASE_URL,
    model: DEFAULT_CEREBRAS_MODEL,
    reasoningEffort: DEFAULT_REASONING_EFFORT,
    versionPatch: undefined,
  });
});

test("resolveCerebrasConfig falls back to ~/.zshrc and supports overrides", async () => {
  const config = await resolveCerebrasConfig(
    {
      MDW_CEREBRAS_MODEL: "gpt-oss-120b",
      MDW_CEREBRAS_BASE_URL: "https://example.test/v1",
      MDW_CEREBRAS_REASONING_EFFORT: "medium",
      MDW_CEREBRAS_VERSION_PATCH: "2",
      MDW_CEREBRAS_ZSHRC_PATH: "/tmp/custom.zshrc",
    },
    {
      readFile: async (filePath) => {
        assert.equal(filePath, "/tmp/custom.zshrc");
        return "export CEREBRAS_API_KEY_FREE=free-from-zshrc\n";
      },
      homedir: () => "/Users/tester",
    },
  );

  assert.deepEqual(config, {
    apiKey: "free-from-zshrc",
    baseUrl: "https://example.test/v1",
    model: "gpt-oss-120b",
    reasoningEffort: "medium",
    versionPatch: "2",
  });
});

test("resolveCerebrasConfig throws when no key is available", async () => {
  await assert.rejects(
    () =>
      resolveCerebrasConfig(
        {},
        {
          readFile: async () => {
            throw new Error("missing");
          },
          homedir: () => "/Users/tester",
        },
      ),
    /Missing Cerebras API key/,
  );
});

test("buildIncidentPrompt includes job, error, and log context", () => {
  const prompt = buildIncidentPrompt({
    jobName: "daily_update",
    runDate: "2026-03-11",
    attempts: 3,
    exitCode: 9,
    repoRoot: "/repo",
    logFile: "/tmp/daily.log",
    errorSummary: "Gateway refused connection",
    logTail: "Traceback...",
  });

  assert.match(prompt, /Gateway refused connection/);
  assert.match(prompt, /Traceback\.\.\./);
  assert.match(prompt, /Attempts: 3/);
});

test("generateHumanReadableIncidentReport posts to Cerebras and parses the json-schema content", async () => {
  let receivedRequest;
  const result = await generateHumanReadableIncidentReport(
    {
      jobName: "daily_update",
      runDate: "2026-03-11",
      attempts: 3,
      exitCode: 7,
      repoRoot: "/repo",
      logFile: "/tmp/daily.log",
      errorSummary: "Timed out",
      logTail: "timeout stack",
    },
    {
      CEREBRAS_API_KEY_FREE: "free-key",
      MDW_CEREBRAS_VERSION_PATCH: "2",
    },
    {
      fetch: async (url, request) => {
        receivedRequest = { url, request };
        return {
          ok: true,
          json: async () => ({
            model: "gpt-oss-120b",
            choices: [
              {
                message: {
                  content: JSON.stringify({
                    summary: "The job timed out after retrying.",
                    probableCause: "The upstream broker stopped responding.",
                    proposedSolution: "Restore broker connectivity and rerun the sync.",
                    nextSteps: ["Check the broker.", "Retry the job."],
                  }),
                },
              },
            ],
          }),
        };
      },
    },
  );

  assert.equal(receivedRequest.url, "https://api.cerebras.ai/v1/chat/completions");
  assert.equal(receivedRequest.request.method, "POST");
  assert.equal(receivedRequest.request.headers.Authorization, "Bearer free-key");
  assert.equal(receivedRequest.request.headers["X-Cerebras-Version-Patch"], "2");

  const body = JSON.parse(receivedRequest.request.body);
  assert.equal(body.model, DEFAULT_CEREBRAS_MODEL);
  assert.equal(body.reasoning_effort, DEFAULT_REASONING_EFFORT);
  assert.equal(body.response_format.type, "json_schema");
  assert.equal(body.response_format.json_schema.strict, true);
  assert.equal(result.summary, "The job timed out after retrying.");
  assert.deepEqual(result.nextSteps, ["Check the broker.", "Retry the job."]);
});

test("generateHumanReadableIncidentReport throws on error responses and missing content", async () => {
  await assert.rejects(
    () =>
      generateHumanReadableIncidentReport(
        {
          errorSummary: "Timed out",
          logTail: "timeout stack",
        },
        {
          CEREBRAS_API_KEY: "paid-key",
        },
        {
          fetch: async () => ({
            ok: false,
            status: 401,
            statusText: "Unauthorized",
            text: async () => "bad key",
          }),
        },
      ),
    /Cerebras request failed with 401: bad key/,
  );

  await assert.rejects(
    () =>
      generateHumanReadableIncidentReport(
        {
          errorSummary: "Timed out",
          logTail: "timeout stack",
        },
        {
          CEREBRAS_API_KEY: "paid-key",
        },
        {
          fetch: async () => ({
            ok: true,
            json: async () => ({
              choices: [{ message: { content: null } }],
            }),
          }),
        },
      ),
    /Cerebras response did not include message content/,
  );
});
