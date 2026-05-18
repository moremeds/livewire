import { mkdtemp, readFile, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import assert from "node:assert/strict";

import {
  buildFailureMessage,
  buildFallbackIncidentReport,
  buildHumanReadableReport,
  buildHumanReadableReportPath,
  main,
  parseArgs,
  parseBoolean,
  readLogTail,
  resolveAlertConfig,
  runFailureAlert,
  sendFailureAlert,
  writeHumanReadableReport,
} from "./send_daily_update_failure_email.mjs";

const baseAlertConfig = {
  from: "from@example.com",
  to: "to@example.com",
  subjectPrefix: "[MDW]",
};

const baseIncidentReport = {
  summary: "The job failed after exhausting retries.",
  probableCause: "The upstream broker timed out.",
  proposedSolution: "Restore broker connectivity and rerun the job.",
  nextSteps: ["Check the broker service.", "Retry the sync."],
};

test("parseArgs applies defaults and reads explicit values", () => {
  const options = parseArgs([
    "--run-date",
    "2026-03-11",
    "--log-file",
    "/tmp/daily.log",
    "--attempts",
    "3",
    "--exit-code",
    "1",
    "--error-summary",
    "boom",
    "--repo-root",
    "/repo",
    "--job-name",
    "daily_update",
    "--tail-lines",
    "25",
  ]);

  assert.deepEqual(options, {
    runDate: "2026-03-11",
    logFile: "/tmp/daily.log",
    attempts: 3,
    exitCode: 1,
    errorSummary: "boom",
    repoRoot: "/repo",
    jobName: "daily_update",
    tailLines: 25,
  });
});

test("parseBoolean accepts common truthy and falsy forms", () => {
  assert.equal(parseBoolean("true"), true);
  assert.equal(parseBoolean("1"), true);
  assert.equal(parseBoolean("no"), false);
  assert.equal(parseBoolean(undefined, true), true);
});

test("resolveAlertConfig supports smtp url", () => {
  const config = resolveAlertConfig({
    MDW_ALERT_EMAIL_FROM: "from@example.com",
    MDW_ALERT_EMAIL_TO: "to@example.com",
    MDW_ALERT_SMTP_URL: "smtp://user:pass@mail.example.com:587",
  });

  assert.equal(config.from, "from@example.com");
  assert.equal(config.to, "to@example.com");
  assert.equal(config.transportOptions, "smtp://user:pass@mail.example.com:587");
});

test("resolveAlertConfig supports host port auth fields and validates required sender", () => {
  const config = resolveAlertConfig({
    MDW_ALERT_EMAIL_FROM: "from@example.com",
    MDW_ALERT_EMAIL_TO: "to@example.com",
    MDW_ALERT_SMTP_HOST: "mail.example.com",
    MDW_ALERT_SMTP_PORT: "465",
    MDW_ALERT_SMTP_SECURE: "true",
    MDW_ALERT_SMTP_USER: "user",
    MDW_ALERT_SMTP_PASS: "pass",
  });

  assert.deepEqual(config.transportOptions, {
    host: "mail.example.com",
    port: 465,
    secure: true,
    auth: {
      user: "user",
      pass: "pass",
    },
  });

  assert.throws(() => resolveAlertConfig({}), /Missing MDW_ALERT_EMAIL_FROM/);
});

test("readLogTail handles null, missing, and existing logs", async () => {
  const tmpdir = await mkdtemp(path.join(os.tmpdir(), "mdw-mailer-test-"));
  const logFile = path.join(tmpdir, "daily.log");
  await writeFile(logFile, "one\ntwo\nthree\nfour\n", "utf8");

  assert.equal(await readLogTail(undefined, 2), "(no log file path provided)");
  assert.match(await readLogTail(path.join(tmpdir, "missing.log"), 2), /unable to read/);
  assert.equal(await readLogTail(logFile, 2), "three\nfour");
});

test("build report helpers create readable output", async () => {
  const reportPath = buildHumanReadableReportPath("/tmp/daily_update_2026-03-11.log");
  assert.equal(reportPath, "/tmp/daily_update_2026-03-11.human.md");

  const fallback = buildFallbackIncidentReport({
    options: {
      logFile: "/tmp/daily.log",
      errorSummary: "Gateway timed out",
    },
    logTail: "timeout stack",
    error: new Error("Missing Cerebras key"),
  });
  assert.match(fallback.probableCause, /AI summary unavailable/);

  const reportBody = buildHumanReadableReport({
    options: {
      jobName: "daily_update",
      runDate: "2026-03-11",
      attempts: 3,
      exitCode: 7,
      repoRoot: "/repo",
      logFile: "/tmp/daily.log",
      errorSummary: "Gateway timed out",
    },
    incidentReport: baseIncidentReport,
    logTail: "Traceback...\nboom",
    hostname: "warehouse.local",
    generatedAt: "2026-03-11T20:05:48Z",
  });

  assert.match(reportBody, /## Human-readable summary/);
  assert.match(reportBody, /Restore broker connectivity/);

  const tmpdir = await mkdtemp(path.join(os.tmpdir(), "mdw-human-report-"));
  const outPath = path.join(tmpdir, "incident.md");
  await writeHumanReadableReport(outPath, reportBody);
  assert.match(await readFile(outPath, "utf8"), /warehouse\.local/);
});

test("buildFailureMessage includes rich text and html content", () => {
  const message = buildFailureMessage({
    alertConfig: {
      ...baseAlertConfig,
      replyTo: "ops@example.com",
    },
    options: {
      runDate: "2026-03-11",
      jobName: "daily_update",
      attempts: 3,
      exitCode: 1,
      repoRoot: "/repo",
      logFile: "/tmp/daily.log",
      errorSummary: "Timed out <boom>",
    },
    incidentReport: baseIncidentReport,
    logTail: "Traceback...\nboom",
    humanReportPath: "/tmp/daily.human.md",
    hostname: "warehouse.local",
    generatedAt: "2026-03-11T20:05:48Z",
  });

  assert.equal(message.subject, "[MDW] daily_update failed on 2026-03-11");
  assert.match(message.text, /Human-readable report: \/tmp\/daily\.human\.md/);
  assert.match(message.text, /Probable cause:/);
  assert.match(message.html, /Market Data Warehouse Alert/);
  assert.match(message.html, /Timed out &lt;boom&gt;/);
  assert.match(message.html, /ops@example\.com/);
});

test("sendFailureAlert can render with nodemailer stream transport", async () => {
  const info = await sendFailureAlert({
    transportOptions: { streamTransport: true, buffer: true },
    message: {
      from: "from@example.com",
      to: "to@example.com",
      subject: "subject",
      text: "body",
    },
  });

  assert.equal(info.messageId.startsWith("<"), true);
});

test("runFailureAlert wires AI summary, report writing, and send function together", async () => {
  let received;
  let reportWrite;
  const tmpdir = await mkdtemp(path.join(os.tmpdir(), "mdw-alert-flow-"));
  const logFile = path.join(tmpdir, "daily.log");

  const result = await runFailureAlert(
    [
      "--run-date",
      "2026-03-11",
      "--log-file",
      logFile,
      "--attempts",
      "3",
      "--exit-code",
      "1",
      "--error-summary",
      "Failed to connect",
      "--repo-root",
      "/repo",
    ],
    {
      MDW_ALERT_EMAIL_FROM: "from@example.com",
      MDW_ALERT_EMAIL_TO: "to@example.com",
      MDW_ALERT_SMTP_URL: "smtp://user:pass@mail.example.com:587",
    },
    {
      readLogTail: async () => "tail line 1\ntail line 2",
      generateIncidentReport: async () => baseIncidentReport,
      writeHumanReadableReport: async (reportPath, reportBody) => {
        reportWrite = { reportPath, reportBody };
        return reportPath;
      },
      sendFailureAlert: async ({ transportOptions, message }) => {
        received = { transportOptions, message };
        return { messageId: "mid-123" };
      },
      hostname: "warehouse.local",
      generatedAt: "2026-03-11T20:05:48Z",
    },
  );

  assert.equal(result.info.messageId, "mid-123");
  assert.equal(reportWrite.reportPath, path.join(tmpdir, "daily.human.md"));
  assert.match(reportWrite.reportBody, /Human-readable summary/);
  assert.equal(received.transportOptions, "smtp://user:pass@mail.example.com:587");
  assert.match(received.message.html, /Restore broker connectivity/);
  assert.equal(result.humanReportPath, path.join(tmpdir, "daily.human.md"));
});

test("runFailureAlert falls back cleanly when AI generation fails", async () => {
  let received;

  const result = await runFailureAlert(
    [
      "--run-date",
      "2026-03-11",
      "--log-file",
      "/tmp/daily.log",
      "--attempts",
      "3",
      "--exit-code",
      "1",
      "--error-summary",
      "Failed to connect",
      "--repo-root",
      "/repo",
    ],
    {
      MDW_ALERT_EMAIL_FROM: "from@example.com",
      MDW_ALERT_EMAIL_TO: "to@example.com",
      MDW_ALERT_SMTP_URL: "smtp://user:pass@mail.example.com:587",
    },
    {
      readLogTail: async () => "tail line 1\ntail line 2",
      generateIncidentReport: async () => {
        throw new Error("Cerebras unavailable");
      },
      writeHumanReadableReport: async (reportPath) => reportPath,
      sendFailureAlert: async ({ message }) => {
        received = message;
        return { messageId: "mid-fallback" };
      },
      hostname: "warehouse.local",
      generatedAt: "2026-03-11T20:05:48Z",
    },
  );

  assert.equal(result.info.messageId, "mid-fallback");
  assert.match(received.text, /AI summary unavailable: Cerebras unavailable/);
  assert.match(received.html, /AI summary unavailable: Cerebras unavailable/);
});

test("runFailureAlert survives report write failures", async () => {
  let received;

  await runFailureAlert(
    [
      "--run-date",
      "2026-03-11",
      "--log-file",
      "/tmp/daily.log",
      "--attempts",
      "3",
      "--exit-code",
      "1",
      "--error-summary",
      "Failed to connect",
      "--repo-root",
      "/repo",
    ],
    {
      MDW_ALERT_EMAIL_FROM: "from@example.com",
      MDW_ALERT_EMAIL_TO: "to@example.com",
      MDW_ALERT_SMTP_URL: "smtp://user:pass@mail.example.com:587",
    },
    {
      readLogTail: async () => "tail line 1\ntail line 2",
      generateIncidentReport: async () => baseIncidentReport,
      writeHumanReadableReport: async () => {
        throw new Error("disk full");
      },
      sendFailureAlert: async ({ message }) => {
        received = message;
        return { messageId: "mid-report-error" };
      },
      hostname: "warehouse.local",
      generatedAt: "2026-03-11T20:05:48Z",
    },
  );

  assert.match(received.text, /Human-readable report could not be written: disk full/);
});

test("main prints help and success output", async () => {
  const logs = [];
  const originalLog = console.log;
  console.log = (message) => logs.push(message);

  try {
    const helpResult = await main(["--help"], {}, {});
    assert.match(helpResult.help, /Usage: node scripts\/send_daily_update_failure_email\.mjs/);

    const successResult = await main(
      [
        "--run-date",
        "2026-03-11",
        "--log-file",
        "/tmp/daily.log",
        "--attempts",
        "3",
        "--exit-code",
        "1",
        "--error-summary",
        "Failed to connect",
        "--repo-root",
        "/repo",
      ],
      {
        MDW_ALERT_EMAIL_FROM: "from@example.com",
        MDW_ALERT_EMAIL_TO: "to@example.com",
        MDW_ALERT_SMTP_URL: "smtp://user:pass@mail.example.com:587",
      },
      {
        readLogTail: async () => "tail line 1\ntail line 2",
        generateIncidentReport: async () => baseIncidentReport,
        writeHumanReadableReport: async (reportPath) => reportPath,
        sendFailureAlert: async () => ({ messageId: "mid-123" }),
        hostname: "warehouse.local",
        generatedAt: "2026-03-11T20:05:48Z",
      },
    );

    assert.equal(successResult.info.messageId, "mid-123");
  } finally {
    console.log = originalLog;
  }

  assert.match(logs[0], /Usage: node scripts\/send_daily_update_failure_email\.mjs/);
  assert.match(logs[1], /Sent daily update failure alert: \[Market Data Warehouse\] daily_update failed on 2026-03-11/);
  assert.match(logs[1], /report: \/tmp\/daily\.human\.md/);
});

test("flag-alert mode renders HTML body with ticker and category", async () => {
  const { parseArgs, buildFlagAlertMessage } = await import("./send_daily_update_failure_email.mjs");
  const args = parseArgs([
    "--mode", "flag-alert",
    "--payload", JSON.stringify({
      source: "ib",
      ticker: "SMH",
      category: "range_shortfall",
      severity: "critical",
      detail: { shortfall_days: 9601 },
      ts: "2026-05-17T19:43:33Z",
    }),
  ]);
  assert.equal(args.mode, "flag-alert");
  const message = buildFlagAlertMessage(args.payload);
  assert.match(message.subject, /\[Livewire\].*SMH.*range_shortfall/);
  assert.match(message.html, /shortfall_days/);
  assert.match(message.html, /critical/i);
});

test("flag-alert mode rejects missing payload", async () => {
  const { parseArgs } = await import("./send_daily_update_failure_email.mjs");
  assert.throws(() => parseArgs(["--mode", "flag-alert"]),
    /payload.*required/i);
});
