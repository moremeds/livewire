#!/usr/bin/env node

import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";

import { generateHumanReadableIncidentReport } from "./cerebras_client.mjs";

const DEFAULT_TAIL_LINES = 40;
const DEFAULT_JOB_NAME = "daily_update";
const DEFAULT_SUBJECT_PREFIX = "[Market Data Warehouse]";
const MODES = new Set(["failure", "flag-alert", "daily-summary"]);

function usage() {
  return [
    "Usage: node scripts/send_daily_update_failure_email.mjs [options]",
    "",
    "Options:",
    "  --run-date YYYY-MM-DD",
    "  --log-file /absolute/path/to/log",
    "  --attempts N",
    "  --exit-code N",
    "  --error-summary TEXT",
    "  --repo-root /absolute/path/to/repo",
    "  --job-name NAME",
    "  --tail-lines N",
    "  --mode failure|flag-alert|daily-summary",
    "  --payload JSON",
  ].join("\n");
}

function parseInteger(name, value) {
  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed)) {
    throw new Error(`Invalid integer for ${name}: ${value}`);
  }
  return parsed;
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

export function parseBoolean(value, fallback = false) {
  if (value == null || value === "") {
    return fallback;
  }
  const normalized = String(value).trim().toLowerCase();
  if (["1", "true", "yes", "on"].includes(normalized)) {
    return true;
  }
  if (["0", "false", "no", "off"].includes(normalized)) {
    return false;
  }
  throw new Error(`Invalid boolean value: ${value}`);
}

export function parseArgs(argv) {
  const options = {
    jobName: DEFAULT_JOB_NAME,
    tailLines: DEFAULT_TAIL_LINES,
  };

  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    if (token === "--help" || token === "-h") {
      options.help = true;
      continue;
    }

    if (!token.startsWith("--")) {
      throw new Error(`Unexpected argument: ${token}`);
    }

    const key = token.slice(2);
    const value = argv[index + 1];
    if (value == null || value.startsWith("--")) {
      throw new Error(`Missing value for --${key}`);
    }
    index += 1;

    switch (key) {
      case "run-date":
        options.runDate = value;
        break;
      case "log-file":
        options.logFile = value;
        break;
      case "attempts":
        options.attempts = parseInteger("--attempts", value);
        break;
      case "exit-code":
        options.exitCode = parseInteger("--exit-code", value);
        break;
      case "error-summary":
        options.errorSummary = value;
        break;
      case "repo-root":
        options.repoRoot = value;
        break;
      case "job-name":
        options.jobName = value;
        break;
      case "tail-lines":
        options.tailLines = parseInteger("--tail-lines", value);
        break;
      case "mode":
        if (!MODES.has(value)) {
          throw new Error(`Invalid mode: ${value}`);
        }
        options.mode = value;
        break;
      case "payload":
        options.payloadRaw = value;
        break;
      default:
        throw new Error(`Unknown option: --${key}`);
    }
  }

  if (options.mode === "flag-alert" || options.mode === "daily-summary") {
    if (!options.payloadRaw) {
      throw new Error(`--payload is required for mode ${options.mode}`);
    }
    options.payload = JSON.parse(options.payloadRaw);
    delete options.payloadRaw;
  }

  return options;
}

export function resolveAlertConfig(env = process.env) {
  const from = env.MDW_ALERT_EMAIL_FROM;
  const to = env.MDW_ALERT_EMAIL_TO;
  if (!from) {
    throw new Error("Missing MDW_ALERT_EMAIL_FROM");
  }
  if (!to) {
    throw new Error("Missing MDW_ALERT_EMAIL_TO");
  }

  const baseConfig = {
    from,
    to,
    cc: env.MDW_ALERT_EMAIL_CC || undefined,
    bcc: env.MDW_ALERT_EMAIL_BCC || undefined,
    replyTo: env.MDW_ALERT_EMAIL_REPLY_TO || undefined,
    subjectPrefix: env.MDW_ALERT_EMAIL_SUBJECT_PREFIX || DEFAULT_SUBJECT_PREFIX,
  };

  if (env.MDW_ALERT_SMTP_URL) {
    return {
      ...baseConfig,
      transportOptions: env.MDW_ALERT_SMTP_URL,
    };
  }

  const host = env.MDW_ALERT_SMTP_HOST;
  const port = env.MDW_ALERT_SMTP_PORT;
  if (!host) {
    throw new Error("Missing MDW_ALERT_SMTP_HOST or MDW_ALERT_SMTP_URL");
  }
  if (!port) {
    throw new Error("Missing MDW_ALERT_SMTP_PORT or MDW_ALERT_SMTP_URL");
  }

  return {
    ...baseConfig,
    transportOptions: {
      host,
      port: parseInteger("MDW_ALERT_SMTP_PORT", port),
      secure: parseBoolean(env.MDW_ALERT_SMTP_SECURE, false),
      auth: env.MDW_ALERT_SMTP_USER
        ? {
            user: env.MDW_ALERT_SMTP_USER,
            pass: env.MDW_ALERT_SMTP_PASS || "",
          }
        : undefined,
    },
  };
}

export async function readLogTail(logFile, tailLines = DEFAULT_TAIL_LINES) {
  if (!logFile) {
    return "(no log file path provided)";
  }

  try {
    const content = await fs.readFile(logFile, "utf8");
    const lines = content.trimEnd().split(/\r?\n/);
    return lines.slice(-tailLines).join("\n");
  } catch (error) {
    return `(unable to read log tail from ${logFile}: ${error.message})`;
  }
}

export function buildHumanReadableReportPath(logFile) {
  if (!logFile) {
    return null;
  }
  const parsed = path.parse(logFile);
  return path.join(parsed.dir, `${parsed.name}.human.md`);
}

export function buildFallbackIncidentReport({ options, logTail, error }) {
  const reason =
    error?.message || "AI summary unavailable because no Cerebras result was returned.";
  return {
    summary:
      options.errorSummary ||
      "The scheduled job failed, but only the raw error summary is available.",
    probableCause: `AI summary unavailable: ${reason}`,
    proposedSolution:
      "Inspect the raw error summary and recent log tail, then rerun the job after correcting the failing dependency or configuration.",
    nextSteps: [
      `Open the raw log at ${options.logFile || "(not provided)"}.`,
      "Verify the failing dependency or upstream service is reachable and healthy.",
      "Retry the scheduled job after addressing the root cause.",
      logTail ? "Use the recent log tail below to confirm the exact failing step." : "",
    ].filter(Boolean),
  };
}

export function buildHumanReadableReport({
  options,
  incidentReport,
  logTail,
  hostname = os.hostname(),
  generatedAt = new Date().toISOString(),
}) {
  const attempts = options.attempts ?? "(unknown)";
  const exitCode = options.exitCode ?? "(unknown)";
  const steps = incidentReport.nextSteps.length
    ? incidentReport.nextSteps.map((step, index) => `${index + 1}. ${step}`).join("\n")
    : "1. Review the raw log tail and retry after addressing the root cause.";

  return [
    `# ${options.jobName} failure report`,
    "",
    `Generated at: ${generatedAt}`,
    `Hostname: ${hostname}`,
    `Run date: ${options.runDate || "(unknown)"}`,
    `Attempts: ${attempts}`,
    `Exit code: ${exitCode}`,
    `Repository: ${options.repoRoot || process.cwd()}`,
    `Raw log file: ${options.logFile || "(not provided)"}`,
    "",
    "## Human-readable summary",
    incidentReport.summary,
    "",
    "## Probable cause",
    incidentReport.probableCause,
    "",
    "## Proposed solution",
    incidentReport.proposedSolution,
    "",
    "## Recommended next steps",
    steps,
    "",
    "## Raw error summary",
    options.errorSummary || "(not provided)",
    "",
    "## Recent log tail",
    "```text",
    logTail,
    "```",
    "",
  ].join("\n");
}

export async function writeHumanReadableReport(reportPath, reportBody) {
  if (!reportPath) {
    return null;
  }
  await fs.mkdir(path.dirname(reportPath), { recursive: true });
  await fs.writeFile(reportPath, reportBody, "utf8");
  return reportPath;
}

function buildFailureText({
  options,
  incidentReport,
  logTail,
  humanReportPath,
  hostname,
  generatedAt,
}) {
  const attempts = options.attempts ?? "(unknown)";
  const exitCode = options.exitCode ?? "(unknown)";
  const nextSteps = incidentReport.nextSteps.length
    ? incidentReport.nextSteps.map((step, index) => `${index + 1}. ${step}`).join("\n")
    : "1. Review the raw log tail and retry after addressing the root cause.";

  return [
    `The scheduled ${options.jobName} job failed.`,
    "",
    "Human-readable summary:",
    incidentReport.summary,
    "",
    "Probable cause:",
    incidentReport.probableCause,
    "",
    "Proposed solution:",
    incidentReport.proposedSolution,
    "",
    "Recommended next steps:",
    nextSteps,
    "",
    "Run details:",
    `Run date: ${options.runDate || "(unknown)"}`,
    `Attempts: ${attempts}`,
    `Exit code: ${exitCode}`,
    `Hostname: ${hostname}`,
    `Generated at: ${generatedAt}`,
    `Repository: ${options.repoRoot || process.cwd()}`,
    `Log file: ${options.logFile || "(not provided)"}`,
    `Human-readable report: ${humanReportPath || "(not written)"}`,
    `Raw error summary: ${options.errorSummary || "(not provided)"}`,
    "",
    "Recent log tail:",
    logTail,
  ].join("\n");
}

function buildFailureHtml({
  alertConfig,
  options,
  incidentReport,
  logTail,
  humanReportPath,
  hostname,
  generatedAt,
}) {
  const attempts = options.attempts ?? "(unknown)";
  const exitCode = options.exitCode ?? "(unknown)";
  const nextSteps = incidentReport.nextSteps.length
    ? incidentReport.nextSteps
    : ["Review the raw log tail and retry after addressing the root cause."];

  const rows = [
    ["Run date", options.runDate || "(unknown)"],
    ["Attempts", attempts],
    ["Exit code", exitCode],
    ["Hostname", hostname],
    ["Generated at", generatedAt],
    ["Repository", options.repoRoot || process.cwd()],
    ["Raw log file", options.logFile || "(not provided)"],
    ["Human-readable report", humanReportPath || "(not written)"],
    ["Reply-to", alertConfig.replyTo || "(not set)"],
  ]
    .map(
      ([label, value]) => `<tr>
        <td style="padding:8px 12px;border-bottom:1px solid #e5e7eb;color:#475569;font-weight:600;">${escapeHtml(label)}</td>
        <td style="padding:8px 12px;border-bottom:1px solid #e5e7eb;color:#0f172a;">${escapeHtml(value)}</td>
      </tr>`,
    )
    .join("");

  const stepItems = nextSteps
    .map(
      (step) =>
        `<li style="margin:0 0 8px 0; color:#0f172a;">${escapeHtml(step)}</li>`,
    )
    .join("");

  return `<!doctype html>
<html>
  <body style="margin:0;padding:24px;background:#f8fafc;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;color:#0f172a;">
    <div style="max-width:760px;margin:0 auto;background:#ffffff;border:1px solid #dbe4ee;border-radius:18px;overflow:hidden;">
      <div style="padding:24px 28px;background:linear-gradient(135deg,#7f1d1d 0%,#991b1b 100%);color:#ffffff;">
        <div style="font-size:12px;letter-spacing:0.08em;text-transform:uppercase;opacity:0.78;">Market Data Warehouse Alert</div>
        <h1 style="margin:8px 0 0;font-size:28px;line-height:1.2;">${escapeHtml(options.jobName)} failed</h1>
        <p style="margin:10px 0 0;font-size:15px;line-height:1.5;opacity:0.92;">${escapeHtml(
          incidentReport.summary,
        )}</p>
      </div>

      <div style="padding:28px;">
        <div style="margin-bottom:24px;padding:20px;border-radius:14px;background:#fff7ed;border:1px solid #fdba74;">
          <div style="font-size:12px;letter-spacing:0.08em;text-transform:uppercase;color:#9a3412;font-weight:700;">Probable cause</div>
          <p style="margin:10px 0 0;line-height:1.6;color:#7c2d12;">${escapeHtml(
            incidentReport.probableCause,
          )}</p>
        </div>

        <div style="margin-bottom:24px;padding:20px;border-radius:14px;background:#ecfdf5;border:1px solid #86efac;">
          <div style="font-size:12px;letter-spacing:0.08em;text-transform:uppercase;color:#166534;font-weight:700;">Proposed solution</div>
          <p style="margin:10px 0 0;line-height:1.6;color:#166534;">${escapeHtml(
            incidentReport.proposedSolution,
          )}</p>
        </div>

        <div style="margin-bottom:24px;">
          <div style="font-size:12px;letter-spacing:0.08em;text-transform:uppercase;color:#475569;font-weight:700;margin-bottom:12px;">Recommended next steps</div>
          <ol style="margin:0;padding-left:20px;">${stepItems}</ol>
        </div>

        <div style="margin-bottom:24px;">
          <div style="font-size:12px;letter-spacing:0.08em;text-transform:uppercase;color:#475569;font-weight:700;margin-bottom:12px;">Run details</div>
          <table style="width:100%;border-collapse:collapse;border:1px solid #e5e7eb;border-radius:12px;overflow:hidden;">${rows}</table>
        </div>

        <div style="margin-bottom:24px;">
          <div style="font-size:12px;letter-spacing:0.08em;text-transform:uppercase;color:#475569;font-weight:700;margin-bottom:12px;">Raw error summary</div>
          <div style="padding:16px;border-radius:12px;background:#f8fafc;border:1px solid #e5e7eb;line-height:1.6;color:#0f172a;">${escapeHtml(
            options.errorSummary || "(not provided)",
          )}</div>
        </div>

        <div>
          <div style="font-size:12px;letter-spacing:0.08em;text-transform:uppercase;color:#475569;font-weight:700;margin-bottom:12px;">Recent log tail</div>
          <pre style="margin:0;padding:16px;border-radius:12px;background:#0f172a;color:#e2e8f0;overflow:auto;white-space:pre-wrap;border:1px solid #1e293b;">${escapeHtml(
            logTail,
          )}</pre>
        </div>
      </div>
    </div>
  </body>
</html>`;
}

export function buildFailureMessage({
  alertConfig,
  options,
  logTail,
  incidentReport,
  humanReportPath,
  hostname = os.hostname(),
  generatedAt = new Date().toISOString(),
}) {
  const runDate = options.runDate || new Date().toISOString().slice(0, 10);
  const subject = `${alertConfig.subjectPrefix} ${options.jobName} failed on ${runDate}`;
  const text = buildFailureText({
    options,
    incidentReport,
    logTail,
    humanReportPath,
    hostname,
    generatedAt,
  });
  const html = buildFailureHtml({
    alertConfig,
    options,
    incidentReport,
    logTail,
    humanReportPath,
    hostname,
    generatedAt,
  });

  return {
    from: alertConfig.from,
    to: alertConfig.to,
    cc: alertConfig.cc,
    bcc: alertConfig.bcc,
    replyTo: alertConfig.replyTo,
    subject,
    text,
    html,
  };
}

export function buildFlagAlertMessage(payload) {
  const { source, ticker, category, severity, detail, ts } = payload;
  const subject = `${DEFAULT_SUBJECT_PREFIX.replace("Market Data Warehouse", "Livewire")} ${severity.toUpperCase()} ${ticker} ${category}`;
  const detailHtml = `<pre>${escapeHtml(JSON.stringify(detail, null, 2))}</pre>`;
  const html = `<html><body>
    <h2>[Livewire] ${escapeHtml(severity)} quality flag</h2>
    <p><b>Source:</b> ${escapeHtml(source)} &nbsp; <b>Ticker:</b> ${escapeHtml(ticker)}</p>
    <p><b>Category:</b> ${escapeHtml(category)}</p>
    <p><b>Detected at:</b> ${escapeHtml(ts || "")}</p>
    ${detailHtml}
  </body></html>`;
  const text = `[Livewire] ${severity.toUpperCase()} ${ticker} ${category}\n${JSON.stringify(detail, null, 2)}`;
  return { subject, html, text };
}

export function buildDailySummaryMessage(payload) {
  const sourceRows = (payload.sources || [])
    .map(
      (row) => `<tr>
        <td>${escapeHtml(row.source)}</td>
        <td>${escapeHtml(row.connection_events ?? 0)}</td>
        <td>${escapeHtml(row.uptime_pct ?? "")}</td>
        <td>${escapeHtml(row.flap_count ?? 0)}</td>
      </tr>`,
    )
    .join("");
  const flagRows = Object.entries(payload.flag_counts_by_category || {})
    .map(
      ([category, count]) => `<tr>
        <td>${escapeHtml(category)}</td>
        <td>${escapeHtml(count)}</td>
      </tr>`,
    )
    .join("");
  const tickerRows = (payload.top_tickers || [])
    .map(
      (row) => `<tr>
        <td>${escapeHtml(row.ticker)}</td>
        <td>${escapeHtml(row.flag_count)}</td>
      </tr>`,
    )
    .join("");
  const subject = `${DEFAULT_SUBJECT_PREFIX.replace("Market Data Warehouse", "Livewire")} daily summary ${payload.window || ""}`.trim();
  const html = `<html><body>
    <h2>[Livewire] daily summary</h2>
    <p><b>Window:</b> ${escapeHtml(payload.window || "")}</p>
    <h3>Sources</h3>
    <table><thead><tr><th>Source</th><th>Events</th><th>Uptime %</th><th>Flaps</th></tr></thead><tbody>${sourceRows}</tbody></table>
    <h3>Flags by category</h3>
    <table><thead><tr><th>Category</th><th>Count</th></tr></thead><tbody>${flagRows}</tbody></table>
    <h3>Top tickers</h3>
    <table><thead><tr><th>Ticker</th><th>Flags</th></tr></thead><tbody>${tickerRows}</tbody></table>
  </body></html>`;
  const text = `[Livewire] daily summary ${payload.window || ""}\n${JSON.stringify(payload, null, 2)}`;
  return { subject, html, text };
}

export async function sendFailureAlert({ transportOptions, message }) {
  const { default: nodemailer } = await import("nodemailer");
  const transport = nodemailer.createTransport(transportOptions);
  return transport.sendMail(message);
}

async function runFlagAlert(options, env = process.env, deps = {}) {
  const alertConfig = resolveAlertConfig(env);
  const message = {
    from: alertConfig.from,
    to: alertConfig.to,
    cc: alertConfig.cc,
    bcc: alertConfig.bcc,
    replyTo: alertConfig.replyTo,
    ...buildFlagAlertMessage(options.payload),
  };
  const info = await (deps.sendFailureAlert || sendFailureAlert)({
    transportOptions: alertConfig.transportOptions,
    message,
  });
  return {
    mode: "flag-alert",
    info,
    message,
  };
}

async function runDailySummary(options, env = process.env, deps = {}) {
  const alertConfig = resolveAlertConfig(env);
  const message = {
    from: alertConfig.from,
    to: alertConfig.to,
    cc: alertConfig.cc,
    bcc: alertConfig.bcc,
    replyTo: alertConfig.replyTo,
    ...buildDailySummaryMessage(options.payload),
  };
  const info = await (deps.sendFailureAlert || sendFailureAlert)({
    transportOptions: alertConfig.transportOptions,
    message,
  });
  return {
    mode: "daily-summary",
    info,
    message,
  };
}

export async function runFailureAlert(argv, env = process.env, deps = {}) {
  const options = parseArgs(argv);
  if (options.help) {
    return { help: usage() };
  }
  if (options.mode === "flag-alert") {
    return runFlagAlert(options, env, deps);
  }
  if (options.mode === "daily-summary") {
    return runDailySummary(options, env, deps);
  }

  const alertConfig = resolveAlertConfig(env);
  const generatedAt = deps.generatedAt || new Date().toISOString();
  const hostname = deps.hostname || os.hostname();
  const logTail = await (deps.readLogTail || readLogTail)(
    options.logFile,
    options.tailLines,
  );

  let incidentReport;
  try {
    incidentReport = await (deps.generateIncidentReport ||
      generateHumanReadableIncidentReport)(
      {
        jobName: options.jobName,
        runDate: options.runDate,
        attempts: options.attempts,
        exitCode: options.exitCode,
        repoRoot: options.repoRoot,
        logFile: options.logFile,
        errorSummary: options.errorSummary,
        logTail,
      },
      env,
      deps.cerebrasDeps || {},
    );
  } catch (error) {
    incidentReport = buildFallbackIncidentReport({ options, logTail, error });
  }

  const humanReportPath =
    (deps.buildHumanReadableReportPath || buildHumanReadableReportPath)(
      options.logFile,
    );
  const humanReport = buildHumanReadableReport({
    options,
    incidentReport,
    logTail,
    hostname,
    generatedAt,
  });

  let writtenReportPath = null;
  try {
    writtenReportPath = await (deps.writeHumanReadableReport ||
      writeHumanReadableReport)(humanReportPath, humanReport);
  } catch (error) {
    incidentReport = {
      ...incidentReport,
      nextSteps: [
        ...incidentReport.nextSteps,
        `Human-readable report could not be written: ${error.message}`,
      ],
    };
  }

  const message = buildFailureMessage({
    alertConfig,
    options,
    logTail,
    incidentReport,
    humanReportPath: writtenReportPath,
    hostname,
    generatedAt,
  });
  const info = await (deps.sendFailureAlert || sendFailureAlert)({
    transportOptions: alertConfig.transportOptions,
    message,
  });

  return {
    info,
    message,
    incidentReport,
    humanReportPath: writtenReportPath,
  };
}

export async function main(argv = process.argv.slice(2), env = process.env, deps = {}) {
  const result = await runFailureAlert(argv, env, deps);
  if (result.help) {
    console.log(result.help);
    return result;
  }

  if (result.mode === "flag-alert") {
    console.log(`Sent quality flag alert: ${result.message.subject}`);
    return result;
  }
  if (result.mode === "daily-summary") {
    console.log(`Sent daily quality summary: ${result.message.subject}`);
    return result;
  }

  const reportSuffix = result.humanReportPath
    ? ` (report: ${result.humanReportPath})`
    : "";
  console.log(`Sent daily update failure alert: ${result.message.subject}${reportSuffix}`);
  return result;
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main().catch((error) => {
    console.error(`Failed to send daily update failure alert: ${error.message}`);
    process.exitCode = 1;
  });
}
