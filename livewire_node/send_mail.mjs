#!/usr/bin/env node

import fs from "node:fs/promises";

const DEFAULT_SUBJECT_PREFIX = "[Livewire]";
const USAGE =
  "Usage: node livewire_node/send_mail.mjs --subject=<text> --body-file=<path>\n" +
  "Single-token --key=value form only (a value may itself begin with '--').\n" +
  "Env: MDW_ALERT_EMAIL_FROM/TO/CC/BCC/REPLY_TO/SUBJECT_PREFIX; MDW_ALERT_SMTP_URL or\n" +
  "     MDW_ALERT_SMTP_HOST/PORT/SECURE/USER/PASS; MDW_ALERT_TRANSPORT=stream prints\n" +
  "     the RFC822 message, no network.";

function parseInteger(name, value) {
  const parsed = Number.parseInt(value, 10);
  if (!Number.isFinite(parsed)) throw new Error(`Invalid integer for ${name}: ${value}`);
  return parsed;
}

export function parseBoolean(value, fallback = false) {
  if (value == null || value === "") return fallback;
  const normalized = String(value).trim().toLowerCase();
  if (["1", "true", "yes", "on"].includes(normalized)) return true;
  if (["0", "false", "no", "off"].includes(normalized)) return false;
  throw new Error(`Invalid boolean value: ${value}`);
}

export function parseArgs(argv) {
  const options = {};
  for (const token of argv) {
    if (token === "--help" || token === "-h") {
      options.help = true;
      continue;
    }
    // --key=value only: the 2026-08-08 page died when a log-derived value
    // began with "---". Split on the FIRST "=" so "=" in the value survives.
    const equals = token.indexOf("=");
    if (!token.startsWith("--") || equals <= 2) {
      throw new Error(`Unexpected argument: ${token}`);
    }
    const key = token.slice(2, equals);
    const value = token.slice(equals + 1);
    if (key === "subject") options.subject = value;
    else if (key === "body-file") options.bodyFile = value;
    else throw new Error(`Unknown option: --${key}`);
  }
  if (options.help) return options;
  if (!options.subject) throw new Error("Missing --subject=");
  if (!options.bodyFile) throw new Error("Missing --body-file=");
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

export async function sendMail({ transportOptions, message }) {
  const { default: nodemailer } = await import("nodemailer");
  const transport = nodemailer.createTransport(transportOptions);
  // base64, not the quoted-printable default. Every body this file sends is
  // `key=value` telemetry, and QP gives `=` a second meaning: `=NN` is an
  // escape for the byte 0xNN. Measured on the 2026-08-16 digest, where the
  // on-disk body read `revision=28 rebuilt=10 unchanged=13135 trimmed=256
  // failed=240 no_trade=972` and the DELIVERED body read
  // `revision( rebuilt\x10 unchanged\x13135 trimmed%6 failed$0 no_trade\x972`
  // — one decode too many somewhere on the relay path, in BOTH the text and
  // html parts. `last=2026-08-14` became `last 26-08-14` the same way (=20 is
  // a space). A value survived only when its first two characters happened
  // not to be valid hex, so `updated=9` looked fine and `revision=28` did not:
  // silent, selective corruption of exactly the numbers the digest exists to
  // report. base64 has no in-band escape character, so no `=` in the payload
  // can be reinterpreted.
  return transport.sendMail({ textEncoding: "base64", ...message });
}

export async function main(argv = process.argv.slice(2), env = process.env, deps = {}) {
  const options = parseArgs(argv);
  if (options.help) {
    console.log(USAGE);
    return 0;
  }
  const stream = env.MDW_ALERT_TRANSPORT === "stream";
  // streamTransport never contacts a transport; the placeholder satisfies the
  // SMTP check, and the stream transportOptions below overrides it regardless.
  const alertConfig = resolveAlertConfig(
    stream ? { MDW_ALERT_SMTP_URL: "smtp://stream.invalid", ...env } : env,
  );
  const body = await (deps.readFile || fs.readFile)(options.bodyFile, "utf8");
  const { from, to, cc, bcc, replyTo, subjectPrefix } = alertConfig;
  const message = { from, to, cc, bcc, replyTo, subject: `${subjectPrefix} ${options.subject}`, text: body };
  const transportOptions = stream
    ? { streamTransport: true, buffer: true }
    : alertConfig.transportOptions;
  const info = await (deps.sendMail || sendMail)({ transportOptions, message });
  // streamTransport returns no `accepted` list — reaching here means the
  // message was built; printing it is the whole job.
  if (stream) {
    console.log(info.message.toString());
    return 0;
  }
  console.log(JSON.stringify({ accepted: info.accepted, messageId: info.messageId }));
  return info.accepted && info.accepted.length > 0 ? 0 : 1;
}

if (import.meta.url === `file://${process.argv[1]}`) {
  main()
    .then((code) => (process.exitCode = code))
    .catch((e) => (console.error(`send_mail: ${e.message}`), (process.exitCode = 1)));
}
