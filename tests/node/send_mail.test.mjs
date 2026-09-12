import { mkdtemp, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import assert from "node:assert/strict";

import {
  main,
  parseArgs,
  parseBoolean,
  resolveAlertConfig,
  sendMail,
} from "../../livewire_node/send_mail.mjs";

const smtpEnv = {
  MDW_ALERT_EMAIL_FROM: "from@example.com",
  MDW_ALERT_EMAIL_TO: "to@example.com",
  MDW_ALERT_SMTP_URL: "smtp://user:pass@mail.example.com:587",
};

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

test("resolveAlertConfig supports host port auth fields and requires FROM and TO", () => {
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
  assert.throws(
    () => resolveAlertConfig({ MDW_ALERT_EMAIL_FROM: "from@example.com" }),
    /Missing MDW_ALERT_EMAIL_TO/,
  );
  assert.throws(
    () =>
      resolveAlertConfig({
        MDW_ALERT_EMAIL_FROM: "from@example.com",
        MDW_ALERT_EMAIL_TO: "to@example.com",
      }),
    /Missing MDW_ALERT_SMTP_HOST or MDW_ALERT_SMTP_URL/,
  );
});

test("parseBoolean accepts common truthy and falsy forms", () => {
  assert.equal(parseBoolean("true"), true);
  assert.equal(parseBoolean("1"), true);
  assert.equal(parseBoolean("no"), false);
  assert.equal(parseBoolean(undefined, true), true);
});

test("parseArgs accepts --subject= and --body-file= only", () => {
  const options = parseArgs([
    "--subject=digest 2026-09-12",
    "--body-file=/tmp/body.txt",
  ]);
  assert.equal(options.subject, "digest 2026-09-12");
  assert.equal(options.bodyFile, "/tmp/body.txt");

  assert.throws(() => parseArgs(["--body-file=/tmp/b.txt"]), /Missing --subject=/);
  assert.throws(() => parseArgs(["--subject=x"]), /Missing --body-file=/);
  assert.throws(() => parseArgs(["--nonsense=x"]), /Unknown option: --nonsense/);
  // No two-token form exists: a bare "--subject" has no "=" and is rejected.
  assert.throws(() => parseArgs(["--subject", "x", "--body-file=/b"]), /Unexpected argument: --subject/);
  assert.throws(() => parseArgs(["/bare/path"]), /Unexpected argument: \/bare\/path/);
  assert.equal(parseArgs(["--help"]).help, true);
});

// The 2026-08-08 and 2026-08-09 intraday-catchup pages were never sent:
// "Missing value for --error-summary". The summary is log-derived text and
// began with "--- Runbook: ...", and a two-token parse treats any value
// starting with "--" as the next flag. The watchdog caught it 5.5h later.
test("a subject beginning with -- survives in --key=value form", () => {
  const options = parseArgs([
    "--subject=--not-a-flag",
    "--body-file=/tmp/b.txt",
  ]);
  assert.equal(options.subject, "--not-a-flag");
});

test("an = inside the value is preserved", () => {
  const options = parseArgs([
    "--subject=exit_code=86 lanes=equity",
    "--body-file=/tmp/b.txt",
  ]);
  assert.equal(options.subject, "exit_code=86 lanes=equity");
});

test("sendMail stamps textEncoding base64 on the message it hands to the transport", async () => {
  let sentData;
  const capture = {
    name: "capture",
    version: "1.0.0",
    send(mail, callback) {
      sentData = mail.data;
      callback(null, {
        envelope: { from: mail.data.from, to: [mail.data.to] },
        messageId: "<captured@local>",
        accepted: ["to@example.com"],
        rejected: [],
        response: "250 captured",
      });
    },
  };

  const info = await sendMail({
    transportOptions: capture,
    message: {
      from: "from@example.com",
      to: "to@example.com",
      subject: "s",
      text: "revision=28",
    },
  });

  assert.equal(sentData.textEncoding, "base64");
  assert.deepEqual(info.accepted, ["to@example.com"]);
});

test("digest metrics survive the wire — no quoted-printable reinterpretation of '='", async () => {
  // Verbatim line 33 of logs/nightly_digest_2026-08-16.txt. Delivered as
  // quoted-printable it arrived as
  //   revision(  rebuilt\x10  unchanged\x13135  trimmed%6  failed$0
  // because `=28`, `=10`, `=13`, `=25` and `=24` are all valid QP escapes.
  // `updated=9` survived only because "9 " is not a hex pair — silent,
  // selective corruption of exactly the numbers the digest exists to report.
  //
  // The em-dash and ⚠ are load-bearing, not decoration: nodemailer sends a
  // pure-ASCII body as 7bit and never consults textEncoding at all. A real
  // digest always carries them, which is what forces an encoding choice — and
  // for a mostly-ASCII body quoted-printable is the shorter of the two, so it
  // is what the default picks.
  const body = [
    "Livewire nightly digest — 2026-08-16",
    "  revision=28  rebuilt=10  unchanged=13135  trimmed=256  failed=240",
    "  ⚠ 43 symbol(s) withheld",
    "  bronze_equity_1d  13,385 symbols  last=2026-08-14",
  ].join("\n");

  const info = await sendMail({
    transportOptions: { streamTransport: true, buffer: true },
    message: {
      from: "from@example.com",
      to: "to@example.com",
      subject: "subject",
      text: body,
    },
  });

  const raw = info.message.toString("utf8");
  // The invariant is "never quoted-printable", not "always base64": 7bit is
  // equally safe (it is sent verbatim). Only QP gives `=` a second meaning.
  assert.doesNotMatch(raw, /Content-Transfer-Encoding: quoted-printable/i);
  assert.match(raw, /Content-Transfer-Encoding: base64/i);

  // Decode what a receiver would actually decode, and require it byte-identical.
  const encoded = raw.split(/\r?\n\r?\n/).slice(1).join("").trim();
  assert.equal(Buffer.from(encoded, "base64").toString("utf8").trim(), body.trim());
});

test("main prefixes the subject, sends the body file, and prints one JSON line", async () => {
  const tmpdir = await mkdtemp(path.join(os.tmpdir(), "mdw-send-mail-"));
  const bodyFile = path.join(tmpdir, "body.txt");
  await writeFile(bodyFile, "revision=28 rebuilt=10\n", "utf8");

  let sent;
  const printed = [];
  const originalLog = console.log;
  console.log = (message) => printed.push(message);
  let code;
  try {
    code = await main(
      [`--subject=PAGE 2026-09-12: Coverage BAD`, `--body-file=${bodyFile}`],
      { ...smtpEnv, MDW_ALERT_EMAIL_SUBJECT_PREFIX: "[MDW]" },
      {
        sendMail: async ({ transportOptions, message }) => {
          sent = { transportOptions, message };
          return { accepted: ["to@example.com"], messageId: "mid-1" };
        },
      },
    );
  } finally {
    console.log = originalLog;
  }

  assert.equal(code, 0);
  assert.equal(sent.message.subject, "[MDW] PAGE 2026-09-12: Coverage BAD");
  assert.equal(sent.message.text, "revision=28 rebuilt=10\n");
  assert.equal(sent.message.from, "from@example.com");
  assert.equal(sent.message.to, "to@example.com");
  assert.equal(sent.transportOptions, "smtp://user:pass@mail.example.com:587");
  assert.deepEqual(JSON.parse(printed[0]), {
    accepted: ["to@example.com"],
    messageId: "mid-1",
  });
});

test("main exits 1 when nothing was accepted, still printing the JSON line", async () => {
  const tmpdir = await mkdtemp(path.join(os.tmpdir(), "mdw-send-mail-"));
  const bodyFile = path.join(tmpdir, "body.txt");
  await writeFile(bodyFile, "body\n", "utf8");

  const printed = [];
  const originalLog = console.log;
  console.log = (message) => printed.push(message);
  let code;
  try {
    code = await main(
      ["--subject=x", `--body-file=${bodyFile}`],
      smtpEnv,
      {
        sendMail: async () => ({
          accepted: [],
          rejected: ["to@example.com"],
          messageId: "mid-rej",
        }),
      },
    );
  } finally {
    console.log = originalLog;
  }

  assert.equal(code, 1);
  assert.deepEqual(JSON.parse(printed[0]), {
    accepted: [],
    messageId: "mid-rej",
  });
});

test("MDW_ALERT_TRANSPORT=stream prints the RFC822 message with no SMTP config", async () => {
  const tmpdir = await mkdtemp(path.join(os.tmpdir(), "mdw-send-mail-"));
  const bodyFile = path.join(tmpdir, "body.txt");
  await writeFile(bodyFile, "revision=28 rebuilt=10\n", "utf8");

  const printed = [];
  const originalLog = console.log;
  console.log = (message) => printed.push(message);
  let code;
  try {
    code = await main(
      ["--subject=digest 2026-09-12", `--body-file=${bodyFile}`],
      {
        MDW_ALERT_TRANSPORT: "stream",
        MDW_ALERT_EMAIL_FROM: "from@example.com",
        MDW_ALERT_EMAIL_TO: "to@example.com",
        // Deliberately no MDW_ALERT_SMTP_* — stream mode must not need one.
      },
    );
  } finally {
    console.log = originalLog;
  }

  assert.equal(code, 0);
  const raw = printed.join("\n");
  assert.match(raw, /^Subject: \[Livewire\] digest 2026-09-12$/m);
  assert.match(raw, /^From: from@example\.com$/m);
  assert.match(raw, /revision=28 rebuilt=10/);
});
