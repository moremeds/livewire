# Resend SMTP from livewire@rsiarc.com

> **For Claude:** config/docs retarget. No SMTP rewrite. Do not add a Resend HTTP client.

**Goal:** Livewire notices send via Resend SMTP from `livewire@rsiarc.com`.

**Architecture:** Keep `notify.py` as the only email surface and `livewire_node/send_mail.mjs` as SMTP-only. Point the existing Nodemailer transport at `smtp.resend.com`. The From address and SMTP credentials stay in warehouse `.env` on the mini; a release carries no `.env`.

**Tech Stack:** Nodemailer 8 SMTP, Resend SMTP (`smtp.resend.com:465`, user `resend`, password = API key).

## Operator cutover (not in git)

1. Verify `rsiarc.com` in the Resend dashboard (SPF/DKIM).
2. Create an API key.
3. On the mini, in `~/market-warehouse/.env`:

```
MDW_ALERT_EMAIL_FROM="Livewire <livewire@rsiarc.com>"
MDW_ALERT_SMTP_HOST="smtp.resend.com"
MDW_ALERT_SMTP_PORT="465"
MDW_ALERT_SMTP_SECURE="true"
MDW_ALERT_SMTP_USER="resend"
MDW_ALERT_SMTP_PASS="<Resend API key>"
```

4. Send one forced page/digest and confirm the Resend emails log shows From `Livewire <livewire@rsiarc.com>`.
5. Mirror the same keys in the MacBook checkout `.env` for local sends.

Do not flip production SMTP until the API key and domain verification exist — that would silence nightly pages.
