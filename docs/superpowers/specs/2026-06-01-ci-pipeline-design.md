# CI Pipeline Design — Livewire

## Goal

Add GitHub Actions CI that gates every PR on: unit tests (100% coverage), secrets scanning, ruff lint/format, and pyright type checking. Migrate dependency management to uv with `pyproject.toml` as the single source of truth.

## Trigger

- PRs targeting `main` only
- No push-to-main trigger (all changes go through PRs per project convention)

## Workflow: `.github/workflows/ci.yml`

Single job, Ubuntu latest, Python 3.13. Steps run sequentially — each step is a hard gate.

### Steps

1. **Checkout** — `actions/checkout@v4`
2. **Set up Python 3.13** — `actions/setup-python@v5` with `python-version: "3.13"`
3. **Install uv** — `astral-sh/setup-uv@v5`
4. **Install dependencies** — `uv sync --frozen` (fails if `uv.lock` is stale)
5. **Secrets scan** — `bash tools/pre-commit-secrets-scan.sh` adapted for CI (scan changed files in the PR diff, not just staged files)
6. **Ruff lint** — `uv run ruff check .`
7. **Ruff format** — `uv run ruff format --check .`
8. **Pyright** — `uv run pyright clients/ livewire_scripts/`
9. **Pytest** — `uv run pytest tests/ -m "not integration" --cov --cov-fail-under=95 -W error::RuntimeWarning`
   (was `... and not postgres_live` with `--cov-fail-under=100`; the marker was dropped when the
   Postgres layer was removed 2026-08-02, and the gate is 95 in `pyproject.toml`)

### Why sequential, not parallel jobs

The full test suite runs in ~23 seconds. Splitting into parallel jobs would add more checkout+install overhead than it saves. A single job keeps the workflow simple and the feedback loop tight.

## Dependency Management Migration

### `pyproject.toml` changes

Add dependencies under standard PEP 621 fields:

```toml
[project]
dependencies = [
    "boto3>=1.42",
    "httpx>=0.28",
    "ib-async>=2.1",
    "lxml>=6.1",
    "psycopg[binary]>=3.3",
    "pyarrow>=23.0",
    "requests>=2.33",
    "rich>=14.0",
]

[project.optional-dependencies]
dev = [
    "pytest>=9.0",
    "pytest-cov>=7.0",
    "responses>=0.26",
    "ruff>=0.12",
    "pyright>=1.1",
]
```

Version bounds are minimums matching what's currently installed. The `uv.lock` file pins exact versions for reproducibility.

### Local workflow change

Developers run `uv sync` instead of manual pip installs. The venv at `~/market-warehouse/.venv/` continues to be used — uv respects `VIRTUAL_ENV` or can be pointed at it.

## Tool Configuration

### Ruff (`pyproject.toml`)

```toml
[tool.ruff]
target-version = "py313"
line-length = 120

[tool.ruff.lint]
select = ["E", "F", "W", "I", "UP", "B", "SIM"]
ignore = [
    "E501",   # line length handled by formatter, not linter
    "SIM108", # ternary — often less readable
]

[tool.ruff.lint.isort]
known-first-party = ["clients", "livewire_scripts"]
```

Rule sets:
- **E/F/W** — pycodestyle errors/warnings + pyflakes (the basics)
- **I** — isort import ordering
- **UP** — pyupgrade (modern Python idioms)
- **B** — bugbear (common pitfalls)
- **SIM** — simplify (unnecessary complexity)

Line length 120 avoids mass reformatting of existing code (current max is 213, but 99th percentile is well under 120).

### Pyright (`pyproject.toml`)

```toml
[tool.pyright]
pythonVersion = "3.13"
typeCheckingMode = "basic"
include = ["clients", "livewire_scripts"]
exclude = ["tests", "scripts"]
```

Basic mode catches real type errors without requiring full annotation coverage. Scoped to `clients/` and `livewire_scripts/` — `tests/` excluded to avoid noise from mock-heavy test code.

## Secrets Scan Adaptation

The existing `tools/pre-commit-secrets-scan.sh` scans `git diff --cached` (staged files). For CI, it needs to scan the PR diff instead. Two options:

- **Option A (recommended):** Add a `--ci` flag to the script that switches to `git diff origin/main...HEAD` instead of `--cached`.
- **Option B:** Write a thin CI-only wrapper that feeds the right file list into the same grep patterns.

Option A keeps one script as the single source of truth for patterns.

## What's Excluded

- **Integration tests** (`-m "not integration"`) — require IB Gateway at `127.0.0.1:4001`
- **Postgres tests** (`-m "not postgres_live"`) — require a live Postgres instance
- **Deployment** — livewire is a local-first tool, no deploy step
- **Matrix builds** — single Python version (3.13), single OS
- **Dependency caching** — uv installs in ~2-3 seconds; not worth the cache complexity initially

## Expected CI Runtime

- Checkout + Python + uv setup: ~10s
- `uv sync --frozen`: ~3s
- Secrets scan: <1s
- Ruff lint + format: ~2s
- Pyright: ~5-10s
- Pytest: ~23s
- **Total: ~45-50s**
