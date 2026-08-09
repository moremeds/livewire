"""Immutable release artifacts, so production never runs from the dev checkout.

``promote`` materializes the merged ``origin/main`` commit into
``<warehouse>/releases/<sha>/`` — a ``git archive`` export plus its own frozen
virtualenv — and then atomically repoints ``<warehouse>/current`` at it.
Scheduled jobs ``cd`` into ``current``, so editing, branching, or breaking the
working tree cannot change what runs tonight.

A linked ``git worktree`` would be the cheaper export, but it leaves a ``.git``
file pointing back at the dev repo — the artifact would still be tethered to the
very checkout it is supposed to be independent of. ``git archive`` has no such
tether.

The data lake is deliberately *not* isolated: it is the single source of truth
and both dev and production write it. Concurrency there is handled where it
always was, by the ``fcntl.flock`` serialization in ``clients/parquet_io.py``.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import subprocess
import sys
import tarfile
import tempfile
from collections.abc import Sequence
from pathlib import Path

from livewire_scripts.paths import warehouse_dir

REPO_ROOT = Path(__file__).resolve().parents[1]
CI_WORKFLOW = "ci.yml"
DEFAULT_KEEP = 3
LOGGER = logging.getLogger("livewire.release")


class ReleaseError(RuntimeError):
    """A release step failed. The ``current`` symlink is left untouched."""


def releases_dir() -> Path:
    """Return the directory holding built release trees."""
    return Path(os.environ.get("MDW_RELEASES_DIR", warehouse_dir() / "releases")).expanduser()


def current_link() -> Path:
    """Return the symlink production resolves its code through."""
    return Path(os.environ.get("MDW_CURRENT_LINK", warehouse_dir() / "current")).expanduser()


def _run(cmd: Sequence[object], cwd: Path | None = None, check: bool = True) -> str:
    """Run a command and return stdout. Tests monkeypatch this single seam."""
    argv = [str(part) for part in cmd]
    result = subprocess.run(
        argv,
        cwd=None if cwd is None else str(cwd),
        capture_output=True,
        text=True,
        check=False,
    )
    if check and result.returncode != 0:
        raise ReleaseError(f"{' '.join(argv)} failed ({result.returncode}): {result.stderr.strip()}")
    return result.stdout


def resolve_main_sha(*, fetch: bool = True) -> str:
    """Return the commit ``origin/main`` currently points at."""
    if fetch:
        _run(["git", "fetch", "--quiet", "origin", "main"], cwd=REPO_ROOT)
    return _run(["git", "rev-parse", "origin/main"], cwd=REPO_ROOT).strip()


def ci_is_green(sha: str) -> bool:
    """Return whether ``ci.yml`` completed successfully for this exact commit.

    ``ci.yml`` must run on pushes to main for this to ever be true: a squash
    merge creates a commit that no pull-request run ever covered.
    """
    raw = _run(
        [
            "gh",
            "run",
            "list",
            "--commit",
            sha,
            "--workflow",
            CI_WORKFLOW,
            "--limit",
            "20",
            "--json",
            "status,conclusion",
        ],
        cwd=REPO_ROOT,
    )
    runs = json.loads(raw or "[]")
    return any(run.get("status") == "completed" and run.get("conclusion") == "success" for run in runs)


def export_tree(sha: str, dest: Path) -> None:
    """Extract the tracked tree at ``sha`` into ``dest``."""
    dest.mkdir(parents=True)
    with tempfile.TemporaryDirectory() as tmp:
        archive = Path(tmp) / "release.tar"
        _run(["git", "archive", "--format=tar", f"--output={archive}", sha], cwd=REPO_ROOT)
        with tarfile.open(archive) as tar:
            tar.extractall(dest, filter="data")


def build_venv(dest: Path) -> None:
    """Build the release's own frozen virtualenv and prove the tree imports."""
    _run(["uv", "sync", "--frozen", "--no-dev"], cwd=dest)
    python = dest / ".venv" / "bin" / "python"
    if not python.exists():
        raise ReleaseError(f"uv sync produced no interpreter at {python}")
    # A tree that cannot import is never promoted.
    _run([python, "-c", "import clients, livewire_scripts"], cwd=dest)
    # Precompile before the tree goes read-only, or every run pays for a cold
    # bytecode cache it can no longer write.
    _run(
        [python, "-m", "compileall", "-q", "clients", "livewire_scripts", "scripts"],
        cwd=dest,
        check=False,
    )


def build_node_modules(dest: Path) -> None:
    """Install the Node alert dependencies into the release.

    `git archive` exports only tracked files and `node_modules/` is gitignored,
    so every release built since the artifact cutover shipped without
    nodemailer. The failure alert is the one message a broken nightly run
    depends on, and it could not send:

        Cannot find package 'nodemailer' imported from
          <release>/livewire_node/send_daily_update_failure_email.mjs

    Must run before `freeze`, which makes the tree read-only.

    This fails the promote rather than warning: `npm ci` needs the network, so
    a registry outage now blocks promotion where it previously succeeded
    silently. That is the correct trade — a release that cannot alert is
    precisely the failure this guards against, and `promote` keeps serving the
    previous release when it refuses to build.
    """
    if not (dest / "package.json").exists():
        return
    _run(["npm", "ci", "--omit=dev"], cwd=dest)
    # A release whose alert path cannot import is never promoted — the same
    # rule `build_venv` already applies to the Python tree.
    _run(["node", "--input-type=module", "-e", "import('nodemailer')"], cwd=dest)


def freeze(dest: Path) -> None:
    """Make the release tree read-only, so an accidental write cannot land."""
    _run(["chmod", "-R", "a-w", dest])


def _discard(path: Path) -> None:
    _run(["chmod", "-R", "u+w", path], check=False)
    shutil.rmtree(path, ignore_errors=True)


def current_sha() -> str | None:
    """Return the release ``current`` points at, or ``None`` if unset."""
    link = current_link()
    if not link.is_symlink():
        return None
    return Path(os.readlink(link)).name


def flip_current(sha: str) -> Path:
    """Atomically repoint ``current`` at a built release.

    A job that has already ``cd``-ed into ``current`` resolved it to a physical
    path, so it finishes against the release it started on.
    """
    target = releases_dir() / sha
    if not target.is_dir():
        raise ReleaseError(f"no such release: {target}")
    link = current_link()
    link.parent.mkdir(parents=True, exist_ok=True)
    staging = link.with_name(link.name + ".staging")
    staging.unlink(missing_ok=True)
    os.symlink(target, staging)
    os.replace(staging, link)
    return target


def _by_recency(root: Path) -> list[Path]:
    if not root.is_dir():
        return []
    return sorted(
        (path for path in root.iterdir() if path.is_dir()),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )


def prune(keep: int, *, dry_run: bool = False) -> list[str]:
    """Drop all but the ``keep`` newest releases, never the one being served.

    ``dry_run`` returns what would go without removing it — the housekeeping
    review this exists for is worthless if the one category that deletes
    422 MB at a time is invisible until --apply.
    """
    active = current_sha()
    removed = []
    for path in _by_recency(releases_dir())[keep:]:
        if path.name == active:
            continue
        if not dry_run:
            _discard(path)
            if path.exists():
                # `_discard` uses rmtree(ignore_errors=True), so returning the
                # name unconditionally would have housekeeping log "pruned
                # release X" over a directory still sitting on disk — the same
                # green-while-wrong shape its own delete loop refuses.
                LOGGER.warning("could not prune release %s — still on disk", path.name)
                continue
        removed.append(path.name)
    return removed


def promote(
    sha: str | None = None,
    *,
    keep: int = DEFAULT_KEEP,
    require_green: bool = True,
    dry_run: bool = False,
) -> int:
    """Build (if needed) and serve the release for ``sha``, defaulting to origin/main."""
    target_sha = sha or resolve_main_sha()
    if current_sha() == target_sha:
        LOGGER.info("current already at %s — nothing to promote", target_sha[:7])
        return 0

    dest = releases_dir() / target_sha
    if dest.is_dir():
        if dry_run:
            LOGGER.info("would repoint current at existing %s", dest)
            return 0
    else:
        if require_green and not ci_is_green(target_sha):
            # Fail safe: keep serving the previous release rather than an
            # unverified one. A red main must not reach production by timeout.
            LOGGER.warning(
                "CI is not green for %s — keeping %s",
                target_sha[:7],
                current_sha() or "<none>",
            )
            return 0
        if dry_run:
            LOGGER.info("would build %s", dest)
            return 0
        staging = dest.with_name(dest.name + ".building")
        if staging.exists():
            _discard(staging)
        export_tree(target_sha, staging)
        build_venv(staging)
        build_node_modules(staging)
        freeze(staging)
        # Only a fully built release ever appears under its bare SHA.
        staging.rename(dest)

    if not (warehouse_dir() / ".env").exists():
        LOGGER.warning(
            "%s is absent — a release carries no .env, so scheduled jobs would resolve every credential to nothing",
            warehouse_dir() / ".env",
        )
    flip_current(target_sha)
    LOGGER.info("current -> %s", target_sha)
    for name in prune(keep):
        LOGGER.info("pruned %s", name)
    return 0


def rollback(to: str | None = None) -> int:
    """Serve a previous release. Defaults to the most recent one that is not current."""
    active = current_sha()
    if to is None:
        to = next((path.name for path in _by_recency(releases_dir()) if path.name != active), None)
        if to is None:
            raise ReleaseError("no other release to roll back to")
    flip_current(to)
    LOGGER.info("current -> %s (rolled back from %s)", to, active or "<none>")
    return 0


def list_releases() -> int:
    """Print every built release, marking the one production is serving."""
    active = current_sha()
    entries = _by_recency(releases_dir())
    if not entries:
        print(f"no releases under {releases_dir()}")
        return 0
    for path in entries:
        print(f"{'*' if path.name == active else ' '} {path.name}")
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    parser = argparse.ArgumentParser(
        prog="livewire_release",
        description="Build and serve immutable release artifacts",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    promote_parser = sub.add_parser("promote", help="Build and serve origin/main")
    promote_parser.add_argument("--sha", help="Promote this commit instead of origin/main")
    promote_parser.add_argument("--keep", type=int, default=DEFAULT_KEEP)
    promote_parser.add_argument(
        "--allow-unverified",
        action="store_true",
        help="Skip the CI-green gate (needed once, to bootstrap the first release)",
    )
    promote_parser.add_argument("--dry-run", action="store_true")

    rollback_parser = sub.add_parser("rollback", help="Serve a previous release")
    rollback_parser.add_argument("--to", help="Release SHA to serve")

    sub.add_parser("list", help="List built releases")

    gc_parser = sub.add_parser("gc", help="Drop all but the newest releases")
    gc_parser.add_argument("--keep", type=int, default=DEFAULT_KEEP)

    args = parser.parse_args(argv)
    logging.basicConfig(
        level=os.environ.get("MDW_LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(message)s",
    )

    try:
        if args.command == "promote":
            return promote(
                args.sha,
                keep=args.keep,
                require_green=not args.allow_unverified,
                dry_run=args.dry_run,
            )
        if args.command == "rollback":
            return rollback(args.to)
        if args.command == "list":
            return list_releases()
        for name in prune(args.keep):
            LOGGER.info("pruned %s", name)
        return 0
    except ReleaseError as exc:
        LOGGER.error("%s", exc)
        return 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
