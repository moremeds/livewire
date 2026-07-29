"""Tests for the immutable-release artifact builder.

Every subprocess is mocked: these tests never touch git, gh, uv, or the network.
"""

from __future__ import annotations

import json
import os
import tarfile
from pathlib import Path

import pytest

from livewire_scripts import release


@pytest.fixture
def warehouse(tmp_path, monkeypatch):
    """Point releases/ and current at a disposable tree."""
    releases = tmp_path / "releases"
    releases.mkdir()
    monkeypatch.setenv("MDW_RELEASES_DIR", str(releases))
    monkeypatch.setenv("MDW_CURRENT_LINK", str(tmp_path / "current"))
    monkeypatch.setenv("MDW_WAREHOUSE_DIR", str(tmp_path))
    return tmp_path


def make_release(warehouse: Path, sha: str, mtime: float) -> Path:
    path = warehouse / "releases" / sha
    path.mkdir()
    os.utime(path, (mtime, mtime))
    return path


# --- path resolution ---------------------------------------------------------


def test_paths_follow_env_overrides(warehouse):
    assert release.releases_dir() == warehouse / "releases"
    assert release.current_link() == warehouse / "current"


def test_paths_default_under_the_warehouse(tmp_path, monkeypatch):
    monkeypatch.delenv("MDW_RELEASES_DIR", raising=False)
    monkeypatch.delenv("MDW_CURRENT_LINK", raising=False)
    monkeypatch.setenv("MDW_WAREHOUSE_DIR", str(tmp_path))

    assert release.releases_dir() == tmp_path / "releases"
    assert release.current_link() == tmp_path / "current"


# --- the subprocess seam -----------------------------------------------------


def test_run_returns_stdout():
    assert release._run(["echo", "hello"]).strip() == "hello"


def test_run_raises_release_error_on_failure():
    with pytest.raises(release.ReleaseError, match=r"failed \(1\)"):
        release._run(["sh", "-c", "echo boom >&2; exit 1"])


def test_run_can_tolerate_failure():
    assert release._run(["sh", "-c", "exit 1"], check=False) == ""


# --- git / gh interrogation --------------------------------------------------


def test_resolve_main_sha_fetches_then_reads(monkeypatch):
    calls = []

    def fake_run(cmd, cwd=None, check=True):
        calls.append(list(map(str, cmd)))
        return "deadbeef\n"

    monkeypatch.setattr(release, "_run", fake_run)

    assert release.resolve_main_sha() == "deadbeef"
    assert calls[0][:2] == ["git", "fetch"]
    assert calls[1][-1] == "origin/main"


def test_resolve_main_sha_can_skip_the_fetch(monkeypatch):
    calls = []

    def fake_run(cmd, cwd=None, check=True):
        calls.append(list(map(str, cmd)))
        return "cafe\n"

    monkeypatch.setattr(release, "_run", fake_run)

    assert release.resolve_main_sha(fetch=False) == "cafe"
    assert len(calls) == 1


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        ('[{"status": "completed", "conclusion": "success"}]', True),
        ('[{"status": "completed", "conclusion": "failure"}]', False),
        ('[{"status": "in_progress", "conclusion": null}]', False),
        ("[]", False),
        ("", False),
    ],
)
def test_ci_is_green_reads_the_run_list(monkeypatch, payload, expected):
    monkeypatch.setattr(release, "_run", lambda cmd, cwd=None, check=True: payload)

    assert release.ci_is_green("deadbeef") is expected


def test_ci_is_green_asks_for_this_exact_commit(monkeypatch):
    seen = {}

    def fake_run(cmd, cwd=None, check=True):
        seen["argv"] = list(map(str, cmd))
        return json.dumps([])

    monkeypatch.setattr(release, "_run", fake_run)
    release.ci_is_green("abc123")

    assert "--commit" in seen["argv"]
    assert seen["argv"][seen["argv"].index("--commit") + 1] == "abc123"
    assert release.CI_WORKFLOW in seen["argv"]


# --- building ----------------------------------------------------------------


def test_export_tree_extracts_the_archive(tmp_path, monkeypatch):
    source = tmp_path / "src"
    (source / "clients").mkdir(parents=True)
    (source / "clients" / "thing.py").write_text("x = 1\n")

    def fake_run(cmd, cwd=None, check=True):
        output = next(str(part) for part in cmd if str(part).startswith("--output="))
        with tarfile.open(output.removeprefix("--output="), "w") as tar:
            tar.add(source, arcname=".")
        return ""

    monkeypatch.setattr(release, "_run", fake_run)
    dest = tmp_path / "dest"
    release.export_tree("deadbeef", dest)

    assert (dest / "clients" / "thing.py").read_text() == "x = 1\n"


def test_build_venv_runs_uv_then_smoke_tests_the_tree(tmp_path, monkeypatch):
    dest = tmp_path / "rel"
    dest.mkdir()
    calls = []

    def fake_run(cmd, cwd=None, check=True):
        argv = list(map(str, cmd))
        calls.append(argv)
        if argv[0] == "uv":
            python = dest / ".venv" / "bin" / "python"
            python.parent.mkdir(parents=True)
            python.touch()
        return ""

    monkeypatch.setattr(release, "_run", fake_run)
    release.build_venv(dest)

    assert calls[0] == ["uv", "sync", "--frozen", "--no-dev"]
    assert calls[1][-1] == "import clients, livewire_scripts"
    assert "compileall" in calls[2]


def test_build_venv_fails_when_uv_produces_no_interpreter(tmp_path, monkeypatch):
    dest = tmp_path / "rel"
    dest.mkdir()
    monkeypatch.setattr(release, "_run", lambda cmd, cwd=None, check=True: "")

    with pytest.raises(release.ReleaseError, match="no interpreter"):
        release.build_venv(dest)


def test_build_node_modules_installs_the_alert_dependencies(tmp_path, monkeypatch):
    """`git archive` exports only tracked files and node_modules/ is gitignored,
    so every release since the artifact cutover shipped without nodemailer and
    the failure alert could not send."""
    dest = tmp_path / "rel"
    dest.mkdir()
    (dest / "package.json").write_text('{"dependencies": {"nodemailer": "8.0.2"}}')
    calls = []
    monkeypatch.setattr(release, "_run", lambda cmd, cwd=None, check=True: calls.append(list(map(str, cmd))))

    release.build_node_modules(dest)

    assert calls[0] == ["npm", "ci", "--omit=dev"]
    assert "nodemailer" in calls[1][-1]


def test_build_node_modules_is_a_noop_without_package_json(tmp_path, monkeypatch):
    dest = tmp_path / "rel"
    dest.mkdir()
    calls = []
    monkeypatch.setattr(release, "_run", lambda cmd, cwd=None, check=True: calls.append(cmd))

    release.build_node_modules(dest)

    assert calls == []


def test_node_modules_are_installed_before_the_tree_is_frozen():
    """`freeze` chmods the tree a-w; npm cannot write after that.

    The promote path's local is `staging`, not `dest` — match the call site.
    """
    import inspect

    source = inspect.getsource(release)
    assert source.index("build_node_modules(staging)") < source.index("freeze(staging)")


def test_freeze_strips_write_permission(tmp_path, monkeypatch):
    seen = {}
    monkeypatch.setattr(release, "_run", lambda cmd, cwd=None, check=True: seen.update(argv=list(map(str, cmd))))
    release.freeze(tmp_path)

    assert seen["argv"][:3] == ["chmod", "-R", "a-w"]


# --- the current symlink -----------------------------------------------------


def test_current_sha_is_none_without_a_link(warehouse):
    assert release.current_sha() is None


def test_flip_current_points_at_the_release(warehouse):
    make_release(warehouse, "aaa111", 1_000.0)
    release.flip_current("aaa111")

    assert release.current_sha() == "aaa111"
    assert (warehouse / "current" / ".").exists()


def test_flip_current_replaces_an_existing_link(warehouse):
    make_release(warehouse, "aaa111", 1_000.0)
    make_release(warehouse, "bbb222", 2_000.0)
    release.flip_current("aaa111")
    release.flip_current("bbb222")

    assert release.current_sha() == "bbb222"
    assert not (warehouse / "current.staging").exists()


def test_flip_current_rejects_a_release_that_was_never_built(warehouse):
    with pytest.raises(release.ReleaseError, match="no such release"):
        release.flip_current("missing")


# --- pruning -----------------------------------------------------------------


def test_prune_keeps_the_newest_and_drops_the_rest(warehouse):
    for index, sha in enumerate(["old", "mid", "new"]):
        make_release(warehouse, sha, 1_000.0 + index)

    assert release.prune(keep=2) == ["old"]
    assert not (warehouse / "releases" / "old").exists()
    assert (warehouse / "releases" / "new").exists()


def test_prune_never_drops_the_release_being_served(warehouse):
    for index, sha in enumerate(["served", "mid", "new"]):
        make_release(warehouse, sha, 1_000.0 + index)
    release.flip_current("served")

    assert release.prune(keep=2) == []
    assert (warehouse / "releases" / "served").exists()


def test_prune_is_a_noop_without_a_releases_directory(tmp_path, monkeypatch):
    monkeypatch.setenv("MDW_RELEASES_DIR", str(tmp_path / "absent"))
    monkeypatch.setenv("MDW_CURRENT_LINK", str(tmp_path / "current"))

    assert release.prune(keep=2) == []


# --- promote -----------------------------------------------------------------


@pytest.fixture
def no_build(monkeypatch):
    """Stub the three expensive build steps; export creates the staging dir."""
    monkeypatch.setattr(release, "export_tree", lambda sha, dest: dest.mkdir(parents=True))
    monkeypatch.setattr(release, "build_venv", lambda dest: None)
    monkeypatch.setattr(release, "freeze", lambda dest: None)


def test_promote_is_a_noop_when_current_already_serves_the_sha(warehouse, monkeypatch, caplog):
    make_release(warehouse, "aaa111", 1_000.0)
    release.flip_current("aaa111")
    monkeypatch.setattr(release, "resolve_main_sha", lambda: "aaa111")

    with caplog.at_level("INFO"):
        assert release.promote() == 0
    assert "nothing to promote" in caplog.text


def test_promote_builds_and_serves_a_green_commit(warehouse, monkeypatch, no_build):
    monkeypatch.setattr(release, "resolve_main_sha", lambda: "aaa111")
    monkeypatch.setattr(release, "ci_is_green", lambda sha: True)
    (warehouse / ".env").touch()

    assert release.promote() == 0
    assert release.current_sha() == "aaa111"
    assert not (warehouse / "releases" / "aaa111.building").exists()


def test_promote_refuses_a_commit_ci_has_not_passed(warehouse, monkeypatch, no_build, caplog):
    make_release(warehouse, "old", 1_000.0)
    release.flip_current("old")
    monkeypatch.setattr(release, "resolve_main_sha", lambda: "unverified")
    monkeypatch.setattr(release, "ci_is_green", lambda sha: False)

    with caplog.at_level("WARNING"):
        assert release.promote() == 0
    assert release.current_sha() == "old"
    assert "CI is not green" in caplog.text


def test_promote_can_bypass_the_ci_gate_to_bootstrap(warehouse, monkeypatch, no_build):
    monkeypatch.setattr(release, "resolve_main_sha", lambda: "aaa111")

    def explode(sha):  # pragma: no cover - must never be consulted
        raise AssertionError("CI must not be queried when unverified builds are allowed")

    monkeypatch.setattr(release, "ci_is_green", explode)

    assert release.promote(require_green=False) == 0
    assert release.current_sha() == "aaa111"


def test_promote_warns_when_the_warehouse_env_file_is_missing(warehouse, monkeypatch, no_build, caplog):
    monkeypatch.setattr(release, "resolve_main_sha", lambda: "aaa111")
    monkeypatch.setattr(release, "ci_is_green", lambda sha: True)

    with caplog.at_level("WARNING"):
        release.promote()
    assert "resolve every credential to nothing" in caplog.text


def test_promote_discards_a_half_built_staging_tree(warehouse, monkeypatch, no_build):
    stale = warehouse / "releases" / "aaa111.building"
    stale.mkdir()
    (stale / "junk").write_text("partial")
    monkeypatch.setattr(release, "resolve_main_sha", lambda: "aaa111")
    monkeypatch.setattr(release, "ci_is_green", lambda sha: True)

    assert release.promote() == 0
    assert not (warehouse / "releases" / "aaa111" / "junk").exists()


def test_promote_reuses_a_release_that_is_already_built(warehouse, monkeypatch):
    make_release(warehouse, "aaa111", 1_000.0)

    def explode(sha):  # pragma: no cover - a built release needs no CI lookup
        raise AssertionError("an already-built release must not be re-gated")

    monkeypatch.setattr(release, "resolve_main_sha", lambda: "aaa111")
    monkeypatch.setattr(release, "ci_is_green", explode)

    assert release.promote() == 0
    assert release.current_sha() == "aaa111"


def test_promote_prunes_after_flipping(warehouse, monkeypatch, no_build):
    for index, sha in enumerate(["one", "two", "three"]):
        make_release(warehouse, sha, 1_000.0 + index)
    monkeypatch.setattr(release, "resolve_main_sha", lambda: "aaa111")
    monkeypatch.setattr(release, "ci_is_green", lambda sha: True)

    assert release.promote(keep=2) == 0
    assert release.current_sha() == "aaa111"
    assert not (warehouse / "releases" / "one").exists()


def test_promote_dry_run_builds_nothing(warehouse, monkeypatch, caplog):
    monkeypatch.setattr(release, "resolve_main_sha", lambda: "aaa111")
    monkeypatch.setattr(release, "ci_is_green", lambda sha: True)

    with caplog.at_level("INFO"):
        assert release.promote(dry_run=True) == 0
    assert not (warehouse / "releases" / "aaa111").exists()
    assert release.current_sha() is None
    assert "would build" in caplog.text


def test_promote_dry_run_reports_an_existing_release(warehouse, monkeypatch, caplog):
    make_release(warehouse, "aaa111", 1_000.0)
    monkeypatch.setattr(release, "resolve_main_sha", lambda: "aaa111")

    with caplog.at_level("INFO"):
        assert release.promote(dry_run=True) == 0
    assert release.current_sha() is None
    assert "would repoint" in caplog.text


# --- rollback / list ---------------------------------------------------------


def test_rollback_serves_the_previous_release(warehouse):
    make_release(warehouse, "old", 1_000.0)
    make_release(warehouse, "new", 2_000.0)
    release.flip_current("new")

    assert release.rollback() == 0
    assert release.current_sha() == "old"


def test_rollback_honours_an_explicit_target(warehouse):
    make_release(warehouse, "first", 1_000.0)
    make_release(warehouse, "second", 2_000.0)
    make_release(warehouse, "third", 3_000.0)
    release.flip_current("third")

    assert release.rollback(to="first") == 0
    assert release.current_sha() == "first"


def test_rollback_fails_when_there_is_nothing_to_fall_back_to(warehouse):
    make_release(warehouse, "only", 1_000.0)
    release.flip_current("only")

    with pytest.raises(release.ReleaseError, match="no other release"):
        release.rollback()


def test_list_releases_marks_the_served_one(warehouse, capsys):
    make_release(warehouse, "old", 1_000.0)
    make_release(warehouse, "new", 2_000.0)
    release.flip_current("old")

    assert release.list_releases() == 0
    out = capsys.readouterr().out

    assert "* old" in out
    assert "  new" in out


def test_list_releases_reports_an_empty_tree(warehouse, capsys):
    assert release.list_releases() == 0
    assert "no releases under" in capsys.readouterr().out


# --- CLI ---------------------------------------------------------------------


def test_main_dispatches_promote(warehouse, monkeypatch):
    seen = {}
    monkeypatch.setattr(
        release,
        "promote",
        lambda sha, keep, require_green, dry_run: (
            seen.update(sha=sha, keep=keep, require_green=require_green, dry_run=dry_run) or 0
        ),
    )

    assert release.main(["promote", "--sha", "abc", "--keep", "5", "--allow-unverified"]) == 0
    assert seen == {"sha": "abc", "keep": 5, "require_green": False, "dry_run": False}


def test_main_dispatches_rollback(warehouse):
    make_release(warehouse, "old", 1_000.0)
    make_release(warehouse, "new", 2_000.0)
    release.flip_current("new")

    assert release.main(["rollback"]) == 0
    assert release.current_sha() == "old"


def test_main_dispatches_list(warehouse, capsys):
    assert release.main(["list"]) == 0
    assert "no releases under" in capsys.readouterr().out


def test_main_dispatches_gc(warehouse):
    for index, sha in enumerate(["one", "two"]):
        make_release(warehouse, sha, 1_000.0 + index)

    assert release.main(["gc", "--keep", "1"]) == 0
    assert not (warehouse / "releases" / "one").exists()


def test_main_reports_a_release_error_as_exit_one(warehouse, caplog):
    with caplog.at_level("ERROR"):
        assert release.main(["rollback"]) == 1
    assert "no other release" in caplog.text
