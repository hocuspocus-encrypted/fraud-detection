"""
docs/PLAN_HITL.md Phase H0: housekeeping checks.

    venv\\Scripts\\python.exe -m pytest tests -q
"""
import subprocess

from db import ROOT


def git(*args) -> str:
    return subprocess.run(["git", *args], cwd=ROOT, capture_output=True, text=True, check=True).stdout


def test_spark_checkpoints_are_not_tracked():
    assert git("ls-files", "checkpoints") == ""


def test_checkpoints_stay_ignored():
    assert git("check-ignore", "checkpoints/transactions_to_postgres/metadata").strip()


def test_claude_md_and_hitl_plan_are_in_the_repo():
    assert (ROOT / "CLAUDE.md").is_file()
    assert (ROOT / "docs" / "PLAN_HITL.md").is_file()
