"""Nox configuration."""

from __future__ import annotations

import sys

import nox

GITHUB_ACTIONS = "GITHUB_ACTIONS"
PYPROJECT = nox.project.load_toml("pyproject.toml")
PYTHON_VERSIONS = nox.project.python_versions(PYPROJECT)

src_dir = "tap_criteo"
tests_dir = "tests"

locations = src_dir, tests_dir, "noxfile.py"
nox.needs_version = ">=2025.2.9"
nox.options.default_venv_backend = "uv"
nox.options.sessions = (
    "mypy",
    "tests",
)

UV_SYNC_COMMAND = (
    "uv",
    "sync",
    "--locked",
    "--no-dev",
)


@nox.session()
def mypy(session: nox.Session) -> None:
    """Check types with mypy."""
    env = {
        "UV_PROJECT_ENVIRONMENT": session.virtualenv.location,
    }
    if isinstance(session.python, str):
        env["UV_PYTHON"] = session.python

    session.run_install(
        *UV_SYNC_COMMAND,
        "--group=typing",
        env=env,
    )
    args = session.posargs or [src_dir, tests_dir]
    session.run("mypy", *args)
    session.run("ty", "check", src_dir, tests_dir)
    if not session.posargs:
        session.run("mypy", f"--python-executable={sys.executable}", "noxfile.py")


@nox.session(python=PYTHON_VERSIONS)
def tests(session: nox.Session) -> None:
    """Execute pytest tests and compute coverage."""
    env = {
        "UV_PROJECT_ENVIRONMENT": session.virtualenv.location,
    }
    if isinstance(session.python, str):
        env["UV_PYTHON"] = session.python

    session.run_install(
        *UV_SYNC_COMMAND,
        "--group=ci",
        env=env,
    )
    session.run("pytest", *session.posargs)
