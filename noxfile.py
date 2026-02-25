"""Nox configuration."""

from __future__ import annotations

import os
import sys
from textwrap import dedent

import nox

try:
    from nox_poetry import Session, session
except ImportError:
    message = f"""\
    Nox failed to import the 'nox-poetry' package.
    Please install it using the following command:
    {sys.executable} -m pip install nox-poetry"""
    raise SystemExit(dedent(message)) from None

GITHUB_ACTIONS = "GITHUB_ACTIONS"
PYPROJECT = nox.project.load_toml("pyproject.toml")
# PYTHON_VERSIONS = nox.project.python_versions(PYPROJECT)  # TODO: Add PEP 621 project classifiers  # noqa: E501, ERA001

src_dir = "tap_criteo"
tests_dir = "tests"

nox.needs_version = ">=2025.2.9"
nox.options.default_venv_backend = "uv"
nox.options.sessions = (
    "mypy",
    "tests",
)

python_versions = [
    "3.14",
    "3.13",
    "3.12",
    "3.11",
    "3.10",
]
main_python_version = "3.14"
locations = src_dir, tests_dir, "noxfile.py"


@session(python=main_python_version)
def mypy(session: Session) -> None:
    """Check types with mypy."""
    args = session.posargs or [src_dir, tests_dir]
    session.install(".")
    session.install(
        "mypy",
        "types-python-dateutil",
        "types-requests",
    )
    session.run("mypy", *args)
    if not session.posargs:
        session.run("mypy", f"--python-executable={sys.executable}", "noxfile.py")


@session(python=python_versions)
def tests(session: Session) -> None:
    """Execute pytest tests and compute coverage."""
    deps = ["pytest"]
    if GITHUB_ACTIONS in os.environ:
        deps.append("pytest-github-actions-annotate-failures")

    session.install(".")
    session.install(*deps)
    session.run("pytest", *session.posargs)
