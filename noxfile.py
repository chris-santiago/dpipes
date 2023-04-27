import os

import nox

os.environ.update({"PDM_IGNORE_SAVED_PYTHON": "1"})


@nox.session(reuse_venv=True)
def tests(session):
    session.run("pdm", "install", "-dG", "test", external=True)
    session.run("pytest")


@nox.session(reuse_venv=True)
def lint(session):
    session.run("pdm", "install", "-dG", "qa", external=True)
    session.run("ruff", "dpipes")


@nox.session(reuse_venv=True)
def type_check(session):
    session.run("pdm", "install", "-dG", "qa", external=True)
    session.run("pyright", "dpipes")
