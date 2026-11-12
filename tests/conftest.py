"""
Shared test setup: the same results from the repository root or from tests/, and no
database settings taken from the developer's shell.
"""
import os
from pathlib import Path

import pytest

TESTS_DIR = Path(__file__).parent


def pytest_configure(config):
    # Test files build paths like ../samples/sample.zip from the working folder
    os.chdir(TESTS_DIR)


@pytest.fixture(autouse=True)
def _no_database_settings_from_shell(monkeypatch, tmp_path):
    """
    UNICORN_* variables override the .env file (db_connector.load_env), so a value set in the
    shell would change what a test reads; nor is the developer's per-user .env (pycorn-setup)
    read. Live tests still use the checkout's sql/.env.
    """
    from pycorn import db_connector
    for key in list(os.environ):
        if key.upper().startswith("UNICORN_") or key.upper() == "PYCORN_ENV_FILE":
            monkeypatch.delenv(key)
    monkeypatch.setattr(db_connector, "user_env_file", lambda: tmp_path / "user_config" / "pycorn" / ".env")
