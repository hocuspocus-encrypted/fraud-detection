"""
Postgres connection helper shared by the migrator, the enqueuer and tests.
Settings come from .env (template: .env.example).
"""
import os
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")

# Roles created by db/migrations/0002_roles.sql (analyst: 0013_analyst.sql) -> the .env
# variable holding each password.
ROLE_PASSWORD_VARS = {
    "pipeline_writer": "PIPELINE_WRITER_PASSWORD",
    "worker": "WORKER_PASSWORD",
    "agent_ro": "AGENT_RO_PASSWORD",
    "analyst": "ANALYST_PASSWORD",
}


def connect_kwargs(role: str = "admin", dbname: str | None = None) -> dict:
    """psycopg2.connect() keyword arguments for the compose superuser ("admin")
    or one of ROLE_PASSWORD_VARS. Also usable with psycopg2.pool."""
    if role == "admin":
        user = os.getenv("ADMIN_USER", "fraud_user")
        password = os.getenv("ADMIN_PASSWORD", "fraud_pass")
    else:
        var = ROLE_PASSWORD_VARS[role]
        user, password = role, os.getenv(var)
        if not password:
            raise RuntimeError(f"{var} is not set in .env")
    return {
        "host": os.getenv("PGHOST", "localhost"),
        "port": int(os.getenv("PGPORT", "5432")),
        "dbname": dbname or os.getenv("PGDATABASE", "fraud_db"),
        "user": user,
        "password": password,
    }


def connect(role: str = "admin", dbname: str | None = None):
    return psycopg2.connect(**connect_kwargs(role, dbname))
