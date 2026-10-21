"""
Applies db/migrations/*.sql in filename order, each once, each in its own
transaction, recording it in schema_migrations. Then gives the roles from
0002_roles.sql LOGIN with their passwords from .env.

Usage (from the repo root):
    venv\\Scripts\\python.exe -m db.migrate
    venv\\Scripts\\python.exe -m db.migrate --dbname some_other_db
"""
import argparse
import hashlib
import os
from pathlib import Path

from db import ROLE_PASSWORD_VARS, connect

MIGRATIONS_DIR = Path(__file__).resolve().parent / "migrations"


def checksum(sql: str) -> str:
    # Normalize line endings so a CRLF checkout doesn't look like an edited migration.
    return hashlib.sha256(sql.replace("\r\n", "\n").encode("utf-8")).hexdigest()


def recreate_database(dbname: str) -> None:
    """Drop and create an empty database. Used for throwaway test/eval databases."""
    if dbname == os.getenv("PGDATABASE", "fraud_db"):
        raise ValueError(f"refusing to recreate the main database {dbname!r}")
    admin = connect("admin", "postgres")
    admin.autocommit = True
    try:
        with admin.cursor() as cur:
            cur.execute(f'DROP DATABASE IF EXISTS "{dbname}" WITH (FORCE)')
            cur.execute(f'CREATE DATABASE "{dbname}"')
    finally:
        admin.close()


def main():
    parser = argparse.ArgumentParser(description="Apply pending SQL migrations.")
    parser.add_argument("--dbname", default=None, help="Database to migrate (default: PGDATABASE from .env).")
    args = parser.parse_args()
    migrate(args.dbname)


def migrate(dbname: str | None = None) -> int:
    """Apply pending migrations to dbname; returns how many were applied."""
    conn = connect("admin", dbname)
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                """CREATE TABLE IF NOT EXISTS schema_migrations (
                       version     TEXT PRIMARY KEY,
                       checksum    TEXT NOT NULL,
                       applied_at  TIMESTAMPTZ NOT NULL DEFAULT now()
                   )"""
            )

        applied = 0
        for path in sorted(MIGRATIONS_DIR.glob("*.sql")):
            version, sql = path.stem, path.read_text(encoding="utf-8")
            # `with conn` commits on success and rolls back on any error.
            with conn, conn.cursor() as cur:
                cur.execute("SELECT checksum FROM schema_migrations WHERE version = %s", (version,))
                row = cur.fetchone()
                if row:
                    if row[0] != checksum(sql):
                        raise SystemExit(
                            f"{version} changed after it was applied. Add a new migration instead of editing it."
                        )
                    continue
                cur.execute(sql)
                cur.execute(
                    "INSERT INTO schema_migrations (version, checksum) VALUES (%s, %s)",
                    (version, checksum(sql)),
                )
            applied += 1
            print(f"applied {version}")

        with conn, conn.cursor() as cur:
            for role, var in ROLE_PASSWORD_VARS.items():
                password = os.getenv(var)
                if password:
                    cur.execute(f"ALTER ROLE {role} WITH LOGIN PASSWORD %s", (password,))
                else:
                    print(f"warning: {var} not set, {role} stays NOLOGIN")

        print(f"applied {applied} migration(s)")
        return applied
    finally:
        conn.close()


if __name__ == "__main__":
    main()
