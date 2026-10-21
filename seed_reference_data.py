"""
seed_reference_data.py

Writes the generator's reference data to Postgres (PLAN.md Phase 4), as the
pipeline_writer role:
  - merchant_risk: every catalog merchant except the ~15% deliberately left out
  - account_profiles: one row per generated customer, source = 'generator'
    (stands in for KYC data; refresh_account_profiles() never overwrites these)

Deterministic from the same seed and user count as the producer, so run it
once before producing (and again only if you change --seed or --users).
Safe to re-run.

Usage:
    venv\\Scripts\\python.exe seed_reference_data.py
    venv\\Scripts\\python.exe seed_reference_data.py --seed 42 --users 1000
"""
import argparse

from db import connect
from generate_transactions import DEFAULT_SEED, DEFAULT_USERS, build_catalog, build_profiles


def seed_reference_data(seed: int = DEFAULT_SEED, n_users: int = DEFAULT_USERS,
                        dbname: str | None = None) -> tuple[int, int]:
    catalog = build_catalog(seed)
    profiles = build_profiles(seed, catalog, n_users)
    in_table = [m for m in catalog if m.in_risk_table]

    conn = connect("pipeline_writer", dbname)
    try:
        with conn, conn.cursor() as cur:
            cur.executemany(
                """INSERT INTO merchant_risk (merchant, risk_level, reason, updated_at)
                   VALUES (%s, %s, %s, now())
                   ON CONFLICT (merchant) DO UPDATE
                       SET risk_level = EXCLUDED.risk_level, reason = EXCLUDED.reason, updated_at = now()""",
                [(m.name, m.risk_level, m.reason) for m in in_table],
            )
            cur.executemany(
                """INSERT INTO account_profiles
                       (user_id, home_city, account_opened_at, avg_monthly_spend, source, updated_at)
                   VALUES (%s, %s, %s, %s, 'generator', now())
                   ON CONFLICT (user_id) DO UPDATE
                       SET home_city = EXCLUDED.home_city, account_opened_at = EXCLUDED.account_opened_at,
                           avg_monthly_spend = EXCLUDED.avg_monthly_spend, source = 'generator',
                           updated_at = now()""",
                [(p.user_id, p.home_city, p.opened_at, p.avg_monthly_spend) for p in profiles],
            )
    finally:
        conn.close()
    return len(in_table), len(profiles)


def main():
    parser = argparse.ArgumentParser(description="Write merchant_risk and account_profiles for the generator.")
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--users", type=int, default=DEFAULT_USERS)
    parser.add_argument("--dbname", default=None, help="Default: PGDATABASE from .env.")
    args = parser.parse_args()
    merchants, profiles = seed_reference_data(args.seed, args.users, args.dbname)
    print(f"merchant_risk: {merchants} merchants; account_profiles: {profiles} customers (seed {args.seed})")


if __name__ == "__main__":
    main()
