"""
Creates a SQLite database with sample accounts and transactions so the
agent has real data to investigate. Swap this out for your Postgres
pipeline later -- the tool functions in tools.py are the only thing
that need to change.

Accounts acc_001 / acc_002 are the original two demo cases (used by
main.py). acc_003 .. acc_015 were added for the eval set (eval_cases.json)
and cover a spread of fraud patterns AND legitimate-but-weird behavior,
because an agent that flags everything unusual is as useless as one that
flags nothing.
"""
import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fraud_demo.db")

ACCOUNTS = [
    # account_id, home_city, avg_monthly_spend, account_age_days
    ("acc_001", "Toronto", 850.00, 620),
    ("acc_002", "Muscat", 400.00, 45),
    ("acc_003", "Toronto", 1200.00, 900),
    ("acc_004", "Toronto", 2000.00, 1500),
    ("acc_005", "Toronto", 600.00, 300),
    ("acc_006", "Muscat", 500.00, 20),
    ("acc_007", "Toronto", 3000.00, 2000),
    ("acc_008", "Toronto", 700.00, 400),
    ("acc_009", "Toronto", 900.00, 800),
    ("acc_010", "Toronto", 1000.00, 700),
    ("acc_011", "Muscat", 450.00, 600),
    ("acc_012", "Toronto", 800.00, 100),
    ("acc_013", "Toronto", 1500.00, 1000),
    ("acc_014", "Toronto", 500.00, 30),
    ("acc_015", "Lagos", 700.00, 500),
]

# txn_id, account_id, timestamp, amount, merchant, city, flagged
TRANSACTIONS = [
    # ---- acc_001: normal Toronto history, then one weird flagged txn (original demo case)
    ("t1", "acc_001", "2026-09-01T09:12:00", 42.50, "Loblaws", "Toronto", 0),
    ("t2", "acc_001", "2026-09-03T18:45:00", 65.00, "Shell", "Toronto", 0),
    ("t3", "acc_001", "2026-09-05T12:00:00", 120.00, "Amazon", "Toronto", 0),
    ("t4", "acc_001", "2026-09-09T02:14:00", 1980.00, "QuickGoldExchange", "Lagos", 1),

    # ---- acc_002: newer account, legit-looking pattern, borderline flagged txn (original demo case)
    ("t5", "acc_002", "2026-09-02T10:00:00", 55.00, "Carrefour", "Muscat", 0),
    ("t6", "acc_002", "2026-09-06T14:30:00", 90.00, "Lulu Hypermarket", "Muscat", 0),
    ("t7", "acc_002", "2026-09-09T20:05:00", 310.00, "Carrefour", "Muscat", 1),

    # ---- acc_003: card-testing burst (tiny charges in rapid succession, then a big one)
    ("c03_1", "acc_003", "2026-09-02T12:10:00", 38.20, "Loblaws", "Toronto", 0),
    ("c03_2", "acc_003", "2026-09-05T19:30:00", 72.00, "Shell", "Toronto", 0),
    ("c03_3", "acc_003", "2026-09-09T03:01:10", 1.00, "GiftCardHub", "Online", 0),
    ("c03_4", "acc_003", "2026-09-09T03:01:45", 1.00, "PrepaidPlus", "Online", 0),
    ("c03_5", "acc_003", "2026-09-09T03:02:20", 2.00, "GiftCardHub", "Online", 0),
    ("c03_6", "acc_003", "2026-09-09T03:03:05", 5.00, "PrepaidPlus", "Online", 0),
    ("c03_7", "acc_003", "2026-09-09T03:04:00", 250.00, "GiftCardHub", "Online", 1),

    # ---- acc_004: legitimate international travel (flight booked days earlier)
    ("c04_1", "acc_004", "2026-09-01T10:00:00", 85.00, "Loblaws", "Toronto", 0),
    ("c04_2", "acc_004", "2026-09-03T14:20:00", 140.00, "Costco", "Toronto", 0),
    ("c04_3", "acc_004", "2026-09-04T09:15:00", 1150.00, "AirCanada", "Toronto", 0),
    ("c04_4", "acc_004", "2026-09-07T16:40:00", 220.00, "HotelLumiere", "Paris", 1),

    # ---- acc_005: impossible travel (Toronto -> Singapore in 90 minutes), low-risk merchant
    ("c05_1", "acc_005", "2026-09-02T08:30:00", 6.50, "Starbucks", "Toronto", 0),
    ("c05_2", "acc_005", "2026-09-04T12:00:00", 45.00, "Loblaws", "Toronto", 0),
    ("c05_3", "acc_005", "2026-09-08T10:00:00", 4.75, "Starbucks", "Toronto", 0),
    ("c05_4", "acc_005", "2026-09-08T11:30:00", 52.00, "Starbucks", "Singapore", 1),

    # ---- acc_006: brand-new account, big first-week purchase at a merchant with no risk data
    ("c06_1", "acc_006", "2026-09-01T13:00:00", 35.00, "Carrefour", "Muscat", 0),
    ("c06_2", "acc_006", "2026-09-03T18:00:00", 60.00, "Lulu Hypermarket", "Muscat", 0),
    ("c06_3", "acc_006", "2026-09-08T21:30:00", 1500.00, "ElectroMart", "Muscat", 1),

    # ---- acc_007: large but perfectly regular (monthly rent) -- should NOT be flagged on amount alone
    ("c07_1", "acc_007", "2026-07-01T09:00:00", 1250.00, "PropertyMgmtCo", "Toronto", 0),
    ("c07_2", "acc_007", "2026-08-01T09:00:00", 1250.00, "PropertyMgmtCo", "Toronto", 0),
    ("c07_3", "acc_007", "2026-09-01T09:00:00", 1250.00, "PropertyMgmtCo", "Toronto", 0),
    ("c07_4", "acc_007", "2026-09-03T17:30:00", 95.00, "Loblaws", "Toronto", 0),
    ("c07_5", "acc_007", "2026-10-01T09:00:00", 1250.00, "PropertyMgmtCo", "Toronto", 1),

    # ---- acc_008: tiny charge at a high-risk merchant (classic card-testing probe)
    ("c08_1", "acc_008", "2026-09-02T11:00:00", 55.00, "Loblaws", "Toronto", 0),
    ("c08_2", "acc_008", "2026-09-05T13:00:00", 30.00, "Shell", "Toronto", 0),
    ("c08_3", "acc_008", "2026-09-09T04:12:00", 4.99, "GiftCardHub", "Online", 1),

    # ---- acc_009: ordinary flagged txn (model false positive) -- routine coffee-shop customer
    ("c09_1", "acc_009", "2026-09-01T08:05:00", 5.25, "TimHortons", "Toronto", 0),
    ("c09_2", "acc_009", "2026-09-02T08:10:00", 5.25, "TimHortons", "Toronto", 0),
    ("c09_3", "acc_009", "2026-09-04T08:07:00", 6.10, "TimHortons", "Toronto", 0),
    ("c09_4", "acc_009", "2026-09-06T17:30:00", 64.00, "Loblaws", "Toronto", 0),
    ("c09_5", "acc_009", "2026-09-08T08:09:00", 38.00, "TimHortons", "Toronto", 1),

    # ---- acc_010: escalating amounts at a high-risk crypto merchant in 40 minutes
    ("c10_1", "acc_010", "2026-09-02T15:00:00", 60.00, "Loblaws", "Toronto", 0),
    ("c10_2", "acc_010", "2026-09-09T01:00:00", 50.00, "CryptoQuickSwap", "Online", 0),
    ("c10_3", "acc_010", "2026-09-09T01:10:00", 200.00, "CryptoQuickSwap", "Online", 0),
    ("c10_4", "acc_010", "2026-09-09T01:20:00", 500.00, "CryptoQuickSwap", "Online", 0),
    ("c10_5", "acc_010", "2026-09-09T01:30:00", 1000.00, "CryptoQuickSwap", "Online", 0),
    ("c10_6", "acc_010", "2026-09-09T01:40:00", 2000.00, "CryptoQuickSwap", "Online", 1),

    # ---- acc_011: ordinary online purchase from a low-risk retailer (geo tool will say "different city")
    ("c11_1", "acc_011", "2026-09-01T11:00:00", 48.00, "Carrefour", "Muscat", 0),
    ("c11_2", "acc_011", "2026-09-04T19:00:00", 75.00, "Lulu Hypermarket", "Muscat", 0),
    ("c11_3", "acc_011", "2026-09-08T20:00:00", 60.00, "Amazon", "Online", 1),

    # ---- acc_012: mid-size purchase at a merchant we have NO risk data on (unknown != risky)
    ("c12_1", "acc_012", "2026-09-01T10:00:00", 70.00, "Loblaws", "Toronto", 0),
    ("c12_2", "acc_012", "2026-09-04T12:30:00", 120.00, "Amazon", "Online", 0),
    ("c12_3", "acc_012", "2026-09-08T15:20:00", 450.00, "ZephyrOutfitters", "Toronto", 1),

    # ---- acc_013: night-shift worker -- 2-3am transactions are this customer's NORMAL
    ("c13_1", "acc_013", "2026-09-01T02:40:00", 14.50, "7Eleven", "Toronto", 0),
    ("c13_2", "acc_013", "2026-09-03T02:55:00", 16.00, "7Eleven", "Toronto", 0),
    ("c13_3", "acc_013", "2026-09-05T02:35:00", 12.75, "7Eleven", "Toronto", 0),
    ("c13_4", "acc_013", "2026-09-07T02:45:00", 15.20, "7Eleven", "Toronto", 0),
    ("c13_5", "acc_013", "2026-09-09T02:40:00", 18.00, "7Eleven", "Toronto", 1),

    # ---- acc_014: young account, sudden jump to ~5x monthly spend at a legit retailer
    ("c14_1", "acc_014", "2026-09-02T12:00:00", 25.00, "Loblaws", "Toronto", 0),
    ("c14_2", "acc_014", "2026-09-05T17:00:00", 40.00, "Shell", "Toronto", 0),
    ("c14_3", "acc_014", "2026-09-09T11:30:00", 2400.00, "BestBuy", "Toronto", 1),

    # ---- acc_015: Lagos RESIDENT shopping in Lagos -- checks the agent isn't biased against a city
    ("c15_1", "acc_015", "2026-09-01T10:30:00", 22.00, "Shoprite", "Lagos", 0),
    ("c15_2", "acc_015", "2026-09-04T18:00:00", 45.00, "Shoprite", "Lagos", 0),
    ("c15_3", "acc_015", "2026-09-08T09:00:00", 30.00, "ChickenRepublic", "Lagos", 0),
    ("c15_4", "acc_015", "2026-09-10T11:15:00", 85.00, "Shoprite", "Lagos", 1),
]

# Merchants deliberately MISSING from this table: ElectroMart, ZephyrOutfitters
# (they test how the agent treats "unknown" -- it should not read as "high risk").
MERCHANT_RISK = [
    ("QuickGoldExchange", "high", "Frequently associated with card-testing and gift-card fraud"),
    ("GiftCardHub", "high", "Prepaid/gift-card reseller; common cash-out and card-testing target"),
    ("PrepaidPlus", "high", "Prepaid card reseller; common cash-out and card-testing target"),
    ("CryptoQuickSwap", "high", "Unregulated exchange; common cash-out destination for stolen cards"),
    ("Amazon", "low", "Major legitimate retailer"),
    ("Carrefour", "low", "Major legitimate retailer"),
    ("Lulu Hypermarket", "low", "Major legitimate retailer"),
    ("Shell", "low", "Legitimate fuel retailer"),
    ("Loblaws", "low", "Legitimate grocery retailer"),
    ("Costco", "low", "Major legitimate retailer"),
    ("AirCanada", "low", "Major legitimate airline"),
    ("HotelLumiere", "low", "Established hotel chain"),
    ("Starbucks", "low", "Major legitimate retailer"),
    ("PropertyMgmtCo", "low", "Established property management company"),
    ("TimHortons", "low", "Major legitimate retailer"),
    ("7Eleven", "low", "Major legitimate convenience retailer"),
    ("BestBuy", "low", "Major legitimate electronics retailer"),
    ("Shoprite", "low", "Major legitimate grocery retailer"),
    ("ChickenRepublic", "low", "Established restaurant chain"),
]


def seed():
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE accounts (
            account_id TEXT PRIMARY KEY,
            home_city TEXT,
            avg_monthly_spend REAL,
            account_age_days INTEGER
        )
    """)
    cur.execute("""
        CREATE TABLE transactions (
            txn_id TEXT PRIMARY KEY,
            account_id TEXT,
            timestamp TEXT,
            amount REAL,
            merchant TEXT,
            city TEXT,
            flagged INTEGER DEFAULT 0
        )
    """)
    cur.execute("""
        CREATE TABLE merchant_risk (
            merchant TEXT PRIMARY KEY,
            risk_level TEXT,
            reason TEXT
        )
    """)

    cur.executemany("INSERT INTO accounts VALUES (?, ?, ?, ?)", ACCOUNTS)
    cur.executemany("INSERT INTO transactions VALUES (?, ?, ?, ?, ?, ?, ?)", TRANSACTIONS)
    cur.executemany("INSERT INTO merchant_risk VALUES (?, ?, ?)", MERCHANT_RISK)

    conn.commit()
    conn.close()
    print(f"Seeded demo database at {DB_PATH}")


def seed_postgres(dbname: str):
    """Load the same dataset into a Postgres database already built by db/migrate.py
    (PLAN.md Phase 2 parity). Never point this at fraud_db.

    Mapping to the pipeline schema: account_id -> user_id, city -> location_city,
    naive timestamps -> UTC. account_age_days becomes account_opened_at = today (UTC) - age.
    is_fraud is set false: the agent can't read it, and these cases are labeled in eval_cases.json.
    """
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))  # repo root, for `db`
    from db import connect

    if dbname == "fraud_db":
        raise ValueError("refusing to seed demo data into fraud_db")

    conn = connect("admin", dbname)
    try:
        with conn, conn.cursor() as cur:
            cur.execute("TRUNCATE transactions, account_profiles, merchant_risk")
            cur.executemany(
                """INSERT INTO transactions
                       (transaction_id, user_id, event_timestamp, amount, currency, merchant,
                        location_city, is_fraud)
                   VALUES (%s, %s, (%s::timestamp AT TIME ZONE 'UTC'), %s, 'USD', %s, %s, false)""",
                [(t[0], t[1], t[2], t[3], t[4], t[5]) for t in TRANSACTIONS],
            )
            cur.executemany(
                """INSERT INTO account_profiles
                       (user_id, home_city, account_opened_at, avg_monthly_spend, source)
                   VALUES (%s, %s,
                           ((now() AT TIME ZONE 'UTC')::date - %s)::timestamp AT TIME ZONE 'UTC',
                           %s, 'generator')""",
                [(a[0], a[1], a[3], a[2]) for a in ACCOUNTS],
            )
            cur.executemany(
                "INSERT INTO merchant_risk (merchant, risk_level, reason) VALUES (%s, %s, %s)",
                MERCHANT_RISK,
            )
    finally:
        conn.close()
    print(f"Seeded demo dataset into Postgres database {dbname}")


if __name__ == "__main__":
    seed()
