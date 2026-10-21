"""
Phase 4 acceptance checks (PLAN.md, Phase 4): the realistic generator.

Pure-Python checks use a fake clock, so thousands of events take a fraction
of a second. The last checks load generated data into a throwaway database
and read it back through the agent's tools.

    venv\\Scripts\\python.exe -m pytest tests -q
"""
import statistics
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone

import pytest

from db import ROOT, connect
from db.migrate import migrate, recreate_database
from generate_transactions import TransactionGenerator, build_catalog, build_profiles

# The Kafka message contract from docs/CURRENT_STATE.md section 2; Spark parses exactly these.
MESSAGE_FIELDS = {
    "transaction_id": str, "timestamp": str, "user_id": str, "amount": float, "currency": str,
    "merchant": str, "merchant_category": str, "card_type": str, "card_last4": str,
    "location_city": str, "location_country": str, "is_fraud": bool,
}


class FakeClock:
    def __init__(self, start=datetime(2026, 10, 21, 12, 0, tzinfo=timezone.utc), tick=0.5):
        self.now, self.tick = start, timedelta(seconds=tick)

    def __call__(self):
        return self.now

    def advance(self):
        self.now += self.tick


def run(gen_or_seed=7, steps=20_000, **kwargs):
    """Step a generator like the producer does; returns [(record, label_or_None)]."""
    clock = FakeClock()
    gen = gen_or_seed if isinstance(gen_or_seed, TransactionGenerator) else \
        TransactionGenerator(seed=gen_or_seed, clock=clock, **kwargs)
    gen.clock = clock
    out = []
    for _ in range(steps):
        out.extend(gen.step())
        clock.advance()
    return gen, out


@pytest.fixture(scope="module")
def stream():
    # 1000 customers so each one has enough transactions to check consistency;
    # the properties checked here don't depend on the customer count.
    return run(7, 20_000, n_users=1000)


# ---------- determinism and contract ----------

def test_same_seed_same_stream_different_seed_different():
    _, a = run(11, 3_000)
    _, b = run(11, 3_000)
    _, c = run(12, 3_000)
    assert a == b
    assert [r["transaction_id"] for r, _ in a] != [r["transaction_id"] for r, _ in c]


def test_message_schema_unchanged(stream):
    _, events = stream
    assert len(events) > 15_000
    for record, _ in events:
        assert set(record) == set(MESSAGE_FIELDS)
        for field, typ in MESSAGE_FIELDS.items():
            assert isinstance(record[field], typ), (field, record[field])
        assert datetime.fromisoformat(record["timestamp"]).tzinfo is not None
        assert record["amount"] > 0 and round(record["amount"], 2) == record["amount"]
        assert record["currency"] == "USD"
        assert record["user_id"].startswith("cust_")


# ---------- catalog and profiles ----------

def test_catalog_shape():
    catalog = build_catalog(seed=7)
    names = [m.name for m in catalog]
    assert len(names) == len(set(names)) >= 200
    high = [m for m in catalog if m.risk_level == "high"]
    assert 0.03 <= len(high) / len(catalog) <= 0.08
    missing = [m for m in catalog if not m.in_risk_table]
    assert 0.10 <= len(missing) / len(catalog) <= 0.20
    weights = sorted((m.popularity for m in catalog), reverse=True)
    assert sum(weights[:10]) / sum(weights) > 0.25, "a few merchants should be very common"


def test_profiles_are_stable_and_never_shop_at_high_risk_merchants():
    catalog = build_catalog(seed=7)
    risk = {m.name: m.risk_level for m in catalog}
    profiles = build_profiles(seed=7, catalog=catalog, n_users=1000)
    assert len(profiles) == 1000
    assert profiles == build_profiles(seed=7, catalog=catalog, n_users=1000)
    for p in profiles:
        assert 5 <= len(p.merchants) <= 15
        assert all(risk[m] != "high" for m in p.merchants)
    assert len({p.home_city for p in profiles}) >= 10
    young = sum(p.account_age_days(datetime(2026, 10, 21, tzinfo=timezone.utc)) < 60 for p in profiles)
    assert 0.05 <= young / len(profiles) <= 0.20


# ---------- behavior ----------

def test_users_are_consistent(stream):
    gen, events = stream
    by_user = defaultdict(list)
    for record, _ in events:
        by_user[record["user_id"]].append(record)
    home = {p.user_id: p.home_city for p in gen.profiles}
    own = {p.user_id: set(p.merchants) for p in gen.profiles}

    busy = {u: rs for u, rs in by_user.items() if len(rs) >= 10}
    assert len(busy) >= 300
    home_share = [sum(r["location_city"] == home[u] for r in rs) / len(rs) for u, rs in busy.items()]
    own_share = [sum(r["merchant"] in own[u] for r in rs) / len(rs) for u, rs in busy.items()]
    print(f"\nmedian home-city share {statistics.median(home_share):.3f}, "
          f"median own-merchant share {statistics.median(own_share):.3f}, busy users {len(busy)}")
    assert statistics.median(home_share) >= 0.90
    assert statistics.median(own_share) >= 0.75

    repeaters = sum(1 for rs in by_user.values()
                    if len(rs) >= 3 and Counter(r["merchant"] for r in rs).most_common(1)[0][1] >= 2)
    assert repeaters / sum(1 for rs in by_user.values() if len(rs) >= 3) > 0.9


def test_distinct_merchants_far_fewer_than_transactions(stream):
    _, events = stream
    merchants = {r["merchant"] for r, _ in events}
    assert len(merchants) <= len(build_catalog(seed=7))
    assert len(merchants) < len(events) / 20


# ---------- labels ----------

def test_every_fraud_transaction_is_labeled_and_vice_versa(stream):
    _, events = stream
    for record, label in events:
        if record["is_fraud"]:
            assert label and label["details"]["is_fraud"] is True and label["pattern_id"]
            assert label["transaction_id"] == record["transaction_id"]
        if label:
            assert label["user_id"] == record["user_id"]
            assert label["details"]["is_fraud"] == record["is_fraud"]


def test_all_scenarios_appear_at_modest_rates(stream):
    _, events = stream
    labels = Counter(label["label_type"] for _, label in events if label)
    fraud_patterns = {label["pattern_id"] for _, label in events if label and label["details"]["is_fraud"]}
    print(f"\nlabels: {dict(labels)}  fraud patterns: {len(fraud_patterns)}")
    for kind in ("impossible_travel", "high_risk_burst", "card_testing",
                 "legit_travel", "legit_big_purchase", "legit_new_merchant"):
        assert labels[kind] > 0, kind
    fraud_share = sum(r["is_fraud"] for r, _ in events) / len(events)
    assert 0.002 <= fraud_share <= 0.03


def test_burst_patterns_fit_inside_one_spark_window(stream):
    """card_testing and high_risk_burst should trip Spark's >2-in-2-minutes rule."""
    _, events = stream
    patterns = defaultdict(list)
    for record, label in events:
        if label and label["label_type"] in ("card_testing", "high_risk_burst"):
            patterns[label["pattern_id"]].append(datetime.fromisoformat(record["timestamp"]))
    complete = [ts for ts in patterns.values() if len(ts) >= 4]
    assert complete
    for ts in complete:
        assert max(ts) - min(ts) < timedelta(minutes=2)


def test_impossible_travel_is_fast_and_far(stream):
    gen, events = stream
    country = {p.user_id: p.home_country for p in gen.profiles}
    last_home = {}
    checked = 0
    for record, label in events:
        if not record["is_fraud"]:
            last_home[record["user_id"]] = datetime.fromisoformat(record["timestamp"])
        elif label["label_type"] == "impossible_travel":
            assert record["location_country"] != country[record["user_id"]]
            gap = datetime.fromisoformat(record["timestamp"]) - last_home[record["user_id"]]
            assert gap <= timedelta(minutes=30)
            checked += 1
    assert checked > 0


# ---------- flag volume of the defaults ----------

def test_defaults_keep_flag_volume_and_precision_in_range():
    """Simulated Spark rules on the default stream (simulate_flags.py). Guards against a
    generator change quietly flooding the investigation queue again."""
    from simulate_flags import simulate
    r = simulate(seed=7, hours=0.5)
    print(f"\nsimulated defaults: {r}")
    assert 30 <= r["incidents_per_hour"] <= 120
    assert r["precision"] >= 0.15
    for kind in ("high_risk_burst", "card_testing"):
        caught, total = r["recall"][kind]
        assert total and caught / total >= 0.8, kind


# ---------- reference data and the agent's view ----------

PHASE4_DB = "fraud_phase4_test"


@pytest.fixture(scope="module")
def loaded_db(stream):
    from seed_reference_data import seed_reference_data
    recreate_database(PHASE4_DB)
    migrate(PHASE4_DB)
    gen, events = stream
    seed_reference_data(seed=7, n_users=gen.n_users, dbname=PHASE4_DB)
    seed_reference_data(seed=7, n_users=gen.n_users, dbname=PHASE4_DB)  # idempotent
    conn = connect("admin", PHASE4_DB)
    with conn, conn.cursor() as cur:
        cur.executemany(
            """INSERT INTO transactions (transaction_id, event_timestamp, user_id, amount, currency, merchant,
                                         merchant_category, card_type, card_last4, location_city,
                                         location_country, is_fraud)
               VALUES (%(transaction_id)s, %(timestamp)s, %(user_id)s, %(amount)s, %(currency)s, %(merchant)s,
                       %(merchant_category)s, %(card_type)s, %(card_last4)s, %(location_city)s,
                       %(location_country)s, %(is_fraud)s)""",
            [r for r, _ in events],
        )
    conn.close()
    yield gen, events
    admin = connect("admin", "postgres")
    admin.autocommit = True
    with admin.cursor() as cur:
        cur.execute(f'DROP DATABASE IF EXISTS "{PHASE4_DB}" WITH (FORCE)')
    admin.close()


def test_reference_data_written_once(loaded_db):
    gen, _ = loaded_db
    conn = connect("admin", PHASE4_DB)
    with conn.cursor() as cur:
        cur.execute("SELECT count(*), count(*) FILTER (WHERE source = 'generator') FROM account_profiles")
        assert cur.fetchone() == (gen.n_users, gen.n_users)
        cur.execute("SELECT count(*) FROM merchant_risk")
        assert cur.fetchone()[0] == sum(m.in_risk_table for m in gen.catalog)
    conn.close()


def test_agent_tools_see_a_coherent_account(loaded_db, monkeypatch):
    gen, events = loaded_db
    sys.path.insert(0, str(ROOT / "agent"))
    import tools
    monkeypatch.setenv("AGENT_PGDATABASE", PHASE4_DB)
    tools.reset_pool()
    try:
        profile = gen.profiles[0]
        history = tools.get_account_history(profile.user_id)
        assert history["home_city"] == profile.home_city
        assert history["avg_monthly_spend"] > 0 and history["account_age_days"] >= 0

        pattern = tools.get_transaction_pattern(profile.user_id)
        cities = Counter(t["city"] for t in pattern["transactions"])
        assert cities.most_common(1)[0][0] == profile.home_city

        high = next(m for m in gen.catalog if m.risk_level == "high" and m.in_risk_table)
        assert tools.check_merchant_risk(high.name)["risk_level"] == "high"
        unknown = next(m for m in gen.catalog if not m.in_risk_table)
        assert tools.check_merchant_risk(unknown.name)["risk_level"] == "unknown"
    finally:
        tools.reset_pool()
