"""
generate_transactions.py

Synthetic transaction generator for the real-time fraud detection pipeline
(PLAN.md Phase 4). Everything is deterministic from one seed:

  - a merchant catalog: a few very common merchants and a long tail of rare
    ones; ~5% high-risk (gift cards, crypto, prepaid); ~15% deliberately left
    out of merchant_risk so "unknown" stays a real case
  - per-user profiles (cust_00001...): home city, spending level, 5-15
    regular merchants, account-open date, one card

Each step() emits what a live feed would send at that moment:
  - mostly normal transactions: own merchant, home city, usual amount
  - legitimate oddities, labeled so false positives can be measured:
    legit_travel, legit_new_merchant, legit_big_purchase
  - injected fraud (is_fraud = true), every transaction labeled with a pattern_id:
    impossible_travel, high_risk_burst, card_testing. Multi-transaction
    patterns are scheduled over the following seconds/minutes.

The Kafka message format is unchanged (docs/CURRENT_STATE.md section 2), so the
Spark jobs need no changes. User ids use a new cust_ prefix so they never mix
with the older random user_ rows already in Postgres.

Usage:
    python generate_transactions.py --count 20 --interval 0.5
    python generate_transactions.py --count 100 --interval 0 --out transactions.jsonl
"""
import argparse
import heapq
import itertools
import json
import math
import random
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

from faker import Faker

DEFAULT_SEED = 42
DEFAULT_USERS = 5000
CATALOG_SIZE = 300
REFERENCE_DATE = datetime(2026, 10, 1, tzinfo=timezone.utc)  # account ages are fixed relative to this

# (city, country, weight)
CITIES = [
    ("Toronto", "CA", 10), ("Vancouver", "CA", 5), ("Montreal", "CA", 5),
    ("New York", "US", 10), ("Chicago", "US", 6), ("Los Angeles", "US", 7), ("Houston", "US", 4),
    ("London", "GB", 8), ("Manchester", "GB", 3), ("Paris", "FR", 5), ("Berlin", "DE", 4),
    ("Madrid", "ES", 3), ("Dubai", "AE", 4), ("Muscat", "OM", 3), ("Mumbai", "IN", 5),
    ("Singapore", "SG", 4), ("Tokyo", "JP", 4), ("Sydney", "AU", 3), ("Lagos", "NG", 3),
    ("Sao Paulo", "BR", 3),
]

EVERYDAY_CATEGORIES = [
    "grocery", "electronics", "travel", "dining", "fuel",
    "entertainment", "healthcare", "clothing", "utilities", "online_retail",
]
HIGH_RISK_KINDS = [  # (category, name suffix, reason)
    ("gift_cards", "Gift Cards", "Gift-card reseller; common cash-out and card-testing target"),
    ("crypto_exchange", "Crypto Exchange", "Unregulated exchange; common cash-out destination for stolen cards"),
    ("prepaid_cards", "Prepaid Cards", "Prepaid card reseller; common cash-out and card-testing target"),
]
CARD_TYPES = ["visa", "mastercard", "amex", "discover"]

# Per-step probabilities of starting each kind of event (one step ~ one producer tick).
P_FRAUD = 0.004
P_LEGIT_TRAVEL = 0.02
P_LEGIT_BIG = 0.01
P_NEW_MERCHANT = 0.05
FRAUD_KINDS = ["impossible_travel", "high_risk_burst", "card_testing"]

# Per-customer distributions. A customer's usual amount is lognormal(mu, sigma)
# with mu ~ N(SPEND_MU_MEAN, SPEND_MU_SD) and sigma ~ U(SPEND_SIGMA_RANGE);
# their share of the stream is lognormal(0, ACTIVITY_SIGMA). Changing these
# changes amounts and activity only: home cities and merchant sets stay the same.
# Chosen with simulate_flags.py (docs/CURRENT_STATE.md section 10): with 5000
# customers at 2 txn/s, typical amounts ~$33 (p95 ~$100) and moderate activity
# skew, the unchanged Spark rules flag ~60-75 incidents/hour at ~25-33% fraud
# precision. The first version (1000 customers, wider spend and activity) gave
# ~360/hour at ~5%, mostly busy customers tripping the >2-in-2-minutes rule.
SPEND_MU_MEAN = 3.5
SPEND_MU_SD = 0.4
SPEND_SIGMA_RANGE = (0.3, 0.7)
ACTIVITY_SIGMA = 0.5


@dataclass(frozen=True)
class Merchant:
    name: str
    category: str
    popularity: float          # relative draw weight; a few large, a long tail of small
    risk_level: str            # low / medium / high
    reason: str
    in_risk_table: bool        # False = deliberately missing from merchant_risk ("unknown")


@dataclass(frozen=True)
class Profile:
    user_id: str
    home_city: str
    home_country: str
    spend_mu: float            # lognormal parameters of this user's usual amount
    spend_sigma: float
    merchants: tuple           # names of regular merchants (never high-risk)
    activity: float            # relative share of the stream
    opened_at: datetime
    card_type: str
    card_last4: str

    @property
    def mean_amount(self) -> float:
        return math.exp(self.spend_mu + self.spend_sigma ** 2 / 2)

    @property
    def avg_monthly_spend(self) -> float:
        # The stream is time-compressed (a user can transact many times an hour),
        # so this is a declared, KYC-style figure: ~one usual purchase a day.
        return round(self.mean_amount * 30, 2)

    def account_age_days(self, now: datetime) -> int:
        return (now - self.opened_at).days


def build_catalog(seed: int = DEFAULT_SEED, size: int = CATALOG_SIZE) -> list[Merchant]:
    rng = random.Random(f"catalog-{seed}")
    fake = Faker()
    fake.seed_instance(seed)
    catalog, seen = [], set()
    n_high = round(size * 0.05)
    for rank in range(1, size + 1):
        high = rank > size - n_high  # high-risk merchants sit in the rare tail
        while True:
            if high:
                category, suffix, reason = HIGH_RISK_KINDS[rank % len(HIGH_RISK_KINDS)]
                name = f"{fake.last_name()} {suffix}"
            else:
                category = rng.choice(EVERYDAY_CATEGORIES)
                name = fake.company()
            if name not in seen:
                seen.add(name)
                break
        if high:
            risk_level = "high"
        elif rng.random() < 0.08:
            risk_level, reason = "medium", "Elevated chargeback rate"
        else:
            risk_level, reason = "low", "No known fraud association"
        catalog.append(Merchant(
            name=name, category=category,
            popularity=1.0 / rank ** 1.1,  # Zipf-like
            risk_level=risk_level, reason=reason,
            in_risk_table=rng.random() >= 0.15,
        ))
    return catalog


def build_profiles(seed: int = DEFAULT_SEED, catalog: list[Merchant] | None = None,
                   n_users: int = DEFAULT_USERS) -> list[Profile]:
    catalog = catalog or build_catalog(seed)
    rng = random.Random(f"profiles-{seed}")
    everyday = [m for m in catalog if m.risk_level != "high"]
    weights = [m.popularity for m in everyday]
    city_weights = [c[2] for c in CITIES]
    profiles = []
    for i in range(1, n_users + 1):
        city, country, _ = rng.choices(CITIES, weights=city_weights)[0]
        own = []
        target = rng.randint(5, 15)
        while len(own) < target:
            m = rng.choices(everyday, weights=weights)[0].name
            if m not in own:
                own.append(m)
        age_days = rng.randint(10, 59) if rng.random() < 0.1 else rng.randint(60, 8 * 365)
        profiles.append(Profile(
            user_id=f"cust_{i:05d}",
            home_city=city, home_country=country,
            spend_mu=rng.gauss(SPEND_MU_MEAN, SPEND_MU_SD), spend_sigma=rng.uniform(*SPEND_SIGMA_RANGE),
            merchants=tuple(own),
            activity=rng.lognormvariate(0, ACTIVITY_SIGMA),
            opened_at=REFERENCE_DATE - timedelta(days=age_days),
            card_type=rng.choice(CARD_TYPES),
            card_last4=f"{rng.randint(0, 9999):04d}",
        ))
    return profiles


@dataclass(order=True)
class _Scheduled:
    due: datetime
    seq: int
    record: dict = field(compare=False)
    label: dict | None = field(compare=False)


class TransactionGenerator:
    """Call step() once per producer tick; it returns the (record, label) pairs due now.
    label is None for ordinary transactions, else a ground_truth_labels row."""

    def __init__(self, seed: int = DEFAULT_SEED, n_users: int = DEFAULT_USERS, clock=None):
        self.seed, self.n_users = seed, n_users
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.catalog = build_catalog(seed)
        self.profiles = build_profiles(seed, self.catalog, n_users)
        self._by_name = {m.name: m for m in self.catalog}
        self._everyday = [m for m in self.catalog if m.risk_level != "high"]
        self._everyday_w = [m.popularity for m in self._everyday]
        self._high = [m for m in self.catalog if m.risk_level == "high"]
        self._activity = [p.activity for p in self.profiles]
        self._rng = random.Random(f"stream-{seed}")
        self._queue: list[_Scheduled] = []
        self._seq = itertools.count()

    # ---------- building blocks ----------

    def _id(self) -> str:
        return str(uuid.UUID(int=self._rng.getrandbits(128), version=4))

    def _amount(self, p: Profile, scale: float = 1.0) -> float:
        return max(1.0, round(self._rng.lognormvariate(p.spend_mu, p.spend_sigma) * scale, 2))

    def _record(self, p: Profile, ts: datetime, merchant: Merchant, amount: float,
                city: str | None = None, country: str | None = None, is_fraud: bool = False) -> dict:
        return {
            "transaction_id": self._id(),
            "timestamp": ts.isoformat(),
            "user_id": p.user_id,
            "amount": float(amount),
            "currency": "USD",
            "merchant": merchant.name,
            "merchant_category": merchant.category,
            "card_type": p.card_type,
            "card_last4": p.card_last4,
            "location_city": city or p.home_city,
            "location_country": country or p.home_country,
            "is_fraud": is_fraud,
        }

    @staticmethod
    def _label(kind: str, pattern_id: str, record: dict, **details) -> dict:
        return {
            "label_type": kind, "pattern_id": pattern_id,
            "user_id": record["user_id"], "transaction_id": record["transaction_id"],
            "details": {"is_fraud": record["is_fraud"], **details},
        }

    def _schedule(self, due: datetime, record: dict, label: dict | None) -> None:
        heapq.heappush(self._queue, _Scheduled(due, next(self._seq), record, label))

    def _far_city(self, p: Profile) -> tuple[str, str]:
        return self._rng.choice([(c, k) for c, k, _ in CITIES if k != p.home_country])

    def _new_merchant(self, p: Profile) -> Merchant:
        while True:
            m = self._rng.choices(self._everyday, weights=self._everyday_w)[0]
            if m.name not in p.merchants:
                return m

    # ---------- one tick ----------

    def step(self) -> list[tuple[dict, dict | None]]:
        now = self.clock()
        out = []
        while self._queue and self._queue[0].due <= now:
            item = heapq.heappop(self._queue)
            item.record["timestamp"] = now.isoformat()  # emitted now, like a live feed
            out.append((item.record, item.label))

        p = self._rng.choices(self.profiles, weights=self._activity)[0]
        roll = self._rng.random()
        if roll < P_FRAUD:
            out.extend(self._start_fraud(p, now))
        elif roll < P_FRAUD + P_LEGIT_TRAVEL:
            city, country = self._rng.choice([(c, k) for c, k, _ in CITIES if c != p.home_city])
            r = self._record(p, now, self._new_merchant(p), self._amount(p), city, country)
            out.append((r, self._label("legit_travel", self._id(), r)))
        elif roll < P_FRAUD + P_LEGIT_TRAVEL + P_LEGIT_BIG:
            m = self._by_name[self._rng.choice(p.merchants)]
            r = self._record(p, now, m, self._amount(p, scale=self._rng.uniform(5, 10)))
            out.append((r, self._label("legit_big_purchase", self._id(), r)))
        elif roll < P_FRAUD + P_LEGIT_TRAVEL + P_LEGIT_BIG + P_NEW_MERCHANT:
            r = self._record(p, now, self._new_merchant(p), self._amount(p))
            out.append((r, self._label("legit_new_merchant", self._id(), r)))
        else:
            m = self._by_name[self._rng.choice(p.merchants)]
            out.append((self._record(p, now, m, self._amount(p)), None))
        return out

    def _start_fraud(self, p: Profile, now: datetime) -> list[tuple[dict, dict | None]]:
        kind = self._rng.choice(FRAUD_KINDS)
        pattern = self._id()
        if kind == "impossible_travel":
            # an ordinary purchase at home now, then the same card far away minutes later
            home = self._record(p, now, self._by_name[self._rng.choice(p.merchants)], self._amount(p))
            city, country = self._far_city(p)
            due = now + timedelta(minutes=self._rng.uniform(3, 20))
            far = self._record(p, due, self._new_merchant(p), self._amount(p, self._rng.uniform(2, 6)),
                               city, country, is_fraud=True)
            self._schedule(due, far, self._label(kind, pattern, far, home_city=p.home_city, far_city=city))
            return [(home, None)]

        if kind == "high_risk_burst":
            m = self._rng.choice(self._high)
            amount = round(self._rng.uniform(40, 80), 2)
            t, first = now, None
            for i in range(self._rng.randint(3, 5)):
                r = self._record(p, t, m, amount, is_fraud=True)
                label = self._label(kind, pattern, r, step=i)
                if first is None:
                    first = (r, label)
                else:
                    self._schedule(t, r, label)
                amount = round(amount * self._rng.uniform(1.6, 2.2), 2)
                t += timedelta(seconds=self._rng.uniform(10, 20))
            return [first]

        # card_testing: tiny probes at assorted merchants, then one large charge
        t, first = now, None
        n_probes = self._rng.randint(4, 6)
        for i in range(n_probes + 1):
            last = i == n_probes
            m = self._rng.choice(self._high) if last else self._rng.choice(self._high + self._everyday[-100:])
            amount = round(self._rng.uniform(200, 800), 2) if last else round(self._rng.uniform(1, 5), 2)
            r = self._record(p, t, m, amount, is_fraud=True)
            label = self._label(kind, pattern, r, step=i, probe=not last)
            if first is None:
                first = (r, label)
            else:
                self._schedule(t, r, label)
            t += timedelta(seconds=self._rng.uniform(5, 15))
        return [first]


def main():
    parser = argparse.ArgumentParser(description="Generate synthetic transaction records.")
    parser.add_argument("--count", type=int, default=50, help="Number of records to generate (0 = infinite).")
    parser.add_argument("--interval", type=float, default=1.0, help="Seconds to sleep between ticks.")
    parser.add_argument("--out", type=str, default=None, help="Optional path to append JSON lines to.")
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--users", type=int, default=DEFAULT_USERS)
    args = parser.parse_args()

    gen = TransactionGenerator(seed=args.seed, n_users=args.users)
    out_file = open(args.out, "a") if args.out else None
    generated = 0
    try:
        while args.count == 0 or generated < args.count:
            for record, _ in gen.step():
                line = json.dumps(record)
                print(line)
                if out_file:
                    out_file.write(line + "\n")
                    out_file.flush()
                generated += 1
            if args.interval > 0:
                time.sleep(args.interval)
    except KeyboardInterrupt:
        print(f"\nStopped after {generated} records.")
    finally:
        if out_file:
            out_file.close()


if __name__ == "__main__":
    main()
