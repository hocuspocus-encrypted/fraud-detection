import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))  # repo root, for `db`

from db.migrate import migrate, recreate_database  # noqa: E402
from seed_db import seed_postgres  # noqa: E402

DEMO_DB = "fraud_agent_demo"


if __name__ == "__main__":
    recreate_database(DEMO_DB)
    migrate(DEMO_DB)
    seed_postgres(DEMO_DB)
    os.environ["AGENT_PGDATABASE"] = DEMO_DB
    from orchestrator import run_investigation  # import after env is set

    cases = [
        {"account_id": "acc_001", "txn_id": "t4", "merchant": "QuickGoldExchange", "amount": 1980.00},
        {"account_id": "acc_002", "txn_id": "t7", "merchant": "Carrefour", "amount": 310.00},
    ]

    for case in cases:
        print(f"\n{'='*60}\nInvestigating {case['txn_id']} ({case['account_id']})\n{'='*60}")
        result = run_investigation(**case)
        print("\nFinal verdict:")
        print(json.dumps(result["verdict"], indent=2))
