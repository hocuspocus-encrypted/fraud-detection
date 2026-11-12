# Review UI walkthrough checklist (PLAN_HITL Phase H5)

How to check that the review pages work end to end. Each item says what to do and what should happen.

**Completed 2026-11-12** on `fraud_h5_walkthrough`, a `pg_dump` copy of `fraud_db` (69 held cases, 0 decisions). A decision can't be deleted from `analyst_decisions`, so the walkthrough did not run on `fraud_db` itself. It was driven through the real entry script and page navigation with Streamlit's `AppTest`, and the server was also started headless to check that it serves every page. The same steps run on every test run in `tests/test_h5_review_ui.py`. Nobody has clicked through it in a browser yet; that pass is the last section.

## Setup

```powershell
docker compose up -d
.\venv\Scripts\python.exe -m db.migrate      # needs ANALYST_PASSWORD in .env, so analyst can log in
.\run_dashboard.bat
```

To repeat the walkthrough without writing to `fraud_db`'s audit trail, point the review pages at a copy:

```powershell
docker exec fraud_postgres psql -U fraud_user -d postgres -c "CREATE DATABASE fraud_h5_walkthrough"
docker exec fraud_postgres sh -c "pg_dump -U fraud_user fraud_db | psql -q -U fraud_user -d fraud_h5_walkthrough"
$env:PGDATABASE = "fraud_h5_walkthrough"; .\venv\Scripts\python.exe -m streamlit run streamlit_dashboard.py
```

The Pipeline page still reads `fraud_db` as `fraud_user` (its connection string is hardcoded, as before), so `PGDATABASE` moves only the review pages.

## Walkthrough

| # | Step | Expected | Result 2026-11-12 |
|---|---|---|---|
| 1 | Open the dashboard | Sidebar shows Pipeline, then Review: Review queue, Case detail, Oversight. Pipeline is the default page and still auto-refreshes | ✅ Pipeline renders, no exception; `/`, `/review_queue`, `/case_detail`, `/oversight` all serve (HTTP 200) |
| 2 | Open **Review queue** | Held and needs-review cases, then QA samples, highest priority first. Columns: case, user, amount, agent verdict and score, baseline verdict and score, reasons, age, status, priority, last decision | ✅ 69 open, 69 held, 0 QA samples; top case 963 (`cust_04110`) |
| 3 | Select the top row and press **Open case** | Case detail opens on that case | ✅ by setting the case in session state, as the button does (`AppTest` can't click a table row) |
| 4 | Read the case | Agent and baseline verdicts and scores, review reasons, rule hits, incident transactions (primary marked), signals, explanation, evidence with the tool each claim cites, a collapsed tool trace, previous decisions | ✅ case 963: likely_fraud 95 vs. baseline legitimate 0; 3 tables, explanation, `Tool trace (3 steps)` |
| 5 | Choose **override**, final action **approve**, leave the reason empty, submit | Inline error "An override needs a reason."; nothing written; case still held | ✅ error shown; 0 rows for case 963; status `held_pending_review` |
| 6 | Enter a reason and submit again | Success message naming the decision; the case leaves the queue | ✅ "Decision 1 recorded for case 963: override, approve. The case left the queue." |
| 7 | Confirm the decision row | One `analyst_decisions` row with the name, override, approve and the reason; case `reviewed` | ✅ `('vib-walkthrough', 'override', 'approve', "Walkthrough: amount is within this customer's usual spend at home.")`; status `reviewed` |
| 8 | Back to **Review queue** | Case 963 is gone; one fewer open case | ✅ 68 open, 963 absent |
| 9 | Open **Oversight** | Queue depth, cases decided, median time to decision, override rate, counts by status | ✅ 68 queued, 1 decided, 17d 19h median (the case was routed 2026-10-25), 100% override |

Also covered by the tests, not walked through by hand: a `request_info` decision keeps the case in the queue and shows in "Last decision"; agree or override without a final action is refused before submit; a decision on a case that isn't open (an auto-cleared non-QA row) shows "Not recorded" and writes nothing.

## Browser pass (to do by hand)

- [ ] Steps 1-9 in a browser, including clicking a row and **Open case**
- [ ] Long merchant names and explanations wrap and don't break the layout
- [ ] The decision form keeps what you typed while you read the case (only the Pipeline page auto-refreshes)
