"""
Side-by-side comparison of two saved eval runs.

Usage (from the agent folder):
    python compare.py results/gemini_20261008_101500.json results/baseline_20261008_101655.json

Shows every case where the two runs disagree, plus who was right.
"""
import json
import sys


def load(path):
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data["meta"], {r["case_id"]: r for r in data["results"]}


def shown(r):
    if r is None:
        return "-"
    if r["error"]:
        return "ERROR"
    if r["parse_error"]:
        return "PARSE_ERR"
    return r["predicted"] or "-"


def main():
    if len(sys.argv) != 3:
        sys.exit("usage: python compare.py <results_a.json> <results_b.json>")

    meta_a, a = load(sys.argv[1])
    meta_b, b = load(sys.argv[2])
    na, nb = meta_a["provider"], meta_b["provider"]
    if na == nb:
        na, nb = f"{na}(A)", f"{nb}(B)"

    ids = [cid for cid in a if cid in b]
    only_a = [cid for cid in a if cid not in b]
    only_b = [cid for cid in b if cid not in a]

    print(f"\nComparing {na} vs {nb} on {len(ids)} shared cases\n")
    print(f"{'case':<8} {'category':<28} {'expected':<13} {na:<14} {nb:<14} note")
    print("-" * 92)

    agree = 0
    a_only_right, b_only_right, both_wrong_diff = [], [], []
    for cid in ids:
        ra, rb = a[cid], b[cid]
        same = ra["predicted"] == rb["predicted"]
        agree += same
        note = ""
        if not same:
            ca, cb = ra["correct_lenient"], rb["correct_lenient"]
            if ca and not cb:
                note = f"{na} right"
                a_only_right.append(cid)
            elif cb and not ca:
                note = f"{nb} right"
                b_only_right.append(cid)
            elif not ca and not cb:
                note = "both wrong, differently"
                both_wrong_diff.append(cid)
            else:
                note = "both acceptable"
        print(f"{cid:<8} {ra['category']:<28} {ra['expected']:<13} {shown(ra):<14} {shown(rb):<14} {note}")

    n = len(ids)
    sa = sum(a[c]["correct_strict"] for c in ids)
    sb = sum(b[c]["correct_strict"] for c in ids)
    la = sum(a[c]["correct_lenient"] for c in ids)
    lb = sum(b[c]["correct_lenient"] for c in ids)
    print(f"\nAgreement between runs: {agree}/{n}")
    print(f"Strict accuracy:  {na} {sa}/{n}   {nb} {sb}/{n}")
    print(f"Lenient accuracy: {na} {la}/{n}   {nb} {lb}/{n}")
    print(f"Only {na} right: {a_only_right or 'none'}")
    print(f"Only {nb} right: {b_only_right or 'none'}")

    # Show reasoning on disagreements so you can see WHY, not just who won
    diffs = [c for c in ids if a[c]["predicted"] != b[c]["predicted"]]
    if diffs:
        print("\nReasoning on disagreements:")
        for cid in diffs:
            print(f"\n  [{cid}] expected {a[cid]['expected']}")
            for name, r in ((na, a[cid]), (nb, b[cid])):
                text = (r["explanation"] or r["error"] or "")[:350]
                print(f"    {name}: {shown(r)} -> {text}")

    if only_a or only_b:
        print(f"\nNot compared (case missing from one run): {only_a + only_b}")
    print()


if __name__ == "__main__":
    main()
