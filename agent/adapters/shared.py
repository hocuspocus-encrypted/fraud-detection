"""
Shared across adapters so the two providers are held to the exact same
task and the same output contract. If you want to compare how Claude
vs. Gemini investigate the same case, this is what keeps it a fair
comparison -- only the plumbing differs.
"""
import json

SYSTEM_PROMPT = """You are a fraud investigation analyst. You are given a flagged \
transaction and must investigate it using the available tools before reaching a \
verdict. Gather enough evidence to justify your conclusion -- don't guess.

When you have enough evidence, respond with ONLY a JSON object (no other text, no \
markdown fences) in exactly this shape:

{
  "risk_score": <integer 0-100>,
  "verdict": "<one of: legitimate, suspicious, likely_fraud>",
  "explanation": "<2-4 sentence explanation citing the specific evidence you gathered>",
  "evidence_cited": ["<short bullet>", "<short bullet>", ...],
  "recommended_action": "<one of: approve, hold_for_review, block_and_notify_customer>"
}

Do not output this JSON until you have actually called tools to gather evidence. \
A verdict with no tool calls behind it is not acceptable."""


# Stored on every investigation row. Bump whenever SYSTEM_PROMPT or build_task's
# wording changes, so verdicts from different prompts are never mixed up.
#   v1  scaffold prompt
#   v2  build_task takes optional incident context from the pipeline (task text
#       without context is unchanged from v1)
PROMPT_VERSION = "v2"


def build_task(account_id: str, txn_id: str, merchant: str, amount: float, context: str | None = None) -> str:
    task = (
        f"Investigate this flagged transaction:\n"
        f"- account_id: {account_id}\n"
        f"- txn_id: {txn_id}\n"
        f"- merchant: {merchant}\n"
        f"- amount: {amount}\n"
    )
    if context:
        task += f"\nContext from the detection pipeline:\n{context}\n"
    return task


def parse_verdict(final_text: str) -> dict:
    try:
        return json.loads(final_text)
    except json.JSONDecodeError:
        return {"raw_output": final_text, "parse_error": True}
