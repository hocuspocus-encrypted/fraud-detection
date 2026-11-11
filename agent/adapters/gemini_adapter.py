"""
Gemini adapter. Uses the Interactions API (client.interactions.create),
which is Google's current interface for this -- not the older
generate_content-based function calling. It's stateful: each turn
references the previous one via previous_interaction_id instead of you
re-sending the full message history yourself (that's the biggest
structural difference from the Anthropic adapter).

Docs: https://ai.google.dev/gemini-api/docs/function-calling
"""
import json
import os

from google import genai
from google.genai import types
from tools import TOOL_SCHEMAS, TOOL_FUNCTIONS
from adapters.shared import build_task, parse_verdict, system_prompt

MODEL = "gemini-3.8-flash"
MAX_ITERATIONS = 8
USAGE_FIELDS = ("total_input_tokens", "total_output_tokens", "total_thought_tokens",
                "total_cached_tokens", "total_tokens")


def _add_usage(totals: dict, interaction) -> None:
    """Each turn of the stateful conversation is billed separately; sum them."""
    usage = getattr(interaction, "usage", None)
    totals["turns"] += 1
    for field in USAGE_FIELDS:
        totals[field] += getattr(usage, field, None) or 0


def _to_gemini_schema(schema: dict) -> dict:
    return {
        "type": "function",
        "name": schema["name"],
        "description": schema["description"],
        "parameters": schema["parameters"],
    }


def run_investigation(account_id: str, txn_id: str, merchant: str, amount: float,
                      context: str | None = None, verbose: bool = True,
                      evidence: list[dict] | None = None) -> dict:
    # reads GEMINI_API_KEY from env. Without a timeout a stalled request blocks
    # forever (seen 2026-10-20); APITimeoutError is retried by the worker as transient.
    timeout_ms = int(float(os.getenv("GEMINI_TIMEOUT_SECONDS", "60")) * 1000)
    client = genai.Client(http_options=types.HttpOptions(timeout=timeout_ms))
    tools = [_to_gemini_schema(s) for s in TOOL_SCHEMAS]

    task = build_task(account_id, txn_id, merchant, amount, context, evidence)
    # Gemini's Interactions API doesn't take a separate "system" argument
    # the way Anthropic's does -- prepend it to the input instead.
    full_input = f"{system_prompt()}\n\n{task}"

    trace = list(evidence or [])  # prefetched checks (prompt v3) come first
    usage = {"turns": 0, **{f: 0 for f in USAGE_FIELDS}}
    interaction = client.interactions.create(model=MODEL, input=full_input, tools=tools)
    _add_usage(usage, interaction)

    for i in range(MAX_ITERATIONS):
        function_call_steps = [s for s in interaction.steps if s.type == "function_call"]

        if not function_call_steps:
            final_text = interaction.output_text or ""
            trace.append({"step": i, "type": "final_response", "content": final_text})
            return {"verdict": parse_verdict(final_text), "trace": trace, "usage": usage}

        function_results = []
        for call in function_call_steps:
            fn = TOOL_FUNCTIONS.get(call.name)
            result = fn(**call.arguments) if fn else {"error": f"Unknown tool: {call.name}"}

            if verbose:
                print(f"  [tool] {call.name}({call.arguments}) -> {json.dumps(result)[:200]}")

            trace.append({"step": i, "type": "tool_call", "tool": call.name, "input": call.arguments, "output": result})
            function_results.append({
                "type": "function_result",
                "name": call.name,
                "call_id": call.id,
                "result": [{"type": "text", "text": json.dumps(result)}],
            })

        interaction = client.interactions.create(
            model=MODEL,
            tools=tools,
            previous_interaction_id=interaction.id,
            input=function_results,
        )
        _add_usage(usage, interaction)

    return {"verdict": {"error": "Max iterations reached without a final verdict"}, "trace": trace, "usage": usage}
