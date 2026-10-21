"""
Anthropic (Claude) adapter. Uses the standard /v1/messages tool-use loop:
the model returns tool_use blocks, we execute them, and feed tool_result
blocks back in the next message.
"""
import json
from anthropic import Anthropic
from tools import TOOL_SCHEMAS, TOOL_FUNCTIONS
from adapters.shared import SYSTEM_PROMPT, build_task, parse_verdict

MODEL = "claude-sonnet-4-6"
MAX_ITERATIONS = 8


def _to_anthropic_schema(schema: dict) -> dict:
    return {
        "name": schema["name"],
        "description": schema["description"],
        "input_schema": schema["parameters"],
    }


def run_investigation(account_id: str, txn_id: str, merchant: str, amount: float,
                      context: str | None = None, verbose: bool = True) -> dict:
    client = Anthropic()  # reads ANTHROPIC_API_KEY from env
    tools = [_to_anthropic_schema(s) for s in TOOL_SCHEMAS]

    messages = [{"role": "user", "content": build_task(account_id, txn_id, merchant, amount, context)}]
    trace = []
    usage = {"turns": 0, "total_input_tokens": 0, "total_output_tokens": 0, "total_tokens": 0}

    for i in range(MAX_ITERATIONS):
        response = client.messages.create(
            model=MODEL,
            max_tokens=1024,
            system=SYSTEM_PROMPT,
            tools=tools,
            messages=messages,
        )

        messages.append({"role": "assistant", "content": response.content})
        usage["turns"] += 1
        usage["total_input_tokens"] += response.usage.input_tokens
        usage["total_output_tokens"] += response.usage.output_tokens
        usage["total_tokens"] += response.usage.input_tokens + response.usage.output_tokens

        tool_calls = [block for block in response.content if block.type == "tool_use"]

        if not tool_calls:
            final_text = "".join(block.text for block in response.content if block.type == "text")
            trace.append({"step": i, "type": "final_response", "content": final_text})
            return {"verdict": parse_verdict(final_text), "trace": trace, "usage": usage}

        tool_results = []
        for call in tool_calls:
            fn = TOOL_FUNCTIONS.get(call.name)
            result = fn(**call.input) if fn else {"error": f"Unknown tool: {call.name}"}

            if verbose:
                print(f"  [tool] {call.name}({call.input}) -> {json.dumps(result)[:200]}")

            trace.append({"step": i, "type": "tool_call", "tool": call.name, "input": call.input, "output": result})
            tool_results.append({
                "type": "tool_result",
                "tool_use_id": call.id,
                "content": json.dumps(result),
            })

        messages.append({"role": "user", "content": tool_results})

    return {"verdict": {"error": "Max iterations reached without a final verdict"}, "trace": trace, "usage": usage}
