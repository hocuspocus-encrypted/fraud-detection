"""
Ollama adapter: a local model through Ollama's /api/chat tool-calling loop.
Same contract as the other adapters -- the model returns tool_calls, we run
them and send each result back as a role="tool" message, until it answers
with the verdict JSON. No API key, no quota; the model runs on this machine.

Settings (.env):
    OLLAMA_MODEL            default qwen3:8b (must support tool calling)
    OLLAMA_HOST             default http://localhost:11434
    OLLAMA_NUM_CTX          context window, default 8192 (Ollama's own default is
                            too small for the prompt plus 50 rows of history)
    OLLAMA_THINK            true/false, default false: thinking models (e.g. qwen3)
                            are much slower with it on
    OLLAMA_TEMPERATURE      unset = the model's default
    OLLAMA_TIMEOUT_SECONDS  per request, default 120 (the first call also loads the model)
    OLLAMA_RECYCLE_AFTER_REQUESTS
                            default 8; 0 disables. Ollama's llama-server keeps a prompt
                            cache in system RAM (8 GB limit, ~190 MB per prompt here, and
                            Ollama exposes no setting for it). Every investigation's prompt
                            is unique, so the cache only costs memory: it ran this 15 GB
                            laptop out of RAM within ~40 investigations. After this many
                            chat requests the adapter unloads the model at the end of an
                            investigation, which empties the cache; the next call reloads it.

Two allowances for small local models, both kept narrow:
  - a tool call with bad arguments returns an error to the model instead of
    crashing the run, so it can correct itself (an unknown tool already did)
  - code fences and <think>...</think> around the final JSON are stripped
    before parsing; anything else that isn't valid JSON is still a parse error
"""
import json
import os
import re
import threading

import httpx

from tools import TOOL_SCHEMAS, TOOL_FUNCTIONS
from adapters.shared import build_task, parse_verdict, system_prompt

MODEL = os.getenv("OLLAMA_MODEL", "qwen3:8b")
MAX_ITERATIONS = 8

_THINK = re.compile(r"<think>.*?</think>", re.DOTALL)
_FENCE = re.compile(r"^```(?:json)?\s*(.*?)\s*```$", re.DOTALL)

_requests_since_recycle = 0
_recycle_lock = threading.Lock()


def _count_request() -> None:
    global _requests_since_recycle
    with _recycle_lock:
        _requests_since_recycle += 1


def _maybe_recycle(client: httpx.Client) -> None:
    """Unload the model once enough chat requests have filled llama-server's prompt cache."""
    global _requests_since_recycle
    limit = int(os.getenv("OLLAMA_RECYCLE_AFTER_REQUESTS", "8"))
    with _recycle_lock:
        if not limit or _requests_since_recycle < limit:
            return
        _requests_since_recycle = 0
    try:
        client.post("/api/generate", json={"model": MODEL, "keep_alive": 0})
    except httpx.HTTPError:
        pass  # best effort: the investigation already has its verdict


class OllamaError(Exception):
    """Non-200 from Ollama. status_code lets the worker tell 5xx (retry) from 404 (model missing)."""

    def __init__(self, status_code: int, message: str):
        super().__init__(f"Ollama HTTP {status_code}: {message}")
        self.status_code = status_code


def _host() -> str:
    host = os.getenv("OLLAMA_HOST", "http://localhost:11434")
    return host if "://" in host else f"http://{host}"  # Ollama's own OLLAMA_HOST may omit the scheme


def _make_client() -> httpx.Client:
    return httpx.Client(base_url=_host(), timeout=float(os.getenv("OLLAMA_TIMEOUT_SECONDS", "120")))


def _to_ollama_schema(schema: dict) -> dict:
    return {
        "type": "function",
        "function": {
            "name": schema["name"],
            "description": schema["description"],
            "parameters": schema["parameters"],
        },
    }


def _options() -> dict:
    options = {"num_ctx": int(os.getenv("OLLAMA_NUM_CTX", "8192"))}
    if os.getenv("OLLAMA_TEMPERATURE"):
        options["temperature"] = float(os.environ["OLLAMA_TEMPERATURE"])
    return options


def _clean_final(text: str) -> str:
    text = _THINK.sub("", text).strip()
    fenced = _FENCE.match(text)
    return fenced.group(1) if fenced else text


def _run_tool(name: str, arguments) -> dict:
    fn = TOOL_FUNCTIONS.get(name)
    if fn is None:
        return {"error": f"Unknown tool: {name}"}
    if isinstance(arguments, str):
        try:
            arguments = json.loads(arguments or "{}")
        except json.JSONDecodeError:
            return {"error": f"Bad arguments for {name}: not valid JSON"}
    try:
        return fn(**(arguments or {}))
    except TypeError as e:
        return {"error": f"Bad arguments for {name}: {e}"}


def run_investigation(account_id: str, txn_id: str, merchant: str, amount: float,
                      context: str | None = None, verbose: bool = True,
                      evidence: list[dict] | None = None) -> dict:
    tools = [_to_ollama_schema(s) for s in TOOL_SCHEMAS]
    messages = [
        {"role": "system", "content": system_prompt()},
        {"role": "user", "content": build_task(account_id, txn_id, merchant, amount, context, evidence)},
    ]
    think = os.getenv("OLLAMA_THINK", "false").strip().lower() in ("1", "true", "yes", "on")
    trace = list(evidence or [])  # prefetched checks (prompt v3) come first
    usage = {"turns": 0, "total_input_tokens": 0, "total_output_tokens": 0, "total_tokens": 0,
             "model_seconds": 0.0}

    with _make_client() as client:
        try:
            return _chat_loop(client, messages, tools, think, trace, usage, verbose)
        finally:
            _maybe_recycle(client)


def _chat_loop(client: httpx.Client, messages: list, tools: list, think: bool,
               trace: list, usage: dict, verbose: bool) -> dict:
    for i in range(MAX_ITERATIONS):
        response = client.post("/api/chat", json={
            "model": MODEL, "messages": messages, "tools": tools,
            "stream": False, "think": think, "options": _options(),
        })
        _count_request()
        if response.status_code != 200:
            try:
                detail = response.json().get("error", response.text)
            except ValueError:
                detail = response.text
            raise OllamaError(response.status_code, str(detail)[:500])
        body = response.json()

        usage["turns"] += 1
        usage["total_input_tokens"] += body.get("prompt_eval_count") or 0
        usage["total_output_tokens"] += body.get("eval_count") or 0
        usage["total_tokens"] = usage["total_input_tokens"] + usage["total_output_tokens"]
        usage["model_seconds"] = round(usage["model_seconds"] + (body.get("total_duration") or 0) / 1e9, 2)

        message = body.get("message") or {}
        messages.append(message)
        tool_calls = message.get("tool_calls") or []

        if not tool_calls:
            final_text = message.get("content") or ""
            trace.append({"step": i, "type": "final_response", "content": final_text})
            return {"verdict": parse_verdict(_clean_final(final_text)), "trace": trace, "usage": usage}

        for call in tool_calls:
            fn = call.get("function") or {}
            name, arguments = fn.get("name"), fn.get("arguments")
            result = _run_tool(name, arguments)

            if verbose:
                print(f"  [tool] {name}({arguments}) -> {json.dumps(result)[:200]}")

            trace.append({"step": i, "type": "tool_call", "tool": name, "input": arguments, "output": result})
            messages.append({"role": "tool", "tool_name": name, "content": json.dumps(result)})

    return {"verdict": {"error": "Max iterations reached without a final verdict"}, "trace": trace, "usage": usage}
