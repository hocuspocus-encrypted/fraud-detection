"""
Ollama adapter checks, against a fake Ollama server (httpx.MockTransport),
so they run without Ollama installed and never touch the database.

    venv\\Scripts\\python.exe -m pytest tests -q
"""
import json
import sys

import httpx
import pytest

from db import ROOT

sys.path.insert(0, str(ROOT / "agent"))
from adapters import ollama_adapter  # noqa: E402
from investigation_worker import is_transient  # noqa: E402

FINAL = {
    "risk_score": 85,
    "verdict": "likely_fraud",
    "explanation": "High-risk merchant far from home.",
    "evidence_cited": ["high-risk merchant"],
    "recommended_action": "block_and_notify_customer",
}


def chat_response(content="", tool_calls=None, prompt_tokens=100, output_tokens=20):
    message = {"role": "assistant", "content": content}
    if tool_calls:
        message["tool_calls"] = tool_calls
    return {"model": "test-model", "message": message, "done": True,
            "prompt_eval_count": prompt_tokens, "eval_count": output_tokens, "total_duration": 2_000_000_000}


def tool_call(name, arguments):
    return {"function": {"name": name, "arguments": arguments}}


@pytest.fixture
def fake_ollama(monkeypatch):
    """Serve a scripted list of /api/chat responses; record every request body."""
    state = {"responses": [], "requests": []}

    def handler(request: httpx.Request):
        assert request.url.path == "/api/chat"
        state["requests"].append(json.loads(request.content))
        status, body = state["responses"].pop(0)
        return httpx.Response(status, json=body)

    monkeypatch.setattr(ollama_adapter, "_make_client",
                        lambda: httpx.Client(base_url="http://ollama.test", transport=httpx.MockTransport(handler)))
    calls = []
    monkeypatch.setattr(ollama_adapter, "TOOL_FUNCTIONS", {
        "get_account_history": lambda account_id: calls.append(("history", account_id)) or
        {"account_id": account_id, "home_city": "Toronto"},
        "check_merchant_risk": lambda merchant: calls.append(("risk", merchant)) or
        {"merchant": merchant, "risk_level": "high"},
    })
    state["tool_calls"] = calls
    return state


def run(**kwargs):
    args = dict(account_id="acc_001", txn_id="t4", merchant="QuickGoldExchange", amount=1980.0, verbose=False)
    args.update(kwargs)
    return ollama_adapter.run_investigation(**args)


def test_tool_round_trip_and_final_verdict(fake_ollama):
    fake_ollama["responses"] = [
        (200, chat_response(tool_calls=[tool_call("get_account_history", {"account_id": "acc_001"}),
                                        tool_call("check_merchant_risk", {"merchant": "QuickGoldExchange"})])),
        (200, chat_response(content=json.dumps(FINAL), prompt_tokens=300, output_tokens=60)),
    ]
    result = run()

    assert result["verdict"] == FINAL
    assert fake_ollama["tool_calls"] == [("history", "acc_001"), ("risk", "QuickGoldExchange")]
    assert [s["tool"] for s in result["trace"] if s["type"] == "tool_call"] == \
        ["get_account_history", "check_merchant_risk"]
    assert result["usage"]["turns"] == 2
    assert result["usage"]["total_input_tokens"] == 400
    assert result["usage"]["total_output_tokens"] == 80
    assert result["usage"]["total_tokens"] == 480

    first, second = fake_ollama["requests"]
    assert first["stream"] is False
    assert first["messages"][0]["role"] == "system"
    assert {t["function"]["name"] for t in first["tools"]} == {s["name"] for s in ollama_adapter.TOOL_SCHEMAS}
    assert all(t["type"] == "function" for t in first["tools"])
    tool_messages = [m for m in second["messages"] if m["role"] == "tool"]
    assert [m["tool_name"] for m in tool_messages] == ["get_account_history", "check_merchant_risk"]
    assert json.loads(tool_messages[1]["content"])["risk_level"] == "high"


def test_context_is_included_in_the_task(fake_ollama):
    fake_ollama["responses"] = [(200, chat_response(content=json.dumps(FINAL)))]
    run(context="Incident window ...: 3 transactions totaling $900.00.")
    user = fake_ollama["requests"][0]["messages"][1]
    assert user["role"] == "user" and "3 transactions totaling $900.00" in user["content"]


@pytest.mark.parametrize("content", [
    "```json\n" + json.dumps(FINAL) + "\n```",
    "<think>reasoning about the case</think>\n" + json.dumps(FINAL),
    "  " + json.dumps(FINAL) + "  \n",
])
def test_formatting_around_the_json_is_tolerated(fake_ollama, content):
    fake_ollama["responses"] = [(200, chat_response(content=content))]
    assert run()["verdict"] == FINAL


def test_prose_instead_of_json_is_a_parse_error(fake_ollama):
    fake_ollama["responses"] = [(200, chat_response(content="I think this is probably fraud."))]
    assert run()["verdict"].get("parse_error") is True


def test_string_arguments_are_decoded(fake_ollama):
    fake_ollama["responses"] = [
        (200, chat_response(tool_calls=[tool_call("get_account_history", '{"account_id": "acc_001"}')])),
        (200, chat_response(content=json.dumps(FINAL))),
    ]
    run()
    assert fake_ollama["tool_calls"] == [("history", "acc_001")]


def test_unknown_tool_and_bad_arguments_go_back_to_the_model(fake_ollama):
    fake_ollama["responses"] = [
        (200, chat_response(tool_calls=[tool_call("delete_everything", {}),
                                        tool_call("get_account_history", {"acct": "acc_001"})])),
        (200, chat_response(content=json.dumps(FINAL))),
    ]
    result = run()
    outputs = [s["output"] for s in result["trace"] if s["type"] == "tool_call"]
    assert "Unknown tool" in outputs[0]["error"]
    assert "Bad arguments" in outputs[1]["error"]
    assert result["verdict"] == FINAL


def test_max_iterations(fake_ollama):
    loop = (200, chat_response(tool_calls=[tool_call("get_account_history", {"account_id": "acc_001"})]))
    fake_ollama["responses"] = [loop] * ollama_adapter.MAX_ITERATIONS
    assert "Max iterations" in run()["verdict"]["error"]


def test_server_errors_are_transient_and_missing_model_is_not(fake_ollama):
    fake_ollama["responses"] = [(500, {"error": "CUDA out of memory"})]
    with pytest.raises(ollama_adapter.OllamaError) as exc:
        run()
    assert exc.value.status_code == 500 and is_transient(exc.value)

    fake_ollama["responses"] = [(404, {"error": "model 'qwen3:8b' not found, try pulling it first"})]
    with pytest.raises(ollama_adapter.OllamaError) as exc:
        run()
    assert "not found" in str(exc.value) and not is_transient(exc.value)


def test_ollama_not_running_is_transient(monkeypatch):
    def refuse(request):
        raise httpx.ConnectError("connection refused", request=request)
    monkeypatch.setattr(ollama_adapter, "_make_client",
                        lambda: httpx.Client(base_url="http://ollama.test", transport=httpx.MockTransport(refuse)))
    with pytest.raises(httpx.ConnectError) as exc:
        run()
    assert is_transient(exc.value)


def test_orchestrator_knows_ollama(monkeypatch):
    import orchestrator
    monkeypatch.setenv("MODEL_PROVIDER", "ollama")
    provider, model = orchestrator.current_model()
    assert provider == "ollama" and model == ollama_adapter.MODEL
