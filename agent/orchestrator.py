"""
Thin dispatcher. This is the one place that knows there's more than
one provider -- everything else (tools.py, main.py, eval.py) just calls
run_investigation() without caring which model answers.

Set MODEL_PROVIDER to one of: anthropic (default), gemini, ollama, baseline.
"baseline" is a rule-based scorer with no LLM -- free, offline, and the
number your LLM agents need to beat. "ollama" runs a local model.
"""
import importlib
import os

from dotenv import load_dotenv

load_dotenv()  # reads .env; anything already set in your shell still wins

PROVIDERS = {
    "anthropic": "adapters.anthropic_adapter",
    "gemini": "adapters.gemini_adapter",
    "ollama": "adapters.ollama_adapter",
    "baseline": "adapters.baseline_adapter",
}


def _adapter():
    provider = os.environ.get("MODEL_PROVIDER", "anthropic").lower()
    if provider not in PROVIDERS:
        raise ValueError(
            f"Unknown MODEL_PROVIDER: {provider!r} (expected one of {', '.join(PROVIDERS)})"
        )
    return provider, importlib.import_module(PROVIDERS[provider])


def current_model() -> tuple[str, str]:
    """(provider, model) that run_investigation will use, for recording on each verdict."""
    provider, module = _adapter()
    return provider, getattr(module, "MODEL", "rules")


def run_investigation(account_id: str, txn_id: str, merchant: str, amount: float,
                      context: str | None = None, verbose: bool = True) -> dict:
    _, module = _adapter()
    return module.run_investigation(account_id, txn_id, merchant, amount, context=context, verbose=verbose)
