import os
import sys
import traceback

MODEL_ENV = os.getenv("OPENAI_MODEL", "gpt-3.5-turbo")
def explain_anomaly(metric_name, change, context):
    """Return an explanation string for an anomaly.

    Behavior:
    - If `openai.OpenAI` client can be constructed, use it.
    - On client-init failure try a direct HTTP POST to OpenAI's REST API
      using `requests` (this avoids httpx/httpcore compatibility issues).
    - If neither path is available, return a clear fallback string.
    """
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return "LLM explanation skipped (no OPENAI_API_KEY)."

    prompt = f"The metric '{metric_name}' changed by {change:.2%}. Context: {context}. Explain why this might happen."

    def _clean_explanation(text: str) -> str:
        """Normalize whitespace and ensure the string ends with terminal punctuation.

        This helps prevent truncated/incomplete sentences appearing in reports.
        """
        if not text:
            return "LLM returned an empty explanation."
        # Normalize whitespace
        s = " ".join(str(text).split())
        # If the text ends with an opening parenthesis or similar, close it gracefully
        s = s.strip()
        # Ensure sentence ends with proper punctuation
        if s and s[-1] not in ".!?":
            s = s + "."
        return s

    # Use a module-level memoized backend detection to avoid noisy repeated
    # tracebacks when many anomalies are processed.
    global _llm_backend, _llm_client, _llm_init_error
    try:
        _llm_backend
    except NameError:
        _llm_backend = None
        _llm_client = None
        _llm_init_error = None

    # initialize backend once
    if _llm_backend is None:
        # try modern OpenAI client
        try:
            from openai import OpenAI as _OpenAI
            try:
                _llm_client = _OpenAI(api_key=api_key)
                _llm_backend = "client"
            except Exception as e:
                # capture initialization error but don't spam
                _llm_init_error = e
                _llm_backend = None
        except Exception as e:
            _llm_init_error = e
            _llm_backend = None

        # If client not available, check for requests
        if _llm_backend is None:
            try:
                import requests as _requests  # noqa: F401
                _llm_backend = "requests"
            except Exception as e:
                _llm_init_error = _llm_init_error or e
                _llm_backend = "none"

        # Log only once to stderr so user can see the root cause
        if _llm_backend == "none":
            traceback.print_exception(type(_llm_init_error), _llm_init_error, _llm_init_error.__traceback__, file=sys.stderr)

    # Now use selected backend
    if _llm_backend == "client" and _llm_client is not None:
        try:
            resp = _llm_client.chat.completions.create(
                model=os.getenv("OPENAI_MODEL", MODEL_ENV),
                messages=[{"role": "user", "content": prompt}],
            )
            if hasattr(resp, "choices") and resp.choices:
                choice = resp.choices[0]
                if hasattr(choice, "message") and getattr(choice.message, "content", None):
                    return _clean_explanation(choice.message.content)
                if hasattr(choice, "text"):
                    return _clean_explanation(choice.text)
            return "LLM explanation returned an unexpected response shape."
        except Exception as e:
            # if client fails on first use, record error and fall back to requests
            traceback.print_exception(type(e), e, e.__traceback__, file=sys.stderr)
            _llm_backend = None

    if _llm_backend == "requests" or _llm_backend is None:
        try:
            import requests
        except Exception:
            return "LLM explanation unavailable (no compatible OpenAI client and `requests` not installed)."

        url = "https://api.openai.com/v1/chat/completions"
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": os.getenv("OPENAI_MODEL", MODEL_ENV),
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 256,
        }
        try:
            r = requests.post(url, headers=headers, json=payload, timeout=30)
        except Exception as e:
            traceback.print_exception(type(e), e, e.__traceback__, file=sys.stderr)
            return f"LLM explanation unavailable (network/request failed: {type(e).__name__})."

        if r.status_code != 200:
            try:
                body = r.json()
            except Exception:
                body = r.text
            return f"LLM explanation failed (HTTP {r.status_code}: {body})."

        data = r.json()
        try:
            return _clean_explanation(data["choices"][0]["message"]["content"])
        except Exception:
            return "LLM explanation returned an unexpected REST response shape."

    return "LLM explanation unavailable (no viable backend)."
