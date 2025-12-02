from openai import OpenAI
import os


def explain_anomaly(metric_name, change, context):
    """
    Return an explanation string for an anomaly. If OpenAI cannot be used
    (missing API key or client errors), return a harmless fallback string so
    the caller can continue.
    """
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return "LLM explanation skipped (no OPENAI_API_KEY)."

    try:
        client = OpenAI(api_key=api_key)
    except Exception:
        return "LLM explanation unavailable (client init failure)."

    prompt = f"The metric '{metric_name}' changed by {change:.2%}. Context: {context}. Explain why this might happen."
    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}]
        )
        # Some SDKs return differently shaped objects; be defensive.
        if hasattr(response, "choices") and response.choices:
            choice = response.choices[0]
            # Different SDKs may nest message differently
            if hasattr(choice, "message") and hasattr(choice.message, "content"):
                return choice.message.content
            if hasattr(choice, "text"):
                return choice.text
        return "LLM explanation returned an unexpected response shape."
    except Exception:
        return "LLM explanation failed (API error)."
