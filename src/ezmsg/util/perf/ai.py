import os
import json
import textwrap
import urllib.request

DEFAULT_TEST_DESCRIPTION = """\
You are analyzing performance test results for the ezmsg pub/sub system.

Configurations (config):
- fanin: Many publishers to one subscriber
- fanout: one publisher to many subscribers
- relay: one publisher to one subscriber through many relays

Communication strategies (comms):
- local: all subs, relays, and pubs are in the SAME process
- shm / tcp: some clients move to a second process; comms via shared memory / TCP
    * fanin: all publishers moved
    * fanout: all subscribers moved
    * relay: the publisher and all relay nodes moved
- shm_spread / tcp_spread: each client in its own process; comms via SHM / TCP respectively

Variables:
- n_clients: pubs (fanin), subs (fanout), or relays (relay)  
- msg_size: nominal message size (bytes)

Metrics:
- sample_rate: messages/sec at the sink (higher = better)
- data_rate: bytes/sec at the sink (higher = better)
- latency_mean: average send -> receive latency in seconds (lower = better)

Task:
Summarize performance test results. Explain trade-offs by comms/config, call out anomalies/outliers. 
Keep the tone concise, technical, and actionable. 
Please format output such that it will display nicely in a terminal output.

If the rest results are a "PERFORMANCE COMPARISON":
- Metrics are in percentages (100.0 = 100 percent = no change) and do not reflect the ground-truth physical units.
- Summarize key improvements/regressions
- Performance differences +/- 5 percent are likely in the noise. 
"""

def chatgpt_analyze_results(
    results_text: str,
    *,
    prompt: str | None = None,
    model: str | None = None,
    max_chars: int = 120_000,
    temperature: float = 0.2,
) -> str:
    """
    Send results + a test description to OpenAI's Responses API and print the analysis.

    Env vars:
      - OPENAI_API_KEY (required)
      - OPENAI_MODEL (optional; e.g., 'gpt-4o-mini' or a newer model)
    """
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("Please set OPENAI_API_KEY in your environment.")

    model = model or os.getenv("OPENAI_MODEL", "gpt-4o-mini")

    # Keep requests reasonable in size
    results_snippet = results_text if len(results_text) <= max_chars else (
        results_text[:max_chars] + "\n\n[...truncated for token budget...]"
    )

    # You can tweak the system instruction here to steer tone/format
    system_instruction = "You are a senior performance engineer. Prefer precise, structured analysis."

    user_payload = textwrap.dedent(f"""\
        {prompt or DEFAULT_TEST_DESCRIPTION}

        === BEGIN RESULTS ===
        {results_snippet}
        === END RESULTS ===
    """)

    body = {
        "model": model,
        "temperature": temperature,
        "input": [
            {"role": "system", "content": [{"type": "text", "text": system_instruction}]},
            {"role": "user",   "content": [{"type": "text", "text": user_payload}]},
        ],
    }

    req = urllib.request.Request(
        "https://api.openai.com/v1/responses",
        data=json.dumps(body).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        },
        method="POST",
    )

    with urllib.request.urlopen(req) as resp:
        data = json.load(resp)

    # Robust extraction: prefer output_text; fall back to concatenating output content
    text = data.get("output_text")
    if not text:
        parts = []
        for item in data.get("output", []) or []:
            for c in item.get("content", []) or []:
                if c.get("type") in ("output_text", "text") and "text" in c:
                    parts.append(c["text"])
        text = "\n".join(parts) if parts else json.dumps(data, indent=2)

    return text
