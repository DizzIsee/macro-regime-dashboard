"""
generate_narrative.py — AI-powered Bottom Line narrative using Claude + web_search.

Reads macro_data.json, searches for the latest macro headlines via Claude's
built-in web_search tool, then writes a structured narrative to data/synthesis.json.

Run standalone:  python3 scripts/generate_narrative.py
Called from:     scripts/fetch_data.py (as final step)
"""

import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

try:
    import anthropic
except ImportError:
    print("ERROR: anthropic package not installed. Run: pip install anthropic>=0.49.0")
    sys.exit(1)

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ── paths ────────────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
MACRO_JSON = DATA_DIR / "macro_data.json"
SYNTHESIS_JSON = DATA_DIR / "synthesis.json"

# ── system prompt (static — cache-friendly) ──────────────────────────────────
SYSTEM_PROMPT = """You are a senior macro strategist writing the "Bottom Line" section of a Bloomberg Terminal-style dashboard. Your audience is sophisticated institutional investors.

Your task:
1. Use the web_search tool to find the 3-5 most market-moving macro headlines from the past 48 hours. Focus on: Fed communications, inflation releases, labor data, fiscal/Treasury supply, credit spreads, geopolitical risk.
2. Synthesize those headlines with the quantitative data provided to write a concise, actionable narrative.

Output ONLY a JSON object — no markdown fences, no preamble — with exactly these keys:
{
  "fed_monetary": "2-3 sentences on Fed policy stance, rate path, and balance sheet.",
  "labor_data": "2-3 sentences on labor market conditions and trend.",
  "fiscal_supply": "2-3 sentences on fiscal policy, Treasury issuance, and deficit dynamics.",
  "corporate": "2-3 sentences on credit conditions, corporate earnings outlook, and risk appetite.",
  "next_catalyst": "1-2 sentences identifying the single most important upcoming data release or event.",
  "sources": ["headline 1 — source", "headline 2 — source", "headline 3 — source"]
}

Style rules:
- Write in present tense, third-person institutional voice.
- Lead each section with the most important fact.
- Cite specific numbers from the data provided.
- "sources" should list the actual headlines you found, not URLs.
- Do not editorialize or use first person.
- Total length: ~350 words across all five narrative fields."""

# ── helpers ──────────────────────────────────────────────────────────────────

def load_macro_data():
    if not MACRO_JSON.exists():
        raise FileNotFoundError(f"macro_data.json not found at {MACRO_JSON}")
    with open(MACRO_JSON) as f:
        return json.load(f)


def build_user_message(macro_data, today):
    """Build the user turn — contains today's date and full JSON context."""
    data_str = json.dumps(macro_data, indent=2)
    return (
        f"Today is {today}.\n\n"
        "Here is the current macro dashboard data:\n\n"
        f"```json\n{data_str}\n```\n\n"
        "Search for today's most important macro headlines, then write the Bottom Line narrative JSON."
    )


def extract_json(text):
    """Strip markdown fences and extract the first {...} block."""
    text = text.strip()
    # Remove ```json ... ``` or ``` ... ``` fences
    text = re.sub(r"^```(?:json)?\s*", "", text)
    text = re.sub(r"\s*```$", "", text)
    text = text.strip()
    # Find first complete JSON object
    start = text.find("{")
    if start == -1:
        raise ValueError("No JSON object found in response")
    # Walk to find matching closing brace
    depth = 0
    for i, ch in enumerate(text[start:], start):
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return text[start : i + 1]
    raise ValueError("Unbalanced braces in response")


REQUIRED_KEYS = {"fed_monetary", "labor_data", "fiscal_supply", "corporate", "next_catalyst", "sources"}


def validate(data):
    missing = REQUIRED_KEYS - set(data.keys())
    if missing:
        raise ValueError(f"Missing required keys: {missing}")
    for key in REQUIRED_KEYS - {"sources"}:
        if not isinstance(data[key], str) or len(data[key]) < 20:
            raise ValueError(f"Field '{key}' is too short or not a string")
    if not isinstance(data["sources"], list) or len(data["sources"]) == 0:
        raise ValueError("'sources' must be a non-empty list")


# ── main ─────────────────────────────────────────────────────────────────────

def generate_narrative():
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY not set")

    client = anthropic.Anthropic(api_key=api_key)

    macro_data = load_macro_data()
    today = datetime.now(timezone.utc).strftime("%B %d, %Y")
    user_message = build_user_message(macro_data, today)

    messages = [{"role": "user", "content": user_message}]

    print(f"[narrative] Sending request to claude-sonnet-4-6 ({today})...")

    MAX_ITERATIONS = 3
    response = None

    for iteration in range(MAX_ITERATIONS):
        with client.messages.stream(
            model="claude-sonnet-4-6",
            max_tokens=2048,
            system=SYSTEM_PROMPT,
            tools=[{"type": "web_search_20260209", "name": "web_search"}],
            messages=messages,
        ) as stream:
            response = stream.get_final_message()

        print(f"[narrative]   iteration {iteration + 1}: stop_reason={response.stop_reason}, "
              f"content blocks={len(response.content)}")

        if response.stop_reason != "pause_turn":
            break

        # Continue the loop: append assistant turn, add empty user continuation
        messages.append({"role": "assistant", "content": response.content})
        messages.append({"role": "user", "content": "Continue."})
        print("[narrative]   pause_turn — continuing...")

    if response is None:
        raise RuntimeError("No response received from API")

    # Extract the text block that contains the JSON object.
    # Claude sometimes appends a brief closing comment after the JSON, so
    # search all text blocks from last to first for one containing '{'.
    narrative_text = None
    for block in reversed(response.content):
        if hasattr(block, "text") and "{" in block.text:
            narrative_text = block.text
            break

    # Fallback: concatenate all text blocks if no single block has JSON
    if not narrative_text:
        combined = "\n".join(
            block.text for block in response.content
            if hasattr(block, "text") and block.text.strip()
        )
        if "{" in combined:
            narrative_text = combined
        else:
            # Debug: print all text blocks so we can see what the model returned
            print("[narrative] DEBUG — all text blocks:")
            for i, block in enumerate(response.content):
                if hasattr(block, "text") and block.text.strip():
                    print(f"  block {i}: {repr(block.text[:200])}")
            raise ValueError("No JSON object found in any text block")

    print(f"[narrative] Extracting JSON from response ({len(narrative_text)} chars)...")

    raw_json = extract_json(narrative_text)
    parsed = json.loads(raw_json)
    validate(parsed)

    parsed["generated_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    with open(SYNTHESIS_JSON, "w") as f:
        json.dump(parsed, f, indent=2)

    print(f"[narrative] Written to {SYNTHESIS_JSON}")
    print(f"\n{'='*60}")
    print("SYNTHESIS OUTPUT:")
    print('='*60)
    print(json.dumps(parsed, indent=2))
    print('='*60)

    return parsed


if __name__ == "__main__":
    try:
        generate_narrative()
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
