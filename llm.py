import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../bedrock_auth"))

from bedrock_client import make_client  # noqa: E402

_ANALYST_ROLE = (
    "You are a senior cybersecurity equity analyst. "
    "You provide concise, insightful analysis of cybersecurity stock movements, "
    "drawing on your knowledge of the sector including threat landscape, earnings, "
    "competitive dynamics, and macro factors. Be direct and specific. "
    "When you don't know something, say so rather than speculating."
)


def summarize_event(
    ticker: str,
    company: str,
    date: str,
    pct_change: float,
    tavily_results: list[dict],
) -> str:
    """Use claude-haiku-4-5 to produce a one-sentence cause summary."""
    direction = "up" if pct_change > 0 else "down"
    pct_str   = f"{abs(pct_change) * 100:.1f}%"

    if not tavily_results:
        return f"No news found to explain {company}'s {pct_str} move {direction} on {date}."

    snippets = "\n\n".join(
        f"Source: {r.get('title', '')}\n{r.get('content', '')[:500]}"
        for r in tavily_results[:5]
    )
    prompt = (
        f"{company} ({ticker}) moved {pct_str} {direction} on {date}. "
        f"Based only on the following news snippets, write exactly one concise sentence "
        f"explaining the most likely cause. Do not add disclaimers or hedge. "
        f"Be specific about the news event if one is clearly identified.\n\n"
        f"News snippets:\n{snippets}"
    )
    client, model_id = make_client()
    msg = client.messages.create(
        model=model_id,
        max_tokens=120,
        system=[{"type": "text", "text": _ANALYST_ROLE}],
        messages=[{"role": "user", "content": prompt}],
    )
    return msg.content[0].text.strip()


def chat(messages: list[dict], context: dict) -> str:
    """
    Multi-turn chat using claude-sonnet-4-6.

    messages: full history [{role, content}, ...]
    context:  {ticker, company, date, pct_change, summary}
    """
    ticker     = context.get("ticker", "")
    company    = context.get("company", "")
    date       = context.get("date", "")
    pct_change = context.get("pct_change", 0)
    summary    = context.get("summary") or "No summary available."

    direction = "up" if pct_change > 0 else "down"
    pct_str   = f"{abs(pct_change) * 100:.2f}%"

    system = (
        f"{_ANALYST_ROLE}\n\n"
        f"Current context: {company} ({ticker}) moved {pct_str} {direction} on {date}. "
        f"Background: {summary}"
    )
    client, model_id = make_client()
    msg = client.messages.create(
        model=model_id,
        max_tokens=800,
        system=[{"type": "text", "text": system}],
        messages=messages,
    )
    return msg.content[0].text.strip()
